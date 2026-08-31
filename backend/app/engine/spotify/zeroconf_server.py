"""Spotify Zeroconf 配对接收器

参考 SpotConnect/cspot 的 enableZeroConf + LoginBlob：
- mDNS 广播 `_spotify-connect._tcp` (TXT: VERSION=1.0, CPath=/spotify_info, STACK=SP)
- GET  /spotify_info  返回设备信息 (含 DH 公钥)
- POST /spotify_info  接收 Spotify 客户端下发的加密凭据 blob 并解密

blob 解密流程 (cspot LoginBlob::loadZeroconfQuery):
  shared_key = DH(clientKey)
  base_key   = SHA1(shared_key)[:16]
  enc_key    = HMAC-SHA1(base_key, "encryption")[:16]
  blob       = AES-CTR(enc_key, iv) 解密
得到第一层明文后，剩余的二次解密 (PBKDF2/AES-ECB) 由 librespot 的
Session.Builder.decrypt_blob 完成。注意 librespot-python 0.0.10 的
decrypt_blob 会对入参先做 base64 解码，因此这里传入 base64 编码后的
第一层明文以保持兼容。
"""

import base64
import hashlib
import hmac
import logging

from aiohttp import web
from Cryptodome.Cipher import AES
from Cryptodome.Util import Counter
from librespot import util
from librespot.crypto import DiffieHellman
from zeroconf import Zeroconf

log = logging.getLogger("miair")

PROTOCOL_VERSION = "2.7.1"
LIBRARY_VERSION = "MiAir-Next"

_SUCCESS_RESPONSE = {
    "status": 101,
    "statusString": "OK",
    "spotifyError": 0,
}


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


class SpotifyZeroconfReceiver:
    """单个虚拟设备的 Zeroconf 配对端点"""

    def __init__(self, hostname: str, device_name: str, device_id: str,
                 shared_zeroconf: Zeroconf | None = None):
        self.hostname = hostname
        self.device_name = device_name
        self.device_id = device_id
        self.shared_zeroconf = shared_zeroconf
        self._keys = DiffieHellman()
        self._app = web.Application()
        self._app.router.add_get("/{tail:.*}", self._handle_get)
        self._app.router.add_post("/{tail:.*}", self._handle_post)
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self.port = 0
        self._own_zeroconf = False
        self._service_info = None
        # 配对结果回调: async (username, first_layer_decrypted_blob) -> None
        self.on_credentials = None

    # ============================================================
    # 服务启停
    # ============================================================

    async def start(self):
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "0.0.0.0", 0)
        await self._site.start()
        self.port = self._site._server.sockets[0].getsockname()[1]

        if self.shared_zeroconf is None:
            self.shared_zeroconf = Zeroconf()
            self._own_zeroconf = True

        import socket as _socket
        from zeroconf import ServiceInfo
        self._service_info = ServiceInfo(
            "_spotify-connect._tcp.local.",
            f"{self.device_name}._spotify-connect._tcp.local.",
            addresses=[_socket.inet_aton(self.hostname)],
            port=self.port,
            properties={
                "VERSION": "1.0",
                "CPath": "/spotify_info",
                "STACK": "SP",
            },
        )
        await self.shared_zeroconf.async_register_service(self._service_info)
        log.info(
            f"Spotify Zeroconf 服务已启动: {self.device_name} "
            f"http://{self.hostname}:{self.port}/spotify_info"
        )

    async def stop(self):
        if self._service_info is not None and self.shared_zeroconf is not None:
            try:
                await self.shared_zeroconf.async_unregister_service(self._service_info)
            except Exception as e:
                log.warning(f"Spotify Zeroconf 注销失败: {e}")
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        if self._own_zeroconf and self.shared_zeroconf is not None:
            try:
                self.shared_zeroconf.close()
            except Exception:
                pass
            self.shared_zeroconf = None
            self._own_zeroconf = False

    # ============================================================
    # HTTP 处理
    # ============================================================

    def _build_info(self) -> dict:
        """cspot LoginBlob::buildZeroconfInfo 的等价实现"""
        return {
            "status": 101,
            "statusString": "OK",
            "spotifyError": 0,
            "version": PROTOCOL_VERSION,
            "libraryVersion": LIBRARY_VERSION,
            "accountReq": "PREMIUM",
            "brandDisplayName": "MiAir",
            "modelDisplayName": self.device_name,
            "voiceSupport": "NO",
            "availability": "",
            "productID": 0,
            "tokenType": "default",
            "groupStatus": "NONE",
            "resolverVersion": "0",
            "scope": "streaming,client-authorization-universal",
            "activeUser": "",
            "deviceID": self.device_id,
            "remoteName": self.device_name,
            "publicKey": _b64(self._keys.public_key_bytes()),
            "deviceType": "SPEAKER",
        }

    async def _handle_get(self, request: web.Request) -> web.Response:
        return web.json_response(self._build_info())

    async def _handle_post(self, request: web.Request) -> web.Response:
        try:
            params = await request.post()
            username, decrypted = self._decrypt_add_user(params)
        except Exception as e:
            log.error(f"Spotify Zeroconf 配对失败: {e}")
            return web.json_response(
                {"status": 102, "statusString": "ERROR", "spotifyError": 1}
            )
        log.info(f"Spotify Zeroconf: 用户 {username} 配对成功 (设备 {self.device_name})")
        # 通知上层建立会话 (librespot decrypt_blob 需要 base64 输入, 见模块说明)
        if self.on_credentials is not None:
            await self.on_credentials(username, decrypted)
        return web.json_response(dict(_SUCCESS_RESPONSE))

    def _decrypt_add_user(self, params) -> tuple[str, bytes]:
        """解密 Spotify 客户端下发的凭据 blob (cspot postHandler + LoginBlob)"""
        username = params.get("userName", "")
        blob_b64 = params.get("blob", "")
        client_key_b64 = params.get("clientKey", "")
        if not username or not blob_b64 or not client_key_b64:
            raise ValueError("缺少 userName / blob / clientKey")

        client_key = base64.b64decode(client_key_b64)
        blob_bytes = base64.b64decode(blob_b64)

        # 第一层解密: DH 共享密钥 -> HMAC 派生密钥 -> AES-CTR
        shared_key = util.int_to_bytes(self._keys.compute_shared_key(client_key))

        iv = blob_bytes[:16]
        encrypted = blob_bytes[16:-20]
        checksum = blob_bytes[-20:]

        base_key = hashlib.sha1(shared_key).digest()[:16]
        encryption_key = hmac.new(base_key, b"encryption", hashlib.sha1).digest()
        checksum_key = hmac.new(base_key, b"checksum", hashlib.sha1).digest()

        expected = hmac.new(checksum_key, encrypted, hashlib.sha1).digest()
        if not hmac.compare_digest(expected, checksum):
            raise ValueError("blob 校验失败 (checksum 不匹配)")

        counter = Counter.new(128, initial_value=int.from_bytes(iv, "big"))
        cipher = AES.new(encryption_key[:16], AES.MODE_CTR, counter=counter)
        decrypted = cipher.decrypt(encrypted)
        return username, decrypted

    # ============================================================
    # 工具
    # ============================================================

    @staticmethod
    def make_device_id(device_name: str) -> str:
        """cspot LoginBlob 构造函数: 基础 ID + 名称哈希, 保证名字稳定时设备 ID 稳定"""
        digest = hashlib.sha256(device_name.encode()).hexdigest()[:16]
        return "142137fd329622137a149016" + digest
