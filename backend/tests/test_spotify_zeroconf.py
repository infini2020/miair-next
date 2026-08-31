"""Spotify Zeroconf 配对接收器测试 (不依赖真实 mDNS / Spotify 客户端)

覆盖:
- GET /spotify_info 返回 cspot buildZeroconfInfo 等价的设备信息 (含 DH 公钥)
- POST /spotify_info 完整配对握手: 客户端侧加密 blob -> 服务端解密 -> 回调
- 篡改 blob 的 checksum 时拒绝配对
- make_device_id 对同名设备稳定
"""

import asyncio
import base64
import hashlib
import hmac
import secrets
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from Cryptodome.Cipher import AES
from Cryptodome.Util import Counter
from librespot import util
from librespot.crypto import DiffieHellman

from app.engine.spotify.zeroconf_server import SpotifyZeroconfReceiver

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def mdns():
    """mock 掉真实 mDNS 注册, 测试只关注 HTTP 配对协议"""
    z = MagicMock()
    z.async_register_service = AsyncMock()
    z.async_unregister_service = AsyncMock()
    return z


async def _start_receiver(mdns):
    recv = SpotifyZeroconfReceiver(
        "127.0.0.1", "Test Speaker", "test-device-id", shared_zeroconf=mdns
    )
    captured = []

    async def on_credentials(username, blob):
        captured.append((username, blob))

    recv.on_credentials = on_credentials
    await recv.start()
    return recv, captured


def _client_encrypt(server_public: bytes, plaintext: bytes) -> tuple[bytes, bytes]:
    """模拟 Spotify 客户端侧的 blob 加密 (cspot postHandler 的逆过程)"""
    client = DiffieHellman()
    shared = util.int_to_bytes(client.compute_shared_key(server_public))
    client_public = client.public_key_bytes()

    base_key = hashlib.sha1(shared).digest()[:16]
    enc_key = hmac.new(base_key, b"encryption", hashlib.sha1).digest()[:16]
    checksum_key = hmac.new(base_key, b"checksum", hashlib.sha1).digest()

    iv = secrets.token_bytes(16)
    counter = Counter.new(128, initial_value=int.from_bytes(iv, "big"))
    encrypted = AES.new(enc_key, AES.MODE_CTR, counter=counter).encrypt(plaintext)
    checksum = hmac.new(checksum_key, encrypted, hashlib.sha1).digest()

    return iv + encrypted + checksum, client_public


async def test_get_info_returns_device_descriptor(mdns):
    recv, _ = await _start_receiver(mdns)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://127.0.0.1:{recv.port}/spotify_info",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                assert resp.status == 200
                info = await resp.json()

        assert info["status"] == 101
        assert info["accountReq"] == "PREMIUM"
        assert info["deviceID"] == "test-device-id"
        assert info["remoteName"] == "Test Speaker"
        assert info["deviceType"] == "SPEAKER"
        assert info["brandDisplayName"] == "MiAir"
        # 公钥必须是 96 字节 DH 公钥 (base64)
        pub = base64.b64decode(info["publicKey"])
        assert len(pub) == 96
    finally:
        await recv.stop()

    mdns.async_register_service.assert_awaited_once()
    mdns.async_unregister_service.assert_awaited_once()


async def test_full_pairing_handshake(mdns):
    recv, captured = await _start_receiver(mdns)
    secret = secrets.token_bytes(64)  # 假的第一层凭据明文
    try:
        async with aiohttp.ClientSession() as session:
            info = await (
                await session.get(
                    f"http://127.0.0.1:{recv.port}/spotify_info",
                    timeout=aiohttp.ClientTimeout(total=5),
                )
            ).json()
            server_public = base64.b64decode(info["publicKey"])

            blob, client_public = _client_encrypt(server_public, secret)
            resp = await session.post(
                f"http://127.0.0.1:{recv.port}/spotify_info",
                data={
                    "userName": "test-user@example.com",
                    "blob": base64.b64encode(blob).decode(),
                    "clientKey": base64.b64encode(client_public).decode(),
                },
                timeout=aiohttp.ClientTimeout(total=5),
            )
            assert resp.status == 200
            result = await resp.json()
            assert result == {"status": 101, "statusString": "OK", "spotifyError": 0}

        # 服务端应解出与客户端加密前完全一致的明文并触发回调
        assert captured == [("test-user@example.com", secret)]
    finally:
        await recv.stop()


async def test_tampered_blob_rejected(mdns):
    recv, captured = await _start_receiver(mdns)
    try:
        async with aiohttp.ClientSession() as session:
            info = await (
                await session.get(
                    f"http://127.0.0.1:{recv.port}/spotify_info",
                    timeout=aiohttp.ClientTimeout(total=5),
                )
            ).json()
            server_public = base64.b64decode(info["publicKey"])

            blob, client_public = _client_encrypt(server_public, b"payload")
            tampered = bytearray(blob)
            tampered[-1] ^= 0xFF  # 破坏 checksum
            resp = await session.post(
                f"http://127.0.0.1:{recv.port}/spotify_info",
                data={
                    "userName": "attacker",
                    "blob": base64.b64encode(bytes(tampered)).decode(),
                    "clientKey": base64.b64encode(client_public).decode(),
                },
                timeout=aiohttp.ClientTimeout(total=5),
            )
            assert resp.status == 200
            result = await resp.json()
            assert result["status"] == 102  # ERROR
        assert captured == []  # 不得触发凭据回调
    finally:
        await recv.stop()


async def test_missing_params_rejected(mdns):
    recv, captured = await _start_receiver(mdns)
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                f"http://127.0.0.1:{recv.port}/spotify_info",
                data={"userName": "x"},  # 缺 blob / clientKey
                timeout=aiohttp.ClientTimeout(total=5),
            )
            result = await resp.json()
            assert result["status"] == 102
        assert captured == []
    finally:
        await recv.stop()


def test_make_device_id_stable():
    a = SpotifyZeroconfReceiver.make_device_id("Living Room")
    b = SpotifyZeroconfReceiver.make_device_id("Living Room")
    c = SpotifyZeroconfReceiver.make_device_id("Bedroom")
    assert a == b and a != c
    assert a.startswith("142137fd329622137a149016") and len(a) == 24 + 16
