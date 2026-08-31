"""Spotify Connect 管理器

对应 SpotConnect 的 per-player 模式 + cspot 的 CSpotContext:
每个小爱音箱一个独立的虚拟 Spotify Connect 设备 (SpeakerSpotify):
- SpotifyZeroconfReceiver: mDNS 广播 + 接收 Spotify 客户端配对
- Session (librespot): 零配置配对凭据 / 已存凭据 登录
- SpircController + SpotifyPlayer: SPIRC 命令解析与播放桥接
- SpotifyAudioServer: 本地 HTTP 音频流 (与 AirPlay 路线一致, 统一 WAV)

凭据持久化: conf/spotify/<did>.json (librespot stored_credentials),
重启后自动重连, 无需重新配对; 过期/失效时自动清除并等待重新配对。
"""

import asyncio
import base64
import logging
import os

from zeroconf import IPVersion, Zeroconf

from librespot.core import Session
from librespot.proto import Connect_pb2 as Connect

from app.engine.speaker import SpeakerController
from app.engine.spotify.audio_server import SpotifyAudioServer
from app.engine.spotify.player import SpotifyPlayer
from app.engine.spotify.spirc import SpircController
from app.engine.spotify.zeroconf_server import SpotifyZeroconfReceiver

log = logging.getLogger("miair")

_HEARTBEAT_INTERVAL = 30  # 秒, SPIRC Notify 保活与进度上报


class SpeakerSpotify:
    """单个音箱的 Spotify Connect 接收器包装"""

    def __init__(self, hostname: str, controller: SpeakerController,
                 shared_zeroconf: Zeroconf | None = None, config=None):
        self.hostname = hostname
        self.controller = controller
        self.speaker = controller.speaker
        self.shared_zeroconf = shared_zeroconf
        self.config = config

        self.audio_server: SpotifyAudioServer | None = None
        self.zeroconf_receiver: SpotifyZeroconfReceiver | None = None
        self.session: Session | None = None
        self.spirc: SpircController | None = None
        self.player: SpotifyPlayer | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._login_lock = asyncio.Lock()
        self._closed = False

    # ============================================================
    # 属性
    # ============================================================

    @property
    def device_name(self) -> str:
        """广播名跟随 dlna_name (与 AirPlay/DLNA 一致)"""
        return self.speaker.get_dlna_name()

    @property
    def device_id(self) -> str:
        """40 字符稳定设备 ID (名字不变则 ID 不变)"""
        return SpotifyZeroconfReceiver.make_device_id(self.device_name)

    @property
    def _credentials_file(self) -> str:
        conf_path = getattr(self.config, "conf_path", "conf") if self.config else "conf"
        return os.path.join(conf_path, "spotify", f"{self.speaker.did}.json")

    @property
    def paired(self) -> bool:
        return self.session is not None and self.spirc is not None

    def status(self) -> dict:
        """API 状态 (speakers 列表用)"""
        playing = bool(self.player and self.player.is_playing)
        meta = self.player.current_meta if self.player else {}
        return {
            "spotify_enabled": True,
            "spotify_paired": self.paired,
            "spotify_playing": playing,
            "spotify_track": meta.get("title", ""),
            "spotify_artist": meta.get("artist", ""),
        }

    # ============================================================
    # 生命周期
    # ============================================================

    async def start(self):
        """启动该音箱的 Spotify Connect 服务"""
        try:
            # 音频流: 统一 WAV (与 AirPlay 路线一致, 零编码延迟)
            self.audio_server = SpotifyAudioServer(self.hostname, 0, audio_format="wav")
            await self.audio_server.start()

            self.zeroconf_receiver = SpotifyZeroconfReceiver(
                self.hostname, self.device_name, self.device_id,
                shared_zeroconf=self.shared_zeroconf,
            )
            self.zeroconf_receiver.on_credentials = self._handle_credentials
            await self.zeroconf_receiver.start()

            # 已有存储凭据则直接恢复会话 (无需重新配对)
            if os.path.isfile(self._credentials_file):
                await self._start_session()

            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            log.info(
                f"音箱 {self.device_name} 的 Spotify Connect 服务已启动 "
                f"(http://{self.hostname}:{self.zeroconf_receiver.port}/spotify_info)"
            )
        except Exception as e:
            log.error(f"启动音箱 {self.device_name} 的 Spotify Connect 服务失败: {e}")
            raise

    async def stop(self):
        """停止该音箱的 Spotify Connect 服务"""
        self._closed = True
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        await self._close_session()
        if self.zeroconf_receiver:
            await self.zeroconf_receiver.stop()
            self.zeroconf_receiver = None
        if self.audio_server:
            await self.audio_server.stop()
            self.audio_server = None
        log.info(f"音箱 {self.device_name} 的 Spotify Connect 服务已停止")

    async def rename(self, new_name: str):
        """重命名后重建服务 (mDNS 需按新名重新注册, 与 AirPlay 一致)"""
        log.info(f"Spotify Connect 重命名: {self.device_name} -> {new_name}")
        await self.stop()
        self.speaker.dlna_name = new_name
        await self.start()

    # ============================================================
    # 登录 / 会话管理
    # ============================================================

    async def _handle_credentials(self, username: str, blob: bytes):
        """Zeroconf 配对回调: 用解密后的第一层 blob 建立 Spotify 会话

        librespot Session.Builder.blob 内部 decrypt_blob 会先对入参做
        base64 解码, 因此这里传 b64 编码后的第一层明文。
        """
        blob_b64 = base64.b64encode(blob).decode()
        try:
            await self._start_session(username, blob_b64)
        except Exception as e:
            log.error(f"Spotify 登录失败 (设备 {self.device_name}): {e}")
            # 配对的凭据无法使用, 清掉等待下次重新配对
            self._remove_credentials()

    async def _start_session(self, username: str | None = None, blob_b64: str | None = None):
        async with self._login_lock:
            if self._closed:
                return
            # 换账号/重新配对时先释放旧会话
            await self._close_session()
            self.session = await asyncio.to_thread(self._create_session, username, blob_b64)

            self.player = SpotifyPlayer(
                self.session, None, self.audio_server, self.controller, config=self.config
            )
            self.spirc = SpircController(
                self.session, self.device_name, self.device_id, handler=self.player
            )
            self.player.spirc = self.spirc
            await asyncio.to_thread(self.spirc.start)
            log.info(
                f"Spotify 会话已建立: {self.device_name} "
                f"(用户 {self.session.username()}, 凭据已存 {self._credentials_file})"
            )

    def _create_session(self, username: str | None, blob_b64: str | None) -> Session:
        """创建 librespot 会话 (阻塞: TCP + 认证, 供 to_thread 调用)"""
        os.makedirs(os.path.dirname(self._credentials_file), exist_ok=True)
        conf = (
            Session.Configuration.Builder()
            .set_cache_enabled(False)
            .set_store_credentials(True)
            .set_stored_credential_file(self._credentials_file)
            .build()
        )
        builder = (
            Session.Builder(conf)
            .set_device_name(self.device_name)
            .set_device_id(self.device_id)
            .set_device_type(Connect.DeviceType.SPEAKER)
            .set_preferred_locale("zh")
        )
        if blob_b64:
            builder.blob(username, blob_b64.encode())
        else:
            builder.stored_file(self._credentials_file)
        return builder.create()

    async def _close_session(self):
        """关闭当前 Spotify 会话 (保留已存凭据文件)"""
        if self.spirc:
            self.spirc.stop()
            self.spirc = None
        self.player = None
        if self.session:
            session, self.session = self.session, None
            try:
                await asyncio.to_thread(session.close)
            except Exception as e:
                log.warning(f"关闭 Spotify 会话失败: {e}")

    def _remove_credentials(self):
        try:
            if os.path.isfile(self._credentials_file):
                os.remove(self._credentials_file)
                log.info(f"已清除 Spotify 凭据: {self._credentials_file}")
        except Exception as e:
            log.warning(f"清除 Spotify 凭据失败: {e}")

    # ============================================================
    # 心跳: SPIRC Notify 保活 + 断线重订阅 + 失效检测
    # ============================================================

    async def _heartbeat_loop(self):
        while not self._closed:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            if self._closed or not self.spirc or not self.session:
                continue
            try:
                if not self.session.is_valid():
                    continue  # librespot 正在自动重连, heartbeat 会检测到新 connection
                await asyncio.to_thread(self.spirc.heartbeat)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"Spotify 心跳失败 ({self.device_name}): {e}")


class SpotifyManager:
    """管理所有音箱的 Spotify Connect 接收器"""

    def __init__(self, hostname: str, config=None):
        self.hostname = hostname
        self.config = config
        self.speaker_spotify: dict[str, SpeakerSpotify] = {}  # did -> SpeakerSpotify
        self._shared_zeroconf: Zeroconf | None = None

    async def start_for_speakers(self, controllers: dict[str, SpeakerController]):
        """为所有音箱启动 Spotify Connect 服务"""
        if not self._shared_zeroconf:
            self._shared_zeroconf = Zeroconf(ip_version=IPVersion.All)
            log.info("创建共享 Zeroconf 实例用于 Spotify Connect")

        for did, controller in controllers.items():
            if did in self.speaker_spotify:
                continue
            try:
                speaker_spotify = SpeakerSpotify(
                    self.hostname, controller, self._shared_zeroconf,
                    config=self.config,
                )
                await speaker_spotify.start()
                self.speaker_spotify[did] = speaker_spotify
            except Exception as e:
                log.error(f"为音箱 {controller.speaker.get_dlna_name()} 启动 Spotify Connect 失败: {e}")

        log.info(f"共启动了 {len(self.speaker_spotify)} 个音箱的 Spotify Connect 服务")

    async def stop(self):
        """停止所有 Spotify Connect 服务"""
        for did, speaker_spotify in list(self.speaker_spotify.items()):
            try:
                await speaker_spotify.stop()
            except Exception as e:
                log.error(f"停止音箱 Spotify Connect 失败: {e}")
        self.speaker_spotify.clear()

        if self._shared_zeroconf:
            try:
                self._shared_zeroconf.close()
                log.info("Spotify 共享 Zeroconf 已关闭")
            except Exception as e:
                log.error(f"关闭 Zeroconf 失败: {e}")
            self._shared_zeroconf = None
        log.info("所有 Spotify Connect 服务已停止")

    async def restart_for_speakers(self, controllers: dict[str, SpeakerController]):
        """重新为音箱启动 Spotify Connect 服务"""
        await self.stop()
        await self.start_for_speakers(controllers)
