"""SPIRC 协议控制器

移植自 SpotConnect 的 cspot (SpircHandler.cpp / PlaybackState.cpp)：
- 订阅 Mercury `hm://remote/user/<username>/`，解码远程 Frame 并分发命令
- 维护本机播放状态 (PlaybackState 的 innerFrame 等价物)，编码 Notify 帧上报
- 设备能力上报与 cspot 一致 (kCanBePlayer / kDeviceType=SPEAKER / ...)
"""

import asyncio
import logging
import time

from librespot.core import Session
from librespot.mercury import MercuryClient, RawMercuryRequest

from app.engine.spotify import spirc_pb2 as spirc

log = logging.getLogger("miair")

PROTOCOL_VERSION = "2.7.1"
SW_VERSION = "MiAir-Next"

# cspot PlaybackState 构造函数中的能力列表 (typ, intValue, stringValue)
_CAPABILITIES = [
    (spirc.kCanBePlayer, [1], []),
    (spirc.kDeviceType, [4], []),  # 4 = SPEAKER
    (spirc.kGaiaEqConnectId, [1], []),
    (spirc.kSupportsLogout, [0], []),
    (spirc.kSupportsPlaylistV2, [1], []),
    (spirc.kIsObservable, [1], []),
    (spirc.kVolumeSteps, [64], []),
    (
        spirc.kSupportedContexts,
        [],
        ["album", "playlist", "search", "inbox", "toplist", "starred", "publishedstarred", "track"],
    ),
    (
        spirc.kSupportedTypes,
        [],
        ["audio/track", "audio/episode", "audio/episode+track"],
    ),
]


def _now_ms() -> int:
    return int(time.time() * 1000)


class SpircHandler:
    """SPIRC 命令的异步回调接口 (由 SpotifyPlayer 实现)"""

    async def on_load(self, tracks: list, index: int, position_ms: int):
        """收到 Load/Replace: 播放新的曲目队列。position_ms < 0 表示保持当前位置"""

    async def on_play(self):
        """恢复播放"""

    async def on_pause(self):
        """暂停播放"""

    async def on_seek(self, position_ms: int):
        """跳转播放位置"""

    async def on_volume(self, volume: int):
        """音量变化 (0-65535)"""

    async def on_next(self):
        """下一曲"""

    async def on_prev(self):
        """上一曲"""

    async def on_deactivate(self):
        """其他设备接管，本设备被抢占"""


class SpircController:
    """单个虚拟设备的 SPIRC 协议处理"""

    def __init__(self, session: Session, device_name: str, device_id: str,
                 handler: SpircHandler):
        self._session = session
        self._device_name = device_name
        self._device_id = device_id
        self._handler = handler
        self._loop = asyncio.get_event_loop()
        self._seq_nr = 0
        self._closed = False

        # ---- 播放状态 (cspot PlaybackState::innerFrame 的等价物) ----
        self.status = spirc.kPlayStatusStop
        self.position_ms = 0
        self.position_measured_at = _now_ms()
        self.volume = 65535  # device_state.volume, 0-65535
        self.is_active = False
        self.became_active_at = 0
        self.context_uri = ""
        self.playing_track_index = 0
        self.shuffle = False
        self.repeat = False
        self.tracks: list[spirc.TrackRef] = []

        # Mercury 订阅 URI
        self._uri = f"hm://remote/user/{session.username()}/"
        # 记录当前 connection 对象，用于检测 librespot 自动重连后重新订阅
        self._last_connection = None

    # ============================================================
    # 生命周期
    # ============================================================

    def start(self):
        """订阅 Mercury 并发送 Hello (阻塞调用，请在后台线程执行)"""
        self._subscribe()
        self._last_connection = self._session.connection
        self._send_cmd(spirc.kMessageTypeHello)
        log.info(f"Spotify SPIRC 已启动: {self._device_name} ({self._uri})")

    def _subscribe(self):
        self._session.mercury().subscribe(self._uri, self._listener)

    def stop(self):
        self._closed = True

    # ============================================================
    # 状态更新 (由 player 调用)
    # ============================================================

    def set_playing(self, position_ms: int | None = None):
        self.status = spirc.kPlayStatusPlay
        if position_ms is not None:
            self.position_ms = position_ms
        self.position_measured_at = _now_ms()

    def set_paused(self, position_ms: int | None = None):
        self.status = spirc.kPlayStatusPause
        if position_ms is not None:
            self.position_ms = position_ms
        self.position_measured_at = _now_ms()

    def set_stopped(self):
        self.status = spirc.kPlayStatusStop
        self.position_ms = 0
        self.position_measured_at = _now_ms()

    def update_position(self, position_ms: int):
        self.position_ms = position_ms
        self.position_measured_at = _now_ms()

    def set_volume(self, volume: int):
        """设置音量 (0-65535)，同时上报"""
        self.volume = max(0, min(65535, int(volume)))

    def set_active(self, active: bool):
        self.is_active = active
        if active:
            self.became_active_at = _now_ms()

    async def notify(self):
        """上报当前状态 (Notify 帧)"""
        await asyncio.to_thread(self._send_cmd, spirc.kMessageTypeNotify)

    # ============================================================
    # Mercury 收发
    # ============================================================

    def _listener(self, resp: MercuryClient.Response):
        """Mercury 订阅回调 (在 librespot receiver 线程执行)"""
        if self._closed:
            return
        try:
            # Response.payload 是分片列表: [0] 为 Mercury header, [1:] 为数据
            payload = resp.payload
            if isinstance(payload, (bytes, bytearray)):
                data = bytes(payload)
            elif payload:
                data = b"".join(payload[1:])
            else:
                return
            frame = spirc.Frame()
            frame.ParseFromString(data)
            self._handle_frame(frame)
        except Exception as e:
            log.error(f"Spotify SPIRC 帧解析失败: {e}")

    def _dispatch(self, coro):
        try:
            asyncio.run_coroutine_threadsafe(coro, self._loop)
        except RuntimeError as e:
            log.warning(f"Spotify SPIRC 事件分发失败 (事件循环未运行): {e}")

    def _handle_frame(self, frame: spirc.Frame):
        # 过滤自己广播出去的帧
        if frame.ident == self._device_id:
            return

        typ = frame.typ
        if typ == spirc.kMessageTypeNotify:
            # 其他设备激活，若本机正在播放则被抢占
            if self.is_active and frame.device_state.is_active:
                log.info(f"Spotify: {self._device_name} 被其他设备接管，停止播放")
                self.is_active = False
                self._dispatch(self._handler.on_deactivate())
        elif typ == spirc.kMessageTypeLoad:
            self.set_active(True)
            self.context_uri = frame.state.context_uri
            self.tracks = list(frame.state.track)
            self.playing_track_index = frame.state.playing_track_index
            position = frame.position or 0
            log.info(
                f"Spotify: Load 帧收到 {len(self.tracks)} 首曲目, "
                f"index={self.playing_track_index}, pos={position}ms"
            )
            self._dispatch(
                self._handler.on_load(self.tracks, self.playing_track_index, position)
            )
        elif typ == spirc.kMessageTypeReplace:
            self.tracks = list(frame.state.track)
            self.playing_track_index = frame.state.playing_track_index
            # cspot: 流仍在播放时仅更新后续队列; 简化为重载当前曲目并保留位置
            self._dispatch(
                self._handler.on_load(self.tracks, self.playing_track_index, -1)
            )
        elif typ == spirc.kMessageTypePlay:
            self._dispatch(self._handler.on_play())
        elif typ == spirc.kMessageTypePause:
            self._dispatch(self._handler.on_pause())
        elif typ == spirc.kMessageTypePlayPause:
            if self.status == spirc.kPlayStatusPlay:
                self._dispatch(self._handler.on_pause())
            else:
                self._dispatch(self._handler.on_play())
        elif typ == spirc.kMessageTypeSeek:
            self._dispatch(self._handler.on_seek(frame.position or 0))
        elif typ == spirc.kMessageTypeVolume:
            self.volume = frame.volume or 0
            self._dispatch(self._handler.on_volume(frame.volume or 0))
        elif typ == spirc.kMessageTypeNext:
            self._dispatch(self._handler.on_next())
        elif typ == spirc.kMessageTypePrev:
            self._dispatch(self._handler.on_prev())
        elif typ == spirc.kMessageTypeShuffle:
            self.shuffle = frame.state.shuffle
            self._dispatch(self.notify())
        elif typ == spirc.kMessageTypeRepeat:
            self.repeat = frame.state.repeat
            self._dispatch(self.notify())
        # Hello/Goodbye/Probe 等不处理

    def _send_cmd(self, typ: int):
        """编码并发送帧 (阻塞，cspot sendCmd 的等价物)"""
        frame = self._build_frame(typ)
        self._session.mercury().send_sync(
            RawMercuryRequest.send(self._uri, frame.SerializeToString())
        )

    def _build_frame(self, typ: int) -> spirc.Frame:
        frame = spirc.Frame()
        frame.version = 1
        frame.ident = self._device_id
        frame.protocol_version = PROTOCOL_VERSION
        frame.seq_nr = self._seq_nr
        self._seq_nr += 1
        frame.typ = typ
        frame.state_update_id = _now_ms()

        # device_state (cspot PlaybackState 构造)
        ds = frame.device_state
        ds.sw_version = SW_VERSION
        ds.is_active = self.is_active
        ds.can_play = True
        ds.volume = self.volume
        ds.name = self._device_name
        if self.is_active:
            ds.became_active_at = self.became_active_at
        for cap_type, int_values, str_values in _CAPABILITIES:
            cap = ds.capabilities.add()
            cap.typ = cap_type
            cap.intValue.extend(int_values)
            cap.stringValue.extend(str_values)

        # state
        st = frame.state
        st.context_uri = self.context_uri
        st.playing_track_index = self.playing_track_index
        st.position_ms = self.position_ms
        st.status = self.status
        st.position_measured_at = self.position_measured_at
        st.shuffle = self.shuffle
        st.repeat = self.repeat
        for ref in self.tracks:
            st.track.add().CopyFrom(ref)
        return frame

    # ============================================================
    # 会话健康检查 (由管理线程心跳调用)
    # ============================================================

    def heartbeat(self):
        """健康心跳 (阻塞，请在后台线程执行)：
        - 发送 Notify 帧保活并上报进度
        - librespot 自动重连后 connection 对象变化 -> 重新订阅
        """
        if self._closed:
            return
        conn = self._session.connection
        if conn is not self._last_connection:
            log.info(f"Spotify: 检测到连接重建，重新订阅 SPIRC ({self._device_name})")
            self._last_connection = conn
            try:
                self._subscribe()
            except Exception as e:
                log.warning(f"Spotify: 重新订阅失败: {e}")
                raise
        self._send_cmd(spirc.kMessageTypeNotify)
