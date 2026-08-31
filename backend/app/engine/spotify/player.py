"""Spotify 曲目播放器

对应 SpotConnect/cspot 的 TrackQueue + TrackPlayer + 桥接层:
- 收到 SPIRC Load 后用 librespot PlayableContentFeeder 加载曲目
  (元数据 / 音频密钥 / CDN URL / 解密流)
- 后台线程 PyAV 解码 Ogg Vorbis -> PCM -> 本地 HTTP 流
- 通过小爱音箱 play_url 拉流播放, 控制 (暂停/音量/切歌) 走 SpeakerController
"""

import asyncio
import logging
import threading

from librespot import util as spot_util
from librespot.audio.decoders import AudioQuality, FormatOnlyAudioQuality
from librespot.core import Session
from librespot.metadata import EpisodeId, TrackId
from librespot.audio.format import SuperAudioFormat

from app.engine.spotify.audio_server import SpotifyAudioServer
from app.engine.spotify.spirc import SpircController, SpircHandler
from app.engine.spotify import spirc_pb2 as spirc

log = logging.getLogger("miair")

_SAMPLE_RATE = 44100
_CHANNELS = 2
_SAMPLE_WIDTH = 2
_BYTE_RATE = _SAMPLE_RATE * _CHANNELS * _SAMPLE_WIDTH
_MAX_CONSECUTIVE_FAILURES = 5  # 连续加载失败上限, 防坏队列死循环


class SpotifyPlayer(SpircHandler):
    """SPIRC 命令到小爱音箱播放的桥接"""

    def __init__(self, session: Session, spirc: SpircController,
                 audio_server: SpotifyAudioServer, speaker, config=None):
        self._session = session
        self.spirc = spirc
        self.audio_server = audio_server
        self.speaker = speaker  # SpeakerController
        self.config = config
        self._loop = asyncio.get_event_loop()

        self._tracks: list = []
        self._index = -1
        self._generation = 0  # 解码线程代数, 递增使旧线程退出
        self._decode_thread: threading.Thread | None = None
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._bytes_fed = 0
        self._seek_offset_ms = 0
        self._consecutive_failures = 0
        self._current_meta: dict = {}
        self._loading = asyncio.Lock()

    # ============================================================
    # 元数据 / 加载
    # ============================================================

    @staticmethod
    def _playable_id_from_ref(ref):
        """TrackRef -> TrackId/EpisodeId (gid 优先, 回退 uri)"""
        if ref.gid:
            hex_id = spot_util.bytes_to_hex(ref.gid)
            if ref.uri and ref.uri.startswith("spotify:episode:"):
                return EpisodeId.from_hex(hex_id)
            return TrackId.from_hex(hex_id)
        if ref.uri:
            try:
                if ref.uri.startswith("spotify:episode:"):
                    return EpisodeId.from_uri(ref.uri)
                return TrackId.from_uri(ref.uri)
            except Exception:
                return None
        return None

    @staticmethod
    def _extract_meta(loaded) -> dict:
        t = loaded.track
        if t is not None:
            return {
                "title": t.name,
                "artist": ", ".join(a.name for a in t.artist) if t.artist else "",
                "album": t.album.name if t.HasField("album") else "",
                "duration": t.duration,
            }
        e = loaded.episode
        if e is not None:
            return {
                "title": e.name,
                "artist": "",
                "album": "",
                "duration": e.duration,
            }
        return {}

    def _load_track(self, playable_id):
        """加载曲目 (阻塞, 供 to_thread 调用); 对应 cspot QueuedTrack 各 step"""
        quality = FormatOnlyAudioQuality(AudioQuality.NORMAL, SuperAudioFormat.VORBIS)
        return self._session.content_feeder().load(playable_id, quality, False, None)

    # ============================================================
    # SPIRC 命令处理
    # ============================================================

    async def on_load(self, tracks: list, index: int, position_ms: int):
        async with self._loading:
            self._tracks = list(tracks)
            if not self._tracks:
                await self._stop_playback()
                return
            index = max(0, min(index, len(self._tracks) - 1))
            if position_ms < 0:
                position_ms = self.spirc.position_ms
            self._index = index
            await self._play_index(index, position_ms)

    async def on_play(self):
        if self._index < 0:
            return
        self._pause_event.set()
        position = self._current_position_ms()
        self.spirc.set_playing(position)
        await self.spirc.notify()
        # 小米音箱重新拉流 (新 sid 参数防缓存)
        await self.speaker.play_url(self._fresh_url())

    async def on_pause(self):
        if self._index < 0:
            return
        self._pause_event.clear()
        position = self._current_position_ms()
        self.spirc.set_paused(position)
        await self.spirc.notify()
        await self.speaker.pause()

    async def on_seek(self, position_ms: int):
        if self._index < 0:
            return
        await self._play_index(self._index, max(0, position_ms))

    async def on_volume(self, volume: int):
        """volume: SPIRC 0-65535 -> 小米 0-100"""
        pct = round(volume / 65535 * 100)
        self.spirc.set_volume(volume)
        await self.speaker.set_volume(pct)
        await self.spirc.notify()

    async def on_next(self):
        if not self._tracks:
            return
        if self._index + 1 < len(self._tracks):
            await self._play_index(self._index + 1, 0)
        elif self.spirc.repeat:
            await self._play_index(0, 0)
        else:
            await self._stop_playback()

    async def on_prev(self):
        if not self._tracks:
            return
        # cspot 语义: 播放超过 3 秒回到本曲开头, 否则上一首
        if self._index > 0 and self._current_position_ms() < 3000:
            await self._play_index(self._index - 1, 0)
        else:
            await self._play_index(max(0, self._index), 0)

    async def on_deactivate(self):
        """其他设备接管"""
        await self._stop_playback()

    # ============================================================
    # 播放控制
    # ============================================================

    def _fresh_url(self) -> str:
        base = self.audio_server.stream_url.split("?")[0]
        return f"{base}?sid={self._loop.time():.0f}"

    async def _play_index(self, index: int, position_ms: int):
        self._index = index
        ref = self._tracks[index]
        playable_id = self._playable_id_from_ref(ref)
        if playable_id is None:
            log.warning(f"Spotify: 无效曲目引用 (index={index}), 跳过")
            await self._skip_after_failure()
            return

        try:
            loaded = await asyncio.to_thread(self._load_track, playable_id)
        except Exception as e:
            log.error(f"Spotify: 曲目加载失败 (index={index}): {e}")
            await self._skip_after_failure()
            return

        self._consecutive_failures = 0
        self._current_meta = self._extract_meta(loaded)
        meta = self._current_meta
        log.info(
            f"Spotify: 播放 [{index + 1}/{len(self._tracks)}] "
            f"{meta.get('title')} - {meta.get('artist')} @{position_ms}ms"
        )

        self.spirc.playing_track_index = index
        self.spirc.set_playing(position_ms)
        await self.spirc.notify()

        # 启动音频流与解码
        self.audio_server.start_streaming()
        self._start_decode(loaded, position_ms)

        # 触屏歌词: 按歌名/歌手搜小米曲库换 audioID (与 DLNA/AirPlay 路线一致)
        audio_id = None
        if self.config and getattr(self.config, "touchscreen_lyrics", False):
            audio_id = await self._resolve_audio_id(meta)
        await self.speaker.play_url(self.audio_server.stream_url, audio_id)

    async def _skip_after_failure(self):
        self._consecutive_failures += 1
        if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
            log.error(f"Spotify: 连续 {self._consecutive_failures} 首曲目加载失败, 停止播放")
            await self._stop_playback()
            return
        if self._index + 1 < len(self._tracks):
            await self._play_index(self._index + 1, 0)

    async def _resolve_audio_id(self, meta: dict) -> str | None:
        title = (meta.get("title") or "").strip()
        if not title:
            return None
        try:
            audio_id = await asyncio.wait_for(
                self.speaker.search_audio_id(title, meta.get("artist", "")),
                timeout=5.0,
            )
            if audio_id:
                log.info(f"Spotify 歌词匹配命中: {title} -> audioID={audio_id}")
            return audio_id or None
        except Exception as e:
            log.warning(f"Spotify 歌词匹配失败: {e}")
            return None

    async def _stop_playback(self):
        self._generation += 1
        self._pause_event.set()
        self.audio_server.stop_streaming()
        self.spirc.set_stopped()
        await self.spirc.notify()
        try:
            await self.speaker.stop()
        except Exception as e:
            log.warning(f"Spotify: 停止音箱播放失败: {e}")

    def _current_position_ms(self) -> int:
        return int(self._seek_offset_ms + self._bytes_fed / _BYTE_RATE * 1000)

    # ============================================================
    # 解码线程 (cspot TrackPlayer dataCallback 的等价物)
    # ============================================================

    def _start_decode(self, loaded, position_ms: int):
        self._generation += 1
        gen = self._generation
        self._pause_event.set()
        self._bytes_fed = 0
        self._seek_offset_ms = position_ms
        self._decode_thread = threading.Thread(
            target=self._decode_loop,
            args=(loaded, position_ms, gen),
            daemon=True,
            name="spotify-decode",
        )
        self._decode_thread.start()

    def _decode_loop(self, loaded, position_ms: int, gen: int):
        import time

        import av

        container = None
        try:
            input_stream = loaded.input_stream.stream()
            container = av.open(input_stream)
            if position_ms > 0:
                container.seek(position_ms * 1000)  # PyAV: 微秒

            resampler = av.AudioResampler(
                format="s16", layout="stereo", rate=_SAMPLE_RATE
            )
            audio_stream = container.streams.audio[0]
            last_report = 0.0
            ch_bytes = _CHANNELS * _SAMPLE_WIDTH

            for frame in container.decode(audio_stream):
                if gen != self._generation:
                    return
                # 暂停: 停止读取与写入
                while gen == self._generation and not self._pause_event.is_set():
                    self._pause_event.wait(timeout=0.5)
                if gen != self._generation:
                    return

                out = resampler.resample(frame)
                if out is None:
                    continue
                frames = out if isinstance(out, list) else [out]
                # s16 packed 格式: plane[0] 即交错 PCM (与 AirPlay 路线一致)
                for f in frames:
                    mv = memoryview(f.planes[0])
                    pcm = bytes(mv[: f.samples * ch_bytes])
                    if not pcm:
                        continue
                    # 背压: 队列满 (音箱未拉流/跟不上) 时 50ms 轮询等待,
                    # 解码随消费速度推进 (对应 cspot dataCallback 返回 0 的处理)
                    while gen == self._generation:
                        if self.audio_server.write_pcm(pcm):
                            self._bytes_fed += len(pcm)
                            break
                        time.sleep(0.05)
                    if gen != self._generation:
                        return

                # 每秒刷新一次上报位置 (心跳 Notify 帧会带上)
                now = time.monotonic()
                if now - last_report >= 1.0:
                    last_report = now
                    self.spirc.update_position(self._current_position_ms())

            # 正常 EOF -> 曲目播完
            if gen == self._generation:
                asyncio.run_coroutine_threadsafe(self._on_track_end(gen), self._loop)
        except Exception as e:
            if gen == self._generation:
                log.error(f"Spotify: 解码失败: {e}")
        finally:
            if container is not None:
                try:
                    container.close()
                except Exception:
                    pass

    async def _on_track_end(self, gen: int):
        if gen != self._generation:
            return
        if self._index + 1 < len(self._tracks):
            await self._play_index(self._index + 1, 0)
        elif self.spirc.repeat and self._tracks:
            await self._play_index(0, 0)
        else:
            log.info("Spotify: 播放队列已播完")
            await self._stop_playback()

    # ============================================================
    # 状态查询
    # ============================================================

    @property
    def current_meta(self) -> dict:
        return dict(self._current_meta)

    @property
    def is_playing(self) -> bool:
        return self._index >= 0 and self.spirc.status == spirc.kPlayStatusPlay
