"""Spotify 音频 HTTP 流服务器

参考 SpotConnect 的 HTTPstreamer：将解码后的 PCM 音频通过本地 HTTP 流
提供给小爱音箱拉流播放。两种输出格式：
- WAV: 零编码延迟直接输出 PCM，适用于大多数音箱
- MP3: PyAV (libmp3lame) 实时转码，用于不支持 WAV 的音箱
       (L05B/L05C/LX06/L16A 等, 与 AirPlay 路线的处理一致)
"""

import asyncio
import logging
import queue
import struct
import threading

from aiohttp import web

log = logging.getLogger("miair")

_QUEUE_MAXSIZE = 300  # ~2.7s 的 44.1kHz 立体声 16bit 缓冲


class SpotifyAudioServer:
    """HTTP 音频流服务器 (接收 PCM，输出 WAV 或 MP3)"""

    def __init__(self, hostname: str, port: int = 0, audio_format: str = "wav"):
        self.hostname = hostname
        self.port = port
        self._audio_format = audio_format  # "wav" or "mp3"
        self._app = web.Application()
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

        self._audio_queue: queue.Queue[bytes | None] = queue.Queue(maxsize=_QUEUE_MAXSIZE)
        self._sample_rate = 44100
        self._channels = 2
        self._sample_width = 2  # 16-bit
        self._active = False
        self._session_id = 0
        self._client_count = 0
        self._client_lock = threading.Lock()
        # MP3 模式: 编码线程输出队列
        self._mp3_queue: queue.Queue[bytes | None] = queue.Queue(maxsize=_QUEUE_MAXSIZE)
        self._encoder_thread: threading.Thread | None = None
        self._encoder_stop = threading.Event()

        self._setup_routes()

    def _setup_routes(self):
        if self._audio_format == "mp3":
            self._app.router.add_get("/spotify/stream.mp3", self._handle_stream_mp3)
        else:
            self._app.router.add_get("/spotify/stream.wav", self._handle_stream_wav)

    @property
    def stream_url(self) -> str:
        ext = "mp3" if self._audio_format == "mp3" else "wav"
        return f"http://{self.hostname}:{self.port}/spotify/stream.{ext}?sid={self._session_id}"

    @property
    def has_clients(self) -> bool:
        """是否有音箱正在拉取音频流"""
        with self._client_lock:
            return self._client_count > 0

    async def start(self):
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await self._site.start()
        self.port = self._site._server.sockets[0].getsockname()[1]
        log.info(f"Spotify 音频流服务器: http://{self.hostname}:{self.port} (格式: {self._audio_format})")

    async def stop(self):
        self._active = False
        self._encoder_stop.set()
        self._put_sentinel(self._audio_queue)
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        if self._encoder_thread is not None:
            self._encoder_thread.join(timeout=3)
            self._encoder_thread = None

    @staticmethod
    def _put_sentinel(q: queue.Queue):
        try:
            q.put_nowait(None)
        except queue.Full:
            pass

    def set_audio_params(self, sample_rate: int, channels: int, sample_width: int = 2):
        self._sample_rate = sample_rate
        self._channels = channels
        self._sample_width = sample_width

    def start_streaming(self):
        """开始新的播放会话 (新 session_id 使 URL 变化, 音箱重新拉流)"""
        import time

        self._active = True
        self._session_id = time.monotonic_ns() & 0x7FFFFFFF
        self._drain_queue(self._audio_queue)
        self._drain_queue(self._mp3_queue)
        self._encoder_stop.clear()
        if self._audio_format == "mp3" and self._encoder_thread is None:
            self._encoder_thread = threading.Thread(
                target=self._encoder_loop, daemon=True, name="spotify-mp3-encoder"
            )
            self._encoder_thread.start()
        log.info("Spotify 音频流: 开始接收 PCM 数据")

    def stop_streaming(self):
        self._active = False
        self._drain_queue(self._audio_queue)
        self._drain_queue(self._mp3_queue)
        self._put_sentinel(self._audio_queue)
        log.info("Spotify 音频流: 停止接收 PCM 数据")

    @staticmethod
    def _drain_queue(q: queue.Queue):
        while True:
            try:
                q.get_nowait()
            except queue.Empty:
                break

    def write_pcm(self, data: bytes) -> bool:
        """写入 PCM 音频数据 (解码线程调用)

        返回 False 表示缓冲队列已满 (音箱尚未拉流或拉流速度跟不上)。
        参考 cspot TrackPlayer 的 dataCallback 协议: 返回 0 时解码方
        sleep 50ms 重试, 由消费速度给解码配速, 避免溢出丢音。
        """
        if not self._active:
            return True  # 会话已停止, 上层用 generation 使解码线程退出
        try:
            self._audio_queue.put_nowait(data)
            return True
        except queue.Full:
            return False

    # ============================================================
    # WAV 模式 — 直接输出 PCM
    # ============================================================

    def _build_wav_header(self, data_size: int = 0x7FFFFF00) -> bytes:
        byte_rate = self._sample_rate * self._channels * self._sample_width
        block_align = self._channels * self._sample_width
        bits_per_sample = self._sample_width * 8
        return struct.pack(
            '<4sI4s4sIHHIIHH4sI',
            b'RIFF', data_size + 36, b'WAVE',
            b'fmt ', 16, 1, self._channels,
            self._sample_rate, byte_rate, block_align, bits_per_sample,
            b'data', data_size,
        )

    async def _handle_stream_wav(self, request: web.Request) -> web.StreamResponse:
        if not self._active:
            return web.Response(status=404, headers={"Connection": "close"})

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "audio/wav",
                "Cache-Control": "no-cache, no-store",
                "Pragma": "no-cache",
                "Connection": "close",
                "Transfer-Encoding": "chunked",
            },
        )
        await response.prepare(request)

        with self._client_lock:
            self._client_count += 1
        conn_abort = threading.Event()
        log.info("Spotify: 音箱开始拉取 WAV 音频流")

        loop = asyncio.get_event_loop()
        data_ready = asyncio.Event()
        pending_data: list[bytes] = []
        data_lock = threading.Lock()
        writer_done = False

        def _reader_thread():
            nonlocal writer_done
            try:
                while self._active and not conn_abort.is_set():
                    try:
                        chunk = self._audio_queue.get(timeout=0.02)
                        if chunk is None:
                            break
                        local_batch = [chunk]
                        for _ in range(31):
                            try:
                                extra = self._audio_queue.get_nowait()
                                if extra is None:
                                    break
                                local_batch.append(extra)
                            except queue.Empty:
                                break
                        with data_lock:
                            pending_data.extend(local_batch)
                        loop.call_soon_threadsafe(data_ready.set)
                    except queue.Empty:
                        continue
            except Exception as e:
                log.error(f"Spotify WAV reader 异常: {e}")
            finally:
                writer_done = True
                loop.call_soon_threadsafe(data_ready.set)

        reader = threading.Thread(target=_reader_thread, daemon=True)
        reader.start()

        try:
            await response.write(self._build_wav_header())
            while not writer_done:
                await data_ready.wait()
                data_ready.clear()
                with data_lock:
                    chunks = pending_data
                    pending_data = []
                if chunks:
                    await response.write(b"".join(chunks))
        except (ConnectionResetError, BrokenPipeError):
            log.info("Spotify: 音箱断开 WAV 音频流连接")
        except Exception:
            pass
        finally:
            conn_abort.set()
            with self._client_lock:
                self._client_count -= 1
        try:
            await response.write_eof()
        except Exception:
            pass
        return response

    # ============================================================
    # MP3 模式 — PyAV (libmp3lame) 实时转码
    # ============================================================

    def _encoder_loop(self):
        """独立编码线程: PCM 队列 -> MP3 队列"""
        import av
        import numpy as np

        try:
            codec = av.codec.Codec("libmp3lame", "w").create()
            codec.sample_rate = self._sample_rate
            codec.format = "s16"
            codec.layout = "stereo"
            codec.bit_rate = 128_000
            codec.open()

            samples_per_frame = 1152  # MP3 帧粒度
            pcm_buffer = bytearray()
            byte_rate = self._sample_rate * self._channels * self._sample_width

            while not self._encoder_stop.is_set():
                try:
                    chunk = self._audio_queue.get(timeout=0.05)
                except queue.Empty:
                    continue
                if chunk is None:
                    break
                pcm_buffer.extend(chunk)
                # 溢出保护: 编码跟不上时丢弃旧数据
                if len(pcm_buffer) > byte_rate * 5:
                    del pcm_buffer[: len(pcm_buffer) - byte_rate]

                need = samples_per_frame * byte_rate // self._sample_rate
                while len(pcm_buffer) >= need:
                    frame_data = bytes(pcm_buffer[:need])
                    del pcm_buffer[:need]
                    self._encode_frame(codec, frame_data, samples_per_frame)

            # flush 编码器
            try:
                for packet in codec.encode(None):
                    self._push_mp3(packet.to_bytes())
            except Exception:
                pass
        except Exception as e:
            log.error(f"Spotify MP3 编码线程异常: {e}")
        finally:
            self._put_sentinel(self._mp3_queue)

    def _encode_frame(self, codec, frame_data: bytes, samples: int):
        import av
        import numpy as np

        arr = np.frombuffer(frame_data, dtype=np.int16).reshape(samples, self._channels)
        frame = av.AudioFrame.from_ndarray(arr, format="s16", layout="stereo")
        frame.sample_rate = self._sample_rate
        for packet in codec.encode(frame):
            self._push_mp3(packet.to_bytes())

    def _push_mp3(self, data: bytes):
        if not self._active:
            return
        try:
            self._mp3_queue.put_nowait(data)
        except queue.Full:
            try:
                self._mp3_queue.get_nowait()
                self._mp3_queue.put_nowait(data)
            except (queue.Empty, queue.Full):
                pass

    async def _handle_stream_mp3(self, request: web.Request) -> web.StreamResponse:
        if not self._active:
            return web.Response(status=404, headers={"Connection": "close"})

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "audio/mpeg",
                "Cache-Control": "no-cache, no-store",
                "Pragma": "no-cache",
                "Connection": "close",
            },
        )
        await response.prepare(request)

        with self._client_lock:
            self._client_count += 1
        log.info("Spotify: 音箱开始拉取 MP3 音频流")

        try:
            while self._active:
                data = await asyncio.to_thread(self._mp3_queue.get, True, 0.2)
                if data is None:
                    break
                await response.write(data)
        except (ConnectionResetError, BrokenPipeError):
            log.info("Spotify: 音箱断开 MP3 音频流连接")
        except Exception:
            pass
        finally:
            with self._client_lock:
                self._client_count -= 1
        try:
            await response.write_eof()
        except Exception:
            pass
        return response
