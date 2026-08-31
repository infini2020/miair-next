"""Spotify 音频流服务器测试: WAV 头 / 流式读取 / 背压协议"""

import asyncio
import struct

import aiohttp
import pytest

from app.engine.spotify.audio_server import SpotifyAudioServer

pytestmark = pytest.mark.anyio

_SAMPLE_RATE = 44100
_HEADER_FMT = "<4sI4s4sIHHIIHH4sI"


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _pcm_chunk(frames: int = 4410) -> bytes:
    return b"\x00\x01" * frames  # 2ch * 2B, 0.1s 静音


async def test_wav_header_and_streaming():
    server = SpotifyAudioServer("127.0.0.1", 0, audio_format="wav")
    await server.start()
    assert "/spotify/stream.wav?sid=" in server.stream_url
    server.start_streaming()
    url = server.stream_url

    # 后台以生产者速度写入
    async def produce():
        for _ in range(20):
            server.write_pcm(_pcm_chunk())

    prod = asyncio.create_task(produce())

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Type"].startswith("audio/wav")

            hdr = await resp.content.readexactly(44)
            (riff, _, wave, fmt, _, ac, ch, rate, byterate, ba, bits, data,
             data_sz) = struct.unpack(_HEADER_FMT, hdr)
            assert riff == b"RIFF" and wave == b"WAVE" and fmt == b"fmt "
            assert ac == 1 and ch == 2 and rate == _SAMPLE_RATE
            assert bits == 16 and ba == 4
            assert byterate == _SAMPLE_RATE * 4
            assert data == b"data" and data_sz == 0x7FFFFF00  # 流式: 未知长度占位

            await prod
            server.stop_streaming()  # 结束流 (模拟曲目播完)
            body = await resp.content.read()
            assert len(body) >= 4410 * 4 * 4  # 至少收到若干 chunk

    await server.stop()
    assert not server._active


async def test_backpressure_returns_false_when_full():
    server = SpotifyAudioServer("127.0.0.1", 0, audio_format="wav")
    await server.start()
    server.start_streaming()

    try:
        # 无人拉流: 队列写满 (_QUEUE_MAXSIZE=300) 后 write_pcm 应返回 False
        full = False
        for _ in range(400):
            if not server.write_pcm(_pcm_chunk()):
                full = True
                break
        assert full, "队列满后 write_pcm 应返回 False 触发解码端等待"

        # 消费者拉流后队列腾空, 恢复可写
        async with aiohttp.ClientSession() as session:
            async with session.get(
                server.stream_url, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                drained = 0
                while drained < 4410 * 4 * 4:
                    chunk = await resp.content.read(16384)
                    if not chunk:
                        break
                    drained += len(chunk)
        assert drained > 0

        ok = False
        for _ in range(10):
            if server.write_pcm(_pcm_chunk()):
                ok = True
                break
        assert ok, "消费后 write_pcm 应恢复返回 True"
    finally:
        await server.stop()


async def test_write_after_stop_is_noop():
    server = SpotifyAudioServer("127.0.0.1", 0, audio_format="wav")
    await server.start()
    await server.stop()
    assert server.write_pcm(_pcm_chunk()) is True  # 停止后静默丢弃
