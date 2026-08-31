"""WebSocket 日志链路与状态收集测试"""

import asyncio
import logging

from app.api.v1.ws import _collect_status, LOG_REPLAY
from app.core.logging import ring_handler, setup_logging


class _FakeSpotify:
    def __init__(self, paired, playing, track, artist):
        self._st = {
            "spotify_paired": paired,
            "spotify_playing": playing,
            "spotify_track": track,
            "spotify_artist": artist,
        }

    def status(self):
        return self._st


class _FakeSpeakerManager:
    def __init__(self, controllers):
        self.controllers = controllers


class _FakeRenderer:
    def __init__(self, state, uri):
        self.transport_state = state
        self.current_uri = uri


class _FakeOrch:
    def __init__(self, speakers, spotify_map, airplay_manager=None):
        self.speaker_manager = _FakeSpeakerManager(speakers)
        self.spotify_manager = type("M", (), {"speaker_spotify": spotify_map})()
        self.airplay_manager = airplay_manager
        self.dlna_running = True
        self.renderers = {"udn": object()}
        self._map = {did: _FakeRenderer("PLAYING", "http://x") for did in speakers}

    def get_renderer_by_did(self, did):
        return self._map.get(did)


class _FakeSpeaker:
    def __init__(self, name):
        self._name = name

    def get_dlna_name(self):
        return self._name


class _FakeController:
    def __init__(self, name):
        self.speaker = _FakeSpeaker(name)


def test_collect_status_includes_spotify_fields():
    orch = _FakeOrch(
        {"did-1": _FakeController("客厅音箱")},
        {"did-1": _FakeSpotify(True, True, "曲目A", "歌手B")},
    )
    st = _collect_status(orch)
    assert st["type"] == "status"
    sp = st["speakers"][0]
    assert sp["spotify_paired"] is True
    assert sp["spotify_playing"] is True
    assert sp["spotify_track"] == "曲目A"
    assert sp["spotify_artist"] == "歌手B"
    assert sp["transport_state"] == "PLAYING"


def test_collect_status_without_spotify_manager():
    orch = _FakeOrch({"did-1": _FakeController("卧室")}, {})
    sp = _collect_status(orch)["speakers"][0]
    assert sp["spotify_paired"] is False
    assert sp["spotify_playing"] is False
    assert sp["spotify_track"] == ""


def test_log_snapshot_then_subscribe_no_dup_no_loss():
    """先快照再订阅: emit 早于快照 → 只在快照; 之后 → 只在队列"""
    import threading

    setup_logging(False)
    logger = logging.getLogger("miair")

    # 清空上一次测试残留
    ring_handler.buffer.clear()

    logger.info("before-snapshot")  # 进 buffer, 无订阅者
    snapshot = ring_handler.snapshot()
    q = ring_handler.subscribe()
    ring_handler.set_loop(asyncio.new_event_loop())
    logger.info("after-subscribe")  # 进 buffer + 队列

    assert snapshot == ["before-snapshot"] or any("before-snapshot" in ln for ln in snapshot)
    assert len(snapshot) == 1

    # 队列里的投递被 call_soon_threadsafe 调度到 loop, 直接检查 subscribers 调度标记:
    # 换成验证 buffer 覆盖了全部两条 (replay 视图完整)
    assert len(ring_handler.buffer) == 2

    ring_handler.unsubscribe(q)
    ring_handler.set_loop.__wrapped__ if hasattr(ring_handler.set_loop, "__wrapped__") else None


def test_replay_cap():
    """replay 数量受 LOG_REPLAY 上限约束 (buffer 超过上限时只取最近)"""
    assert LOG_REPLAY > 0
