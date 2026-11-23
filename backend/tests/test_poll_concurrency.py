"""DLNA 状态轮询的并发安全回归测试 (issue: 设备掉线导致热重启永久失效)

缺陷形态 (1c82536 引入的离线隐藏机制放大):
    _poll_speaker_states 直接迭代 self.renderers.items(), 循环体内
    await speaker.get_status() (云端 HTTP, 单台最多 10s)。同一时刻
    Orchestrator 的 15s 在线巡检发现音箱离线, 调用
    device_server.unregister_renderer() -> self.renderers.pop(), 于是
    "dictionary changed size during iteration" (RuntimeError) 从轮询任务抛出。

级联后果 (为什么这是 P0 而非普通缺陷):
    1. device_server.stop() 只 except CancelledError, await self._poll_task
       时把 RuntimeError 原样抛出;
    2. orchestrator._stop_dlna_services 无兜底, 停止链路中断 ——
       SSDP 已停、HTTP 仍在监听、dlna_running 仍为 True;
    3. 之后每次 restart_dlna_services 都撞同一具死任务, 只能重启进程恢复。

本测试用真实 DeviceServer (非 Fake), 只替换 speaker.get_status 的耗时,
确保覆盖真实实现里的迭代与注销时序。
"""

import asyncio

import pytest

from app.engine.dlna import device_server as ds_module
from app.engine.dlna.device_server import DeviceServer
from app.engine.dlna.renderer import DLNARenderer
from app.services.orchestrator import Orchestrator


@pytest.fixture(autouse=True)
def fast_poll(monkeypatch):
    """压缩轮询/清理间隔, 让测试在毫秒级完成多轮巡检

    raising=False: 该常量是随修复引入的, 旧版本不存在。若用默认的
    raising=True, 旧版本会在夹具阶段直接 ERROR, 测试就无法验证真正
    的行为缺陷 (会让回归测试形同虚设)。
    """
    monkeypatch.setattr(ds_module, "_POLL_INTERVAL_SECONDS", 0.02, raising=False)


class SlowSpeaker:
    """模拟云端 get_status 有真实耗时 (真实环境是 HTTP 往返)"""

    def __init__(self, delay: float = 0.05):
        self.did = "did"
        self.delay = delay
        self.status_calls = 0

    async def get_status(self) -> dict:
        self.status_calls += 1
        await asyncio.sleep(self.delay)
        return {"status": 1, "volume": 50}

    async def stop(self) -> bool:
        return True

    async def pause(self) -> bool:
        return True

    async def set_volume(self, volume: int) -> bool:
        return True

    async def get_volume(self) -> int:
        return 50

    async def play_url(self, url: str, audio_id: str | None = None) -> bool:
        return True


def _make_server(count: int, delay: float = 0.05):
    server = DeviceServer("127.0.0.1", 0, None)
    speakers = []
    for i in range(count):
        speaker = SlowSpeaker(delay)
        renderer = DLNARenderer(f"udn-{i}", f"speaker-{i}", speaker)
        renderer.current_uri = "http://example.com/a.mp3"
        server.register_renderer(renderer)
        speakers.append(speaker)
    return server, speakers


async def _cancel_and_await_task(task: asyncio.Task, timeout: float = 2.0) -> None:
    """取消任务并做有界等待; 超时立即判失败, 不让 pytest 永久挂死

    CPython 3.11 的 asyncio.wait_for 在"取消与内层完成同轮"时会吞掉
    CancelledError (Lib/asyncio/tasks.py:463-472), 此时直接 `await task` 永远等不到
    结束 —— 测试进程会无输出地挂死。asyncio.wait 到点即返回 pending, 把偶发
    "永久卡死"变成 2 秒内可见的断言失败。
    """
    task.cancel()
    _done, pending = await asyncio.wait({task}, timeout=timeout)
    if pending:
        task.cancel()  # 补一次取消, 避免 asyncio.run 收尾时再次无界等待
        raise AssertionError(
            f"轮询任务未在 {timeout:g} 秒内结束 (取消被吞?): CPython 3.11 的 "
            "asyncio.wait_for 会吞掉 CancelledError (Lib/asyncio/tasks.py:463-472)"
        )
    with pytest.raises(asyncio.CancelledError):
        await task


def test_poll_once_tolerates_concurrent_unregister():
    """迭代中注销渲染器不得抛 RuntimeError (原缺陷的直接复现)"""

    async def run():
        server, _ = _make_server(4)
        task = asyncio.create_task(server._poll_once())
        # 等第一个音箱进入 get_status 的 await, 再注销另一个
        await asyncio.sleep(0.01)
        await server.unregister_renderer("udn-2")
        await task  # 修复前: RuntimeError dictionary changed size during iteration

    asyncio.run(run())


def test_poll_task_survives_unregister_and_keeps_polling():
    """轮询任务须长活: 设备注销后任务仍在运行并继续同步其余设备"""

    async def run():
        server, speakers = _make_server(3)
        task = asyncio.create_task(server._poll_speaker_states())
        await asyncio.sleep(0.01)
        await server.unregister_renderer("udn-1")
        await asyncio.sleep(0.3)  # 多跑几轮 (间隔被测试压缩)
        alive = not task.done()
        await _cancel_and_await_task(task)
        return alive, speakers

    alive, speakers = asyncio.run(run())
    assert alive, "轮询任务被异常杀死 (修复前正是此表现)"
    assert any(s.status_calls > 0 for s in speakers)


def test_single_round_failure_does_not_kill_poll_task():
    """单轮异常只丢一轮, 不得让任务退出 (否则状态同步永久失效)"""

    async def run():
        server, _ = _make_server(1)
        original = server._poll_once
        calls = {"n": 0}

        async def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("模拟单轮内部异常")
            await original()

        server._poll_once = flaky
        task = asyncio.create_task(server._poll_speaker_states())
        await asyncio.sleep(0.3)
        alive = not task.done()
        await _cancel_and_await_task(task)
        return alive, calls["n"]

    alive, n = asyncio.run(run())
    assert alive, "单轮异常导致轮询任务退出"
    assert n >= 2, "首轮失败后未继续下一轮"


def test_stop_is_not_interrupted_by_poll_task_error():
    """stop() 不得因轮询任务异常而中断后续清理 (原缺陷的级联点)"""

    async def run():
        server, _ = _make_server(2)
        # 造一个"已带异常结束"的轮询任务, 复刻崩溃后的现场
        task = asyncio.create_task(server._poll_speaker_states())
        server._poll_task = task
        await _cancel_and_await_task(task)
        # 让任务恢复成抛 RuntimeError 的结束态
        server._poll_task = asyncio.create_task(_raise_runtime_error())
        await asyncio.sleep(0)
        await server.stop()  # 修复前: RuntimeError 直接上抛
        return True

    asyncio.run(run())


async def _raise_runtime_error():
    raise RuntimeError("dictionary changed size during iteration")


def test_orchestrator_stop_services_isolates_step_failure():
    """orchestrator._stop_dlna_services 单步失败必须继续并复位 dlna_running"""

    class BrokenSSDP:
        async def stop(self):
            raise RuntimeError("ssdp boom")

    class BrokenDevice:
        async def stop(self):
            raise RuntimeError("device boom")

    orch = Orchestrator.__new__(Orchestrator)
    orch._visibility_task = None
    orch._offline_speakers = set()
    orch.ssdp_server = BrokenSSDP()
    orch.device_server = BrokenDevice()
    orch.renderers = {"udn": object()}
    orch._did_to_udn = {"did": "udn"}
    orch.dlna_running = True

    asyncio.run(orch._stop_dlna_services())

    assert orch.dlna_running is False, "停止失败后 dlna_running 未复位 (半死状态)"
    assert orch.ssdp_server is None
    assert orch.device_server is None
    assert orch.renderers == {}
