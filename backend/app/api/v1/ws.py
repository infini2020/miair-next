"""WebSocket: 实时推送日志与音箱状态 (连接需带 ?token=)"""

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.logging import ring_handler
from app.core.security import decode_access_token

log = logging.getLogger("miair")

router = APIRouter()

STATUS_INTERVAL = 3  # 状态推送间隔 (秒)
TOKEN_RECHECK_INTERVAL = 60  # token 过期复验间隔 (秒)
LOG_REPLAY = 300  # 新连接重放的日志条数 (页面打开前的日志也可见)


def _collect_status(orch) -> dict:
    speakers = []
    for did, controller in orch.speaker_manager.controllers.items():
        renderer = orch.get_renderer_by_did(did)
        airplay_active = False
        if orch.airplay_manager:
            sap = orch.airplay_manager.speaker_airplays.get(did)
            if sap and sap.airplay_server and sap.airplay_server.is_playing:
                airplay_active = True
        spotify_paired = False
        spotify_playing = False
        spotify_track = ""
        spotify_artist = ""
        if orch.spotify_manager:
            ss = orch.spotify_manager.speaker_spotify.get(did)
            if ss:
                st = ss.status()
                spotify_paired = st["spotify_paired"]
                spotify_playing = st["spotify_playing"]
                spotify_track = st["spotify_track"]
                spotify_artist = st["spotify_artist"]
        speakers.append({
            "did": did,
            "dlna_name": controller.speaker.get_dlna_name(),
            "transport_state": renderer.transport_state if renderer else "UNKNOWN",
            "current_uri": renderer.current_uri if renderer else "",
            "airplay_active": airplay_active,
            "spotify_paired": spotify_paired,
            "spotify_playing": spotify_playing,
            "spotify_track": spotify_track,
            "spotify_artist": spotify_artist,
        })
    return {
        "type": "status",
        "dlna_running": orch.dlna_running,
        "renderers_count": len(orch.renderers),
        "speakers": speakers,
    }


async def _send_json(ws: WebSocket, obj: dict):
    await ws.send_text(json.dumps(obj, ensure_ascii=False))


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = ""):
    # WebSocket 无法带 Header, 通过查询参数校验 token
    if not decode_access_token(token):
        await websocket.close(code=4401)
        return

    await websocket.accept()
    orch = websocket.app.state.orchestrator

    # 先快照再订阅: 重放历史 + 增量推送构成完整日志视图
    # (snapshot 与 emit 由 RingBufferHandler 内部锁串行, 订阅方不重不漏)
    replay = ring_handler.snapshot()[-LOG_REPLAY:]
    log_queue = ring_handler.subscribe()

    async def push_logs():
        try:
            for line in replay:
                await _send_json(websocket, {"type": "log", "line": line})
            while True:
                line = await log_queue.get()
                await _send_json(websocket, {"type": "log", "line": line})
        except Exception:
            # 发送失败 (客户端已断开) 时关闭连接让主循环退出,
            # 前端自动重连重建订阅 —— 避免协程静默死亡后日志流永久中断
            # 而 status 仍在推送的"日志不实时"假象
            try:
                await websocket.close()
            except Exception:
                pass

    async def push_status():
        try:
            while True:
                await _send_json(websocket, _collect_status(orch))
                await asyncio.sleep(STATUS_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await websocket.close()
            except Exception:
                pass

    async def check_token():
        # 连接建立后 token 仍可能过期, 周期复验; 失效则主动关闭 (4401)
        while True:
            await asyncio.sleep(TOKEN_RECHECK_INTERVAL)
            if not decode_access_token(token):
                await websocket.close(code=4401)
                return

    tasks = [
        asyncio.create_task(push_logs()),
        asyncio.create_task(push_status()),
        asyncio.create_task(check_token()),
    ]
    try:
        # 保持接收循环以感知客户端断开
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        for t in tasks:
            t.cancel()
        ring_handler.unsubscribe(log_queue)
