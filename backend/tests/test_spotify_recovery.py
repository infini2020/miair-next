"""Spotify 会话恢复 / 错误分类 / 凭据管理测试

回归场景 (2026-08-31 日志事故):
- 网络类登录失败 (ConnectionResetError 等 str() 为空的异常) 曾被当成
  认证失败, 导致有效凭据被误删
- SPIRC 启动失败后残留半死会话, 心跳持续撞死连接
- 会话缺失后无法自动恢复, 需要用户重新配对
"""

import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest

from app.engine.spotify import manager as manager_mod
from app.engine.spotify.errors import exc_desc, is_network_error
from app.engine.spotify.manager import SpeakerSpotify


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def sp(tmp_path):
    """不依赖真实网络/音箱的 SpeakerSpotify 实例"""
    speaker = MagicMock()
    speaker.did = "289933540"
    speaker.get_dlna_name.return_value = "Test Speaker"
    controller = MagicMock()
    controller.speaker = speaker
    config = MagicMock()
    config.conf_path = str(tmp_path)
    return SpeakerSpotify("127.0.0.1", controller, config=config)


class TestErrorUtils:
    def test_exc_desc_includes_type_name_for_empty_message(self):
        """空消息异常 (ConnectionResetError) 也应有可读描述"""
        assert exc_desc(ConnectionResetError()) == "ConnectionResetError"

    def test_exc_desc_with_message(self):
        assert exc_desc(ValueError("boom")) == "ValueError: boom"

    def test_is_network_error(self):
        assert is_network_error(ConnectionResetError())
        assert is_network_error(TimeoutError())
        assert not is_network_error(RuntimeError("BadCredentials"))


class TestLoginFailureHandling:
    def test_network_error_preserves_credentials(self, sp):
        """网络类登录失败: 保留凭据, 等待自动恢复"""
        async def inner():
            cred_file = sp._credentials_file
            os.makedirs(os.path.dirname(cred_file), exist_ok=True)
            with open(cred_file, "w") as f:
                f.write("{}")
            with patch.object(sp, "_start_session",
                              side_effect=ConnectionResetError()):
                await sp._handle_credentials("user", b"blob")
            assert os.path.isfile(cred_file)
        _run(inner())

    def test_auth_error_removes_credentials(self, sp):
        """认证类登录失败: 清除凭据, 等待重新配对"""
        async def inner():
            cred_file = sp._credentials_file
            os.makedirs(os.path.dirname(cred_file), exist_ok=True)
            with open(cred_file, "w") as f:
                f.write("{}")
            with patch.object(sp, "_start_session",
                              side_effect=RuntimeError("BadCredentials")):
                await sp._handle_credentials("user", b"blob")
            assert not os.path.isfile(cred_file)
        _run(inner())

    def test_spirc_start_failure_cleans_session(self, sp):
        """SPIRC 启动失败: 释放半死会话"""
        async def inner():
            sp._create_session = MagicMock(return_value=MagicMock())
            spirc = MagicMock()
            spirc.start = MagicMock(side_effect=ConnectionResetError())
            with patch.object(manager_mod, "SpotifyPlayer"), \
                 patch.object(manager_mod, "SpircController", return_value=spirc):
                with pytest.raises(ConnectionResetError):
                    await sp._start_session("user", b"blob")
            assert sp.session is None
            assert sp.spirc is None
        _run(inner())


class TestHeartbeatRecovery:
    def test_recovers_session_from_stored_credentials(self, sp):
        """会话缺失但有存储凭据: 心跳循环自动重登"""
        async def inner():
            cred_file = sp._credentials_file
            os.makedirs(os.path.dirname(cred_file), exist_ok=True)
            with open(cred_file, "w") as f:
                f.write("{}")

            started = asyncio.Event()

            async def fake_start(username=None, blob=None):
                sp.session = MagicMock()
                started.set()

            with patch.object(manager_mod, "_HEARTBEAT_INTERVAL", 0), \
                 patch.object(sp, "_start_session", side_effect=fake_start):
                task = asyncio.create_task(sp._heartbeat_loop())
                await asyncio.wait_for(started.wait(), timeout=2)
                sp._closed = True
                await asyncio.sleep(0.1)
            assert task.done()
            assert task.exception() is None
            assert sp.session is not None
        _run(inner())

    def test_rebuilds_after_consecutive_failures(self, sp):
        """连续心跳失败达到阈值: 释放会话等待下一轮自动重建"""
        async def inner():
            session = MagicMock()
            session.is_valid.return_value = True
            sp.session = session
            spirc = MagicMock()
            spirc.heartbeat = MagicMock(side_effect=ConnectionResetError())
            sp.spirc = spirc

            closed = []

            async def fake_close():
                closed.append(True)
                sp.spirc = None
                sp.session = None

            with patch.object(manager_mod, "_HEARTBEAT_INTERVAL", 0), \
                 patch.object(sp, "_close_session", side_effect=fake_close):
                task = asyncio.create_task(sp._heartbeat_loop())
                for _ in range(60):
                    if closed:
                        break
                    await asyncio.sleep(0.05)
                sp._closed = True
                await asyncio.sleep(0.1)
            assert closed
            assert sp.spirc is None
        _run(inner())
