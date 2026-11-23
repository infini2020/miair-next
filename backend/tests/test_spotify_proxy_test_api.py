"""Spotify 代理「测试」接口测试

覆盖: 出口 IP 探测 (成功/回退/全失败/超时/异常) 与
POST /api/v1/settings/spotify/proxy-test 的契约 (saved/空串直连/非法 400/未登录 401)。

全部 monkeypatch, 不发起任何真实外网请求。
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import app.engine.airplay  # noqa: F401 -- 先初始化 airplay 包, 单独运行本文件时避免 audio_stream 循环导入
from app.core.config import get_settings
from app.core.security import login_limiter
from app.db import store
from app.engine.spotify import soloist as soloist_mod
from app.main import create_app

# TestClient 不使用 with 语句 → 不执行 lifespan → 不启动 Orchestrator
client = TestClient(create_app())
TEST_IP = "testclient"


@pytest.fixture(autouse=True)
def fresh_state():
    """每个测试前清空用户表与登录限速, 避免跨用例干扰"""
    store.init_db()
    with sqlite3.connect(get_settings().db_path) as conn:
        conn.execute("DELETE FROM users")
    login_limiter.reset(TEST_IP)
    yield


def _auth_headers() -> dict[str, str]:
    resp = client.post(
        "/api/v1/login/setup", json={"username": "admin", "password": "pass1234"}
    )
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


class _FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self, _size: int = -1) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None


class TestParsePublicIp:
    @pytest.mark.parametrize(
        "payload,expected",
        [
            (b'{"ip": "1.2.3.4"}', "1.2.3.4"),  # ipify
            (b'{"ip": "5.6.7.8", "city": "X"}', "5.6.7.8"),  # ipinfo
            (b'{"query": "9.9.9.9", "status": "success"}', "9.9.9.9"),  # ip-api
            (b"not-json", None),
            (b'{"foo": "bar"}', None),
            (b'{"ip": ""}', None),
        ],
    )
    def test_extract_ip(self, payload, expected):
        assert soloist_mod.parse_public_ip(payload) == expected


class TestFetchPublicIp:
    def test_direct_uses_urlopen(self, monkeypatch):
        calls: list[tuple] = []

        def fake_urlopen(request, timeout):
            calls.append((request.full_url, timeout))
            return _FakeResponse(b'{"ip": "203.0.113.7"}')

        monkeypatch.setattr(soloist_mod.urllib.request, "urlopen", fake_urlopen)
        ip = soloist_mod.fetch_public_ip("https://api.ipify.org?format=json", None, 3.0)
        assert ip == "203.0.113.7"
        assert calls == [("https://api.ipify.org?format=json", 3.0)]

    def test_with_opener_uses_opener_open(self):
        calls: list[tuple] = []

        class FakeOpener:
            def open(self, request, timeout):
                calls.append((request.full_url, timeout))
                return _FakeResponse(b'{"query": "198.51.100.9"}')

        ip = soloist_mod.fetch_public_ip("http://ip-api.com/json", FakeOpener(), 2.0)
        assert ip == "198.51.100.9"
        assert calls == [("http://ip-api.com/json", 2.0)]

    def test_missing_ip_raises(self):
        class FakeOpener:
            def open(self, request, timeout):
                return _FakeResponse(b'{"status": "fail"}')

        with pytest.raises(ValueError):
            soloist_mod.fetch_public_ip("http://ip-api.com/json", FakeOpener(), 1.0)


class TestProbeExitIpSync:
    def test_falls_back_until_success(self, monkeypatch):
        attempted: list[str] = []

        def fake_fetch(url, opener, timeout):
            attempted.append(url)
            if url == soloist_mod.IP_PROBE_URLS[-1]:
                return "203.0.113.9"
            raise OSError("blocked")

        monkeypatch.setattr(soloist_mod, "fetch_public_ip", fake_fetch)
        ip, error = soloist_mod.probe_exit_ip_sync("http://127.0.0.1:7890")
        assert ip == "203.0.113.9"
        assert error is None
        assert attempted == list(soloist_mod.IP_PROBE_URLS)

    def test_all_fail_returns_error_summary(self, monkeypatch):
        def fake_fetch(url, opener, timeout):
            raise OSError("connect refused")

        monkeypatch.setattr(soloist_mod, "fetch_public_ip", fake_fetch)
        ip, error = soloist_mod.probe_exit_ip_sync("socks5://127.0.0.1:1080")
        assert ip is None
        assert "connect refused" in (error or "")
        assert soloist_mod.IP_PROBE_URLS[0] in (error or "")

    def test_empty_proxy_skips_opener(self, monkeypatch):
        used: list[object] = []

        def fail_build(proxy):
            used.append(proxy)
            raise AssertionError("直连不应构造代理 opener")

        monkeypatch.setattr(soloist_mod, "build_proxy_opener", fail_build)
        monkeypatch.setattr(
            soloist_mod, "fetch_public_ip", lambda url, opener, timeout: "192.0.2.1"
        )
        ip, error = soloist_mod.probe_exit_ip_sync("")
        assert ip == "192.0.2.1"
        assert error is None
        assert used == []


class TestProbeProxyExitIpContract:
    def test_success(self, monkeypatch):
        monkeypatch.setattr(
            soloist_mod, "probe_exit_ip_sync", lambda proxy: ("203.0.113.5", None)
        )
        result = asyncio.run(soloist_mod.probe_proxy_exit_ip("http://127.0.0.1:7890"))
        assert result["ok"] is True
        assert result["ip"] == "203.0.113.5"
        assert result["via"] == "proxy"
        assert result["error"] is None
        assert isinstance(result["elapsed_ms"], int)

    def test_timeout(self, monkeypatch):
        monkeypatch.setattr(soloist_mod, "PROXY_TEST_TOTAL_TIMEOUT", 0.05)
        monkeypatch.setattr(soloist_mod, "probe_exit_ip_sync", lambda proxy: time.sleep(0.3))
        result = asyncio.run(soloist_mod.probe_proxy_exit_ip("socks5://127.0.0.1:1080"))
        assert result["ok"] is False
        assert result["ip"] is None
        assert result["error"] == "连接超时"
        assert result["via"] == "proxy"

    def test_exception(self, monkeypatch):
        def boom(proxy):
            raise RuntimeError("no route to host")

        monkeypatch.setattr(soloist_mod, "probe_exit_ip_sync", boom)
        result = asyncio.run(soloist_mod.probe_proxy_exit_ip(""))
        assert result["ok"] is False
        assert result["error"] == "no route to host"
        assert result["via"] == "direct"


class TestProxyTestEndpoint:
    @staticmethod
    def _install_config(proxy: str = "http://127.0.0.1:7890"):
        client.app.state.orchestrator = SimpleNamespace(
            config=SimpleNamespace(spotify_proxy=proxy)
        )

    def test_null_uses_saved_proxy(self, monkeypatch):
        seen: list[str] = []

        async def fake_probe(proxy):
            seen.append(proxy)
            return {
                "ok": True,
                "ip": "203.0.113.11",
                "elapsed_ms": 812,
                "via": "proxy",
                "error": None,
            }

        monkeypatch.setattr("app.api.v1.settings.probe_proxy_exit_ip", fake_probe)
        self._install_config("127.0.0.1:7890")
        resp = client.post(
            "/api/v1/settings/spotify/proxy-test",
            json={"proxy": None},
            headers=_auth_headers(),
        )
        assert resp.status_code == 200
        assert seen == ["http://127.0.0.1:7890"]
        assert resp.json() == {
            "ok": True,
            "ip": "203.0.113.11",
            "elapsed_ms": 812,
            "via": "proxy",
            "error": None,
        }

    def test_empty_proxy_tests_direct(self, monkeypatch):
        seen: list[str] = []

        async def fake_probe(proxy):
            seen.append(proxy)
            return {
                "ok": True,
                "ip": "198.51.100.20",
                "elapsed_ms": 5,
                "via": "direct",
                "error": None,
            }

        monkeypatch.setattr("app.api.v1.settings.probe_proxy_exit_ip", fake_probe)
        self._install_config()
        resp = client.post(
            "/api/v1/settings/spotify/proxy-test",
            json={"proxy": ""},
            headers=_auth_headers(),
        )
        assert resp.status_code == 200
        assert seen == [""]
        assert resp.json()["via"] == "direct"

    def test_invalid_proxy_returns_400(self, monkeypatch):
        async def fake_probe(proxy):  # pragma: no cover - 不应被调用
            raise AssertionError("非法代理不应发起探测")

        monkeypatch.setattr("app.api.v1.settings.probe_proxy_exit_ip", fake_probe)
        self._install_config()
        resp = client.post(
            "/api/v1/settings/spotify/proxy-test",
            json={"proxy": "ftp://127.0.0.1:21"},
            headers=_auth_headers(),
        )
        assert resp.status_code == 400
        assert "Spotify 代理地址无效" in resp.json()["detail"]

    def test_requires_auth(self):
        self._install_config()
        resp = client.post(
            "/api/v1/settings/spotify/proxy-test", json={"proxy": ""}
        )
        assert resp.status_code == 401
        assert resp.json()["detail"] == "未登录"
