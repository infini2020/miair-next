"""Spotify 代理测试: URL 规范化 / 下载 opener / Soloist 进程环境变量 / 设置接口"""

from __future__ import annotations

import asyncio
import urllib.request
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import BackgroundTasks, HTTPException

from app.api.v1.settings import save_settings_api
from app.engine.config import Config
from app.engine.spotify.soloist import (
    LOCAL_NO_PROXY,
    ProxyConfigError,
    SoloistBinary,
    SoloistProcess,
    build_proxy_opener,
    normalize_spotify_proxy,
    proxy_env,
)
from app.models.schemas import SettingsPayload


class TestNormalizeSpotifyProxy:
    def test_bare_host_port_defaults_to_http(self):
        assert normalize_spotify_proxy("127.0.0.1:7890") == "http://127.0.0.1:7890"

    def test_http_and_https_kept(self):
        assert normalize_spotify_proxy("http://127.0.0.1:7890") == "http://127.0.0.1:7890"
        assert normalize_spotify_proxy("https://proxy.local:8443") == "https://proxy.local:8443"

    def test_socks5_variants_kept(self):
        assert normalize_spotify_proxy("socks5://127.0.0.1:1080") == "socks5://127.0.0.1:1080"
        assert normalize_spotify_proxy("socks5h://127.0.0.1:1080") == "socks5h://127.0.0.1:1080"

    def test_auth_info_kept_and_path_dropped(self):
        assert (
            normalize_spotify_proxy("socks5h://u:p@127.0.0.1:1080/")
            == "socks5h://u:p@127.0.0.1:1080"
        )

    def test_empty_means_direct(self):
        assert normalize_spotify_proxy("") == ""
        assert normalize_spotify_proxy("   ") == ""
        assert normalize_spotify_proxy(None) == ""

    @pytest.mark.parametrize(
        "value",
        [
            "ftp://127.0.0.1:21",
            "http://",
            "http://host:abc",
            "http://bad host:80",
            "socks4://127.0.0.1:1080",
        ],
    )
    def test_invalid_rejected(self, value):
        with pytest.raises(ProxyConfigError):
            normalize_spotify_proxy(value)


class TestProxyOpener:
    def test_http_proxy_uses_urllib_proxy_handler(self):
        opener = build_proxy_opener("127.0.0.1:7890")
        handlers = [h for h in opener.handlers if isinstance(h, urllib.request.ProxyHandler)]
        assert handlers, "http 代理应使用 urllib ProxyHandler"
        assert handlers[0].proxies == {
            "http": "http://127.0.0.1:7890",
            "https": "http://127.0.0.1:7890",
        }

    def test_socks5_proxy_uses_pysocks_handler(self):
        import socks
        from sockshandler import SocksiPyHandler

        opener = build_proxy_opener("socks5://u:p@127.0.0.1:1080")
        handler = next(h for h in opener.handlers if isinstance(h, SocksiPyHandler))
        assert handler.args == (socks.SOCKS5, "127.0.0.1", 1080)
        assert handler.kw["rdns"] is False
        assert handler.kw["username"] == "u"
        assert handler.kw["password"] == "p"

    def test_socks5h_uses_remote_dns(self):
        from sockshandler import SocksiPyHandler

        opener = build_proxy_opener("socks5h://127.0.0.1:1080")
        handler = next(h for h in opener.handlers if isinstance(h, SocksiPyHandler))
        assert handler.kw["rdns"] is True


class TestProxyEnv:
    def test_http_proxy_env_upper_and_lower(self):
        env = proxy_env("127.0.0.1:7890")
        proxy = "http://127.0.0.1:7890"
        assert env["HTTP_PROXY"] == proxy
        assert env["http_proxy"] == proxy
        assert env["HTTPS_PROXY"] == proxy
        assert env["https_proxy"] == proxy
        assert env["NO_PROXY"] == LOCAL_NO_PROXY
        assert env["no_proxy"] == LOCAL_NO_PROXY
        for token in ("127.0.0.1", "localhost", "::1"):
            assert token in env["NO_PROXY"]

    def test_socks5_proxy_env_uses_all_proxy(self):
        env = proxy_env("socks5://127.0.0.1:1080")
        proxy = "socks5://127.0.0.1:1080"
        assert env["ALL_PROXY"] == proxy
        assert env["all_proxy"] == proxy
        assert env["HTTPS_PROXY"] == proxy
        assert env["https_proxy"] == proxy
        assert env["NO_PROXY"] == LOCAL_NO_PROXY
        assert "HTTP_PROXY" not in env

    def test_empty_proxy_only_sets_no_proxy(self):
        assert proxy_env("") == {"NO_PROXY": LOCAL_NO_PROXY, "no_proxy": LOCAL_NO_PROXY}

    def test_invalid_proxy_degrades_to_direct(self):
        assert proxy_env("ftp://127.0.0.1:21") == {
            "NO_PROXY": LOCAL_NO_PROXY,
            "no_proxy": LOCAL_NO_PROXY,
        }


class TestSoloistProcessProxyEnv:
    @staticmethod
    def _process(proxy: str) -> SoloistProcess:
        binary = SimpleNamespace(path="/nonexistent/soloist")
        return SoloistProcess(
            binary,
            "/tmp/miair-spotify-proxy-test",
            "api-key",
            "Test Device",
            12345,
            env={"PATH": "/usr/bin"},
            proxy=proxy,
        )

    def test_http_proxy_injected_into_subprocess_env(self):
        proc = self._process("127.0.0.1:7890")
        assert proc.env["HTTP_PROXY"] == "http://127.0.0.1:7890"
        assert proc.env["HTTPS_PROXY"] == "http://127.0.0.1:7890"
        assert proc.env["PATH"] == "/usr/bin", "原有 env 必须保留"
        assert proc.env["NO_PROXY"] == LOCAL_NO_PROXY

    def test_socks5_proxy_injected_into_subprocess_env(self):
        proc = self._process("socks5://127.0.0.1:1080")
        assert proc.env["ALL_PROXY"] == "socks5://127.0.0.1:1080"
        assert proc.env["all_proxy"] == "socks5://127.0.0.1:1080"
        assert proc.env["HTTPS_PROXY"] == "socks5://127.0.0.1:1080"
        assert proc.env["NO_PROXY"] == LOCAL_NO_PROXY


class TestSoloistDownloadProxy:
    def test_download_uses_proxy_path_when_configured(self, tmp_path, monkeypatch):
        binary = SoloistBinary(str(tmp_path), proxy="127.0.0.1:7890")
        calls: list[tuple] = []

        def fake_via_proxy(url, dest, proxy):
            calls.append((url, dest, proxy))

        monkeypatch.setattr(binary, "_download_via_proxy", fake_via_proxy)
        binary._download_to("https://example.com/soloist.tar.gz", str(tmp_path / "out"))
        assert calls == [
            (
                "https://example.com/soloist.tar.gz",
                str(tmp_path / "out"),
                "http://127.0.0.1:7890",
            )
        ]

    def test_invalid_proxy_falls_back_to_direct_download(self, tmp_path, monkeypatch):
        binary = SoloistBinary(str(tmp_path), proxy="ftp://127.0.0.1:21")
        direct_calls: list[tuple] = []

        def fake_direct(url, dest, prefer_ipv4=True):
            direct_calls.append((url, dest, prefer_ipv4))

        def fail_via_proxy(*args, **kwargs):
            raise AssertionError("非法代理不应走代理下载")

        monkeypatch.setattr(binary, "_download_direct", fake_direct)
        monkeypatch.setattr(binary, "_download_via_proxy", fail_via_proxy)
        binary._download_to("https://example.com/soloist.tar.gz", str(tmp_path / "out"))
        assert direct_calls, "非法代理应降级为直连下载"
        assert binary._resolved_proxy() == ""


class TestProxySettingsApi:
    def _save(self, tmp_path, proxy, config=None):
        config = config or Config(conf_path=str(tmp_path))
        background = BackgroundTasks()
        result = asyncio.run(
            save_settings_api(
                SettingsPayload(spotify_proxy=proxy),
                background,
                orch=SimpleNamespace(restart_dlna_services=AsyncMock()),
                config=config,
            )
        )
        return result, config, background

    def test_bare_proxy_is_normalized_and_persisted(self, tmp_path):
        result, config, background = self._save(tmp_path, "127.0.0.1:7890")
        assert result["ok"]
        assert config.spotify_proxy == "http://127.0.0.1:7890"
        loaded = Config.load(str(tmp_path))
        assert loaded.spotify_proxy == "http://127.0.0.1:7890"
        assert len(background.tasks) == 1

    def test_socks5_proxy_saved_as_is(self, tmp_path):
        _, config, _ = self._save(tmp_path, "socks5://127.0.0.1:1080")
        assert config.spotify_proxy == "socks5://127.0.0.1:1080"

    def test_empty_proxy_clears_setting(self, tmp_path):
        config = Config(conf_path=str(tmp_path), spotify_proxy="http://127.0.0.1:7890")
        _, config, _ = self._save(tmp_path, "", config=config)
        assert config.spotify_proxy == ""

    def test_invalid_proxy_rejected_with_400_and_kept_unchanged(self, tmp_path):
        config = Config(conf_path=str(tmp_path), spotify_proxy="http://127.0.0.1:7890")
        with pytest.raises(HTTPException) as excinfo:
            self._save(tmp_path, "ftp://127.0.0.1:21", config=config)
        assert excinfo.value.status_code == 400
        assert "Spotify 代理地址无效" in str(excinfo.value.detail)
        assert config.spotify_proxy == "http://127.0.0.1:7890"


class TestSpotifySettingsDefaults:
    def test_spotify_volume_control_defaults_on(self):
        assert Config().spotify_volume_control is True

    def test_spotify_proxy_defaults_empty(self):
        assert Config().spotify_proxy == ""
