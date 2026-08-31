"""Spotify Connect 接收引擎

参考 infini2020/SpotConnect (cspot) 的架构，将小爱音箱虚拟为 Spotify Connect 设备:

- zeroconf_server: mDNS 广播 `_spotify-connect._tcp` + `/spotify_info` 配对端点
  (对应 cspot 的 LoginBlob / enableZeroConf)
- spirc: SPIRC 协议控制器，订阅 `hm://remote/user/<name>/` 并处理
  Load/Play/Pause/Seek/Volume/Next/Prev 命令 (对应 cspot 的 SpircHandler/PlaybackState)
- audio_server: 本地 HTTP 音频流服务器，把 PCM 以 WAV/MP3 提供给音箱拉流
  (对应 SpotConnect 的 HTTPstreamer)
- player: 曲目加载 (librespot PlayableContentFeeder) 与解码播放
  (对应 cspot 的 TrackQueue/TrackPlayer)
- manager: 每个音箱一个虚拟 Spotify 设备 (对应 SpotConnect 的 per-player 模式)

音频链路: Spotify CDN (加密 Ogg Vorbis) -> librespot 解密 -> PyAV 解码为 PCM
-> 本地 HTTP 流 -> 小爱音箱 play_url 拉流播放。
"""

from app.engine.spotify.manager import SpeakerSpotify, SpotifyManager

__all__ = ["SpeakerSpotify", "SpotifyManager"]
