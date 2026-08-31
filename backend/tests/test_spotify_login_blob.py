"""Spotify 配对凭据两层解密链路测试

完整模拟 Spotify 客户端的两层加密, 验证服务端链路:
  客户端第二层加密 (PBKDF2 + AES-ECB + 交错混淆) -> base64 文本
  客户端第一层加密 (DH 共享密钥 + AES-CTR)        -> POST blob
  服务端第一层解密 (zeroconf_server._decrypt_add_user)
  服务端第二层解密 (librespot Session.Builder.blob)

回归场景: 曾经在 _handle_credentials 里对第一层明文多做一次 b64encode,
导致 librespot 把 base64 文本当第二层密文解, 读出非法 enum 值 48
("Enum AuthenticationType has no name defined for value 48")。
"""

import base64
import hashlib
import hmac
import secrets

import pytest
from Cryptodome.Cipher import AES
from Cryptodome.Hash import SHA1
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Util import Counter
from librespot import util
from librespot.core import Session
from librespot.crypto import DiffieHellman
from librespot.proto import Authentication_pb2 as Authentication

from app.engine.spotify.zeroconf_server import SpotifyZeroconfReceiver

_DEVICE_NAME = "Test Speaker"
_DEVICE_ID = SpotifyZeroconfReceiver.make_device_id(_DEVICE_NAME)
_USERNAME = "315nghpyc7kxxvphmph5hiznxi3y"  # 真实 Spotify 用户 ID 形态
_AUTH_DATA = secrets.token_bytes(20)  # Spotify reusable credential 为 20 字节


# ---------------------------------------------------------------------------
# 客户端侧: 第二层加密 (librespot decrypt_blob 的逆过程)
# ---------------------------------------------------------------------------

def _write_blob_int(n: int) -> bytes:
    """read_blob_int 兼容的 7-bit 变体长度编码"""
    if n < 0x80:
        return bytes([n])
    return bytes([(n & 0x7F) | 0x80, (n >> 7) & 0xFF])


def _client_encrypt_second(device_id: str, username: str,
                           auth_data: bytes, typ: int) -> bytes:
    """构造第二层明文 (librespot read_blob 解析的 TLV 结构) 并加密

    返回第一层明文: 即第二层密文的 base64 文本 (bytes)。
    """
    typ_name = Authentication.AuthenticationType.Name(typ)
    inner = bytearray()
    inner += b"\x00"
    inner += _write_blob_int(len(username))
    inner += username.encode()
    inner += b"\x00"
    inner += _write_blob_int(typ)
    inner += b"\x00"
    inner += _write_blob_int(len(auth_data))
    inner += auth_data
    # pad 到 AES 块大小
    if len(inner) % 16:
        inner += b"\x00" * (16 - len(inner) % 16)

    # 客户端密钥派生 (与 decrypt_blob 一致): PBKDF2(SHA1(device_id), username)
    secret = SHA1.new(device_id.encode()).digest()
    base_key = PBKDF2(secret, username.encode(), 20, 0x100, hmac_hash_module=SHA1)
    key = SHA1.new(base_key).digest() + b"\x00\x00\x00\x14"

    # 解密侧 shuffle 的逆: 反向执行同一 XOR 循环
    data = bytearray(inner)
    l = len(data)
    for i in range(l - 0x11, -1, -1):
        data[l - i - 1] ^= data[l - i - 0x11]

    encrypted = AES.new(key, AES.MODE_ECB).encrypt(bytes(data))
    return base64.b64encode(encrypted)  # 第一层明文 = base64 文本


def _client_encrypt_first(server_public: bytes, first_layer_plain: bytes) -> tuple[bytes, bytes]:
    """客户端第一层加密 (DH + AES-CTR), 返回 (POST 的 blob, clientKey)"""
    client = DiffieHellman()
    shared = util.int_to_bytes(client.compute_shared_key(server_public))

    base_key = hashlib.sha1(shared).digest()[:16]
    enc_key = hmac.new(base_key, b"encryption", hashlib.sha1).digest()[:16]
    checksum_key = hmac.new(base_key, b"checksum", hashlib.sha1).digest()

    iv = secrets.token_bytes(16)
    counter = Counter.new(128, initial_value=int.from_bytes(iv, "big"))
    encrypted = AES.new(enc_key, AES.MODE_CTR, counter=counter).encrypt(first_layer_plain)
    checksum = hmac.new(checksum_key, encrypted, hashlib.sha1).digest()

    return iv + encrypted + checksum, client.public_key_bytes()


def _pair_params(server_public: bytes, typ: int) -> dict:
    """模拟客户端生成完整 POST 参数"""
    first_layer = _client_encrypt_second(_DEVICE_ID, _USERNAME, _AUTH_DATA, typ)
    blob, client_public = _client_encrypt_first(server_public, first_layer)
    return {
        "userName": _USERNAME,
        "blob": base64.b64encode(blob).decode(),
        "clientKey": base64.b64encode(client_public).decode(),
    }


def _server_public(recv: SpotifyZeroconfReceiver) -> bytes:
    return base64.b64decode(recv._build_info()["publicKey"])


def test_two_layer_decryption_via_builder():
    """端到端: 第一层服务端解密 -> 直接透传 -> librespot 第二层解密出凭据"""
    recv = SpotifyZeroconfReceiver("127.0.0.1", _DEVICE_NAME, _DEVICE_ID)
    typ = Authentication.AuthenticationType.Value(
        "AUTHENTICATION_STORED_SPOTIFY_CREDENTIALS")
    params = _pair_params(_server_public(recv), typ)

    # 服务端第一层解密 (即 _handle_post 内部逻辑)
    username, first_layer = recv._decrypt_add_user(params)
    assert username == _USERNAME
    # 第一层明文是合法 base64 文本 (第二层密文的 base64)
    assert base64.b64decode(first_layer) is not None

    # 修复后的调用方式: 直接透传给 Session.Builder.blob (不再 b64encode)
    builder = Session.Builder().set_device_id(_DEVICE_ID)
    builder.blob(username, first_layer)

    cred = builder.login_credentials
    assert cred is not None
    assert cred.username == _USERNAME
    assert bytes(cred.auth_data) == _AUTH_DATA
    assert cred.typ == typ


def test_double_b64encode_reproduces_enum_error():
    """回归: 旧代码多做一次 b64encode 会导致解密错位 (enum 值非法)"""
    recv = SpotifyZeroconfReceiver("127.0.0.1", _DEVICE_NAME, _DEVICE_ID)
    typ = Authentication.AuthenticationType.Value(
        "AUTHENTICATION_STORED_SPOTIFY_CREDENTIALS")
    params = _pair_params(_server_public(recv), typ)
    username, first_layer = recv._decrypt_add_user(params)

    # 旧实现: b64encode(第一层明文) 再传入 -> librespot 解密错位
    bad_blob = base64.b64encode(first_layer)
    builder = Session.Builder().set_device_id(_DEVICE_ID)
    with pytest.raises(Exception):
        builder.blob(username, bad_blob)


def test_first_layer_output_matches_client_plaintext():
    """服务端第一层解密输出应与客户端构造的第一层明文逐字节一致"""
    recv = SpotifyZeroconfReceiver("127.0.0.1", _DEVICE_NAME, _DEVICE_ID)
    typ = Authentication.AuthenticationType.Value("AUTHENTICATION_USER_PASS")
    expected = _client_encrypt_second(_DEVICE_ID, _USERNAME, _AUTH_DATA, typ)
    params = _pair_params(_server_public(recv), typ)
    _, first_layer = recv._decrypt_add_user(params)
    assert first_layer == expected
