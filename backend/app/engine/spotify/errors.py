"""Spotify 模块公共异常工具"""


def exc_desc(e: BaseException) -> str:
    """异常的可读描述。

    原生 socket 异常 (ConnectionResetError / TimeoutError / ConnectionRefusedError)
    无参构造时 str() 为空字符串, 直接 f"{e}" 会打出空消息无法排查,
    因此带上异常类名。
    """
    msg = str(e)
    return f"{type(e).__name__}: {msg}" if msg else type(e).__name__


def is_network_error(e: BaseException) -> bool:
    """是否网络类异常 (连接被重置 / 超时 / DNS 失败等)。

    这类失败不代表配对凭据无效 (stored credential 可重复使用),
    不应清除凭据, 网络恢复后可自动重登。
    """
    if isinstance(e, OSError):  # 覆盖 ConnectionError / TimeoutError / gaierror / socket.timeout
        return True
    # requests (ApResolver) 的连接异常
    return "requests.exceptions" in type(e).__module__
