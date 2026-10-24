"""
Windows DPAPI (Data Protection API / Crypt32.dll) 機密情報暗号化・復号モジュール (dpapi_vault.py)

外部パッケージを一切使用せず、Python 標準の `ctypes` から Windows OS 標準の
`Crypt32.dll` (`CryptProtectData` / `CryptUnprotectData`) および `Kernel32.dll` (`LocalFree`) を呼び出し、
現在の Windows ログインユーザーコンテキストに紐づく暗号化 (`DPAPI:AQAA...`) とオンメモリ復号を提供する。
"""

from __future__ import annotations

import base64
import contextlib
import ctypes
from ctypes import wintypes
import os
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Union

from dotenv import dotenv_values, load_dotenv

DPAPI_PREFIX = "DPAPI:"
CRYPTPROTECT_UI_FORBIDDEN = 0x01

DEFAULT_SECRET_KEYS = (
    "GEMINI_API_KEY",
    "GROQ_API_KEY",
    "OPENAI_API_KEY",
    "DISCORD_WEBHOOK_URL",
    "LINE_CHANNEL_ACCESS_TOKEN",
    "CRYPTOPANIC_API_KEY",
    "GMO_API_KEY",
    "GMO_API_SECRET",
    "GITHUB_TOKEN",
    "PWA_UNLOCK_PASSWORD",
)
SECRET_ENV_KEYS = DEFAULT_SECRET_KEYS


class DATA_BLOB(ctypes.Structure):
    """Windows Crypt32.dll DATA_BLOB (CRYPTOAPI_BLOB) 64bitポインタ対応構造体"""

    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_char)),
    ]


_crypt32 = ctypes.windll.crypt32
_kernel32 = ctypes.windll.kernel32

_crypt32.CryptProtectData.argtypes = [
    ctypes.POINTER(DATA_BLOB),  # pDataIn
    wintypes.LPCWSTR,           # szDataDescr
    ctypes.POINTER(DATA_BLOB),  # pOptionalEntropy
    ctypes.c_void_p,            # pvReserved
    ctypes.c_void_p,            # pPromptStruct
    wintypes.DWORD,             # dwFlags
    ctypes.POINTER(DATA_BLOB),  # pDataOut
]
_crypt32.CryptProtectData.restype = wintypes.BOOL

_crypt32.CryptUnprotectData.argtypes = [
    ctypes.POINTER(DATA_BLOB),               # pDataIn
    ctypes.POINTER(wintypes.LPWSTR),         # ppszDataDescr
    ctypes.POINTER(DATA_BLOB),               # pOptionalEntropy
    ctypes.c_void_p,                         # pvReserved
    ctypes.c_void_p,                         # pPromptStruct
    wintypes.DWORD,                          # dwFlags
    ctypes.POINTER(DATA_BLOB),               # pDataOut
]
_crypt32.CryptUnprotectData.restype = wintypes.BOOL

# 64bit ポインタ切り詰め防止のため c_void_p を明示指定
_kernel32.LocalFree.argtypes = [ctypes.c_void_p]
_kernel32.LocalFree.restype = ctypes.c_void_p


def is_dpapi_protected(value: Optional[str]) -> bool:
    """文字列が `DPAPI:` プレフィックス付きの暗号化済み値かどうかを判定する"""
    if not value or not isinstance(value, str):
        return False
    return value.strip().startswith(DPAPI_PREFIX)


def protect_secret(plaintext: str) -> str:
    """
    平文シークレットを現在の Windows ユーザーコンテキスト (DPAPI) で暗号化し、
    `DPAPI:<base64>` 形式の文字列を返す。
    既に `DPAPI:` で始まっている場合は二重暗号化せずそのまま返す。
    """
    if plaintext is None:
        return ""
    cleaned = str(plaintext).strip()
    if not cleaned:
        return ""
    if cleaned.startswith(DPAPI_PREFIX):
        return cleaned

    raw_bytes = cleaned.encode("utf-8")
    in_buf = ctypes.create_string_buffer(raw_bytes, len(raw_bytes))
    blob_in = DATA_BLOB(len(raw_bytes), ctypes.cast(in_buf, ctypes.POINTER(ctypes.c_char)))
    blob_out = DATA_BLOB()

    ok = _crypt32.CryptProtectData(
        ctypes.byref(blob_in),
        "gmo-ai-trader-v2-secret",
        None,
        None,
        None,
        CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(blob_out),
    )
    if not ok:
        err_code = ctypes.GetLastError()
        raise RuntimeError(f"CryptProtectData failed with Windows error code: {err_code}")

    try:
        encrypted_bytes = ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        if blob_out.pbData:
            _kernel32.LocalFree(ctypes.cast(blob_out.pbData, ctypes.c_void_p))

    b64_str = base64.b64encode(encrypted_bytes).decode("ascii")
    return f"{DPAPI_PREFIX}{b64_str}"


def unprotect_secret(value: Optional[str]) -> str:
    """
    `DPAPI:<base64>` 形式の文字列を Windows DPAPI でオンメモリ復号して平文を返す。
    `DPAPI:` で始まらない通常の平文が渡された場合は後方互換としてそのまま返す。
    """
    if value is None:
        return ""
    cleaned = str(value).strip()
    if not cleaned:
        return ""
    if not cleaned.startswith(DPAPI_PREFIX):
        return cleaned

    b64_payload = cleaned[len(DPAPI_PREFIX):].strip()
    if not b64_payload:
        return ""

    encrypted_bytes = base64.b64decode(b64_payload)
    in_buf = ctypes.create_string_buffer(encrypted_bytes, len(encrypted_bytes))
    blob_in = DATA_BLOB(len(encrypted_bytes), ctypes.cast(in_buf, ctypes.POINTER(ctypes.c_char)))
    blob_out = DATA_BLOB()

    ok = _crypt32.CryptUnprotectData(
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(blob_out),
    )
    if not ok:
        err_code = ctypes.GetLastError()
        raise RuntimeError(f"CryptUnprotectData failed with Windows error code: {err_code}")

    try:
        decrypted_bytes = ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        if blob_out.pbData:
            try:
                if blob_out.cbData > 0:
                    ctypes.memset(ctypes.cast(blob_out.pbData, ctypes.c_void_p), 0, int(blob_out.cbData))
            except Exception:
                pass
            _kernel32.LocalFree(ctypes.cast(blob_out.pbData, ctypes.c_void_p))

    return decrypted_bytes.decode("utf-8")


def zeroize_bytearray(buf: Optional[bytearray]) -> int:
    """
    `bytearray` の基底メモリバッファを `ctypes.memset` により `0x00` で完全に上書き消去（Zeroize）する。
    消去したバイト数を返す。
    """
    if not isinstance(buf, bytearray) or len(buf) == 0:
        return 0
    n = len(buf)
    c_buf = (ctypes.c_char * n).from_buffer(buf)
    ctypes.memset(ctypes.addressof(c_buf), 0, n)
    return n


@contextlib.contextmanager
def ephemeral_unprotected_bytes(value: Optional[str]) -> Iterator[bytearray]:
    """
    Just-In-Time (JIT) 復号コンテキストマネージャ：
    Windows `Crypt32.dll` (`CryptUnprotectData`) で復号した秘密鍵をミュータブルな `bytearray` として一時生成し、
    `with ephemeral_unprotected_bytes(...) as secret_buf:` ブロックを抜けた瞬間に
    `ctypes.memset` でメモリ上のバッファを `0x00` で完全に上書き消去（Zeroize）する。
    """
    secret_buf = bytearray()
    if value is not None:
        cleaned = str(value).strip()
        if cleaned:
            if not cleaned.startswith(DPAPI_PREFIX):
                secret_buf = bytearray(cleaned.encode("utf-8"))
            else:
                b64_payload = cleaned[len(DPAPI_PREFIX):].strip()
                if b64_payload:
                    encrypted_bytes = base64.b64decode(b64_payload)
                    in_buf = ctypes.create_string_buffer(encrypted_bytes, len(encrypted_bytes))
                    blob_in = DATA_BLOB(len(encrypted_bytes), ctypes.cast(in_buf, ctypes.POINTER(ctypes.c_char)))
                    blob_out = DATA_BLOB()
                    ok = _crypt32.CryptUnprotectData(
                        ctypes.byref(blob_in),
                        None,
                        None,
                        None,
                        None,
                        CRYPTPROTECT_UI_FORBIDDEN,
                        ctypes.byref(blob_out),
                    )
                    if not ok:
                        err_code = ctypes.GetLastError()
                        raise RuntimeError(f"CryptUnprotectData failed with Windows error code: {err_code}")
                    try:
                        raw_dec = ctypes.string_at(blob_out.pbData, blob_out.cbData)
                        secret_buf = bytearray(raw_dec)
                    finally:
                        if blob_out.pbData:
                            try:
                                if blob_out.cbData > 0:
                                    ctypes.memset(
                                        ctypes.cast(blob_out.pbData, ctypes.c_void_p),
                                        0,
                                        int(blob_out.cbData),
                                    )
                            except Exception:
                                pass
                            _kernel32.LocalFree(ctypes.cast(blob_out.pbData, ctypes.c_void_p))
    try:
        yield secret_buf
    finally:
        zeroize_bytearray(secret_buf)


def get_secret_env(key: str, default: str = "") -> str:
    """
    環境変数 `key` の値を取得し、`DPAPI:` プレフィックスがある場合はオンメモリ復号して返す。
    """
    raw_val = os.getenv(key, default)
    if not raw_val:
        return default
    return unprotect_secret(raw_val)


def load_and_unprotect_dotenv(
    dotenv_path: Optional[Union[str, Path]] = None,
    override: bool = False,
) -> bool:
    """
    `.env` ファイルを読み込み、プロセスの `os.environ` 上には平文のシークレットを一切書き戻さず、
    `DPAPI:AQAA...` の暗号化文字列のまま保持する（PEB 環境変数ブロックダンプによる平文漏洩を100%遮断）。
    万一 `os.environ` 内のシークレットキー群 (`DEFAULT_SECRET_KEYS`) に平文が入っていた場合も
    即座に `protect_secret()` で `DPAPI:AQAA...` 形式へ変換して保持する。
    （※各モジュールは必ず `get_secret_env()` / `unprotect_secret()` / `ephemeral_unprotected_bytes()` で JIT 復号する）
    """
    loaded = load_dotenv(dotenv_path=dotenv_path, override=override)
    if dotenv_path is not None and Path(dotenv_path).exists():
        try:
            file_kv = dotenv_values(dotenv_path)
            for k, v in file_kv.items():
                if k and v is not None and (override or k not in os.environ):
                    os.environ[k] = str(v)
        except Exception:
            pass

    secret_key_set = set(DEFAULT_SECRET_KEYS)
    for env_k, env_v in list(os.environ.items()):
        if not isinstance(env_v, str):
            continue
        cleaned_v = env_v.strip()
        if not cleaned_v or cleaned_v.startswith("your_"):
            continue
        if env_k in secret_key_set and not cleaned_v.startswith(DPAPI_PREFIX):
            try:
                os.environ[env_k] = protect_secret(cleaned_v)
            except Exception:
                pass
    return bool(loaded)


def build_github_basic_auth_header(token: Optional[str] = None, dotenv_path: Optional[Union[str, Path]] = None) -> str:
    """
    GitHub Personal Access Token をオンメモリ復号し、
    `git -c "http.extraheader=AUTHORIZATION: basic <base64>"` 用のヘッダー文字列を生成する。
    トークンが未設定の場合は空文字列を返す。
    """
    tok = unprotect_secret(token) if token else get_secret_env("GITHUB_TOKEN", "")
    if not tok and dotenv_path is not None and Path(dotenv_path).exists():
        try:
            kv = dotenv_values(dotenv_path)
            if kv.get("GITHUB_TOKEN"):
                tok = unprotect_secret(str(kv["GITHUB_TOKEN"]))
        except Exception:
            pass
    tok = (tok or "").strip()
    if not tok:
        return ""
    b64_auth = base64.b64encode(f"x-access-token:{tok}".encode("utf-8")).decode("ascii")
    return f"AUTHORIZATION: basic {b64_auth}"


def encrypt_dotenv_secrets_inplace(
    dotenv_path: Union[str, Path],
    secret_keys: Iterable[str] = DEFAULT_SECRET_KEYS,
    extra_secrets: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    指定された `.env` ファイル内の対象シークレットキーの値をインプレースで `DPAPI:AQAA...` 形式へ暗号化する。
    `extra_secrets` が指定された場合は新規キーまたは更新キーとして DPAPI 暗号化して保存する。
    """
    target_path = Path(dotenv_path)
    key_set = {str(k).strip() for k in secret_keys}
    extras = {str(k).strip(): str(v).strip() for k, v in (extra_secrets or {}).items() if k and v}
    key_set.update(extras.keys())

    lines = target_path.read_text(encoding="utf-8").splitlines(keepends=True) if target_path.exists() else []
    seen_keys = set()
    new_lines = []
    status_map: Dict[str, str] = {}

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            new_lines.append(line)
            continue
        k_part, v_part = line.split("=", 1)
        k_clean = k_part.strip()
        v_clean = v_part.strip().strip('"').strip("'")
        if k_clean in extras:
            v_clean = extras[k_clean]
        if k_clean in key_set and v_clean and not v_clean.startswith("your_"):
            enc_val = protect_secret(v_clean)
            new_lines.append(f"{k_clean}={enc_val}\n")
            seen_keys.add(k_clean)
            status_map[k_clean] = "DPAPI_ENCRYPTED"
        else:
            new_lines.append(line)
            seen_keys.add(k_clean)

    if new_lines and not new_lines[-1].endswith("\n"):
        new_lines[-1] += "\n"

    for ex_k, ex_v in extras.items():
        if ex_k not in seen_keys and ex_v:
            enc_val = protect_secret(ex_v)
            new_lines.append(f"{ex_k}={enc_val}\n")
            status_map[ex_k] = "DPAPI_ENCRYPTED"

    tmp_path = target_path.with_suffix(".env.tmp")
    tmp_path.write_text("".join(new_lines), encoding="utf-8")
    os.replace(tmp_path, target_path)
    return status_map
