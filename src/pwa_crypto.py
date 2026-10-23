"""
スマホPWA ゼロ知識クライアント復号用 AES-256-GCM + PBKDF2-SHA256 暗号化モジュール (pwa_crypto.py)

外部パッケージを一切使用せず、Python 標準の `hashlib.pbkdf2_hmac` (SHA-256, 150,000 iterations) と
Windows OS 標準の CNG API `bcrypt.dll` (`BCryptOpenAlgorithmProvider`, `BCryptSetProperty`,
`BCryptGenerateSymmetricKey`, `BCryptEncrypt`, `BCryptDecrypt`) を呼び出して
ブラウザの Web Crypto API (`window.crypto.subtle.decrypt({name: "AES-GCM", iv})`) と完全互換の
AES-256-GCM 暗号化エンベロープ (`ciphertext + 16-byte auth tag`) を生成・検証する。
"""

from __future__ import annotations

import base64
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
import secrets
from typing import Any, Dict, Optional

PBKDF2_ITERATIONS = 150_000
SALT_LEN = 16
IV_LEN = 12
TAG_LEN = 16
KEY_LEN = 32

BCRYPT_AES_ALGORITHM = "AES"
BCRYPT_CHAINING_MODE = "ChainingMode"
BCRYPT_CHAIN_MODE_GCM = "ChainingModeGCM"
BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO_VERSION = 1

STATUS_SUCCESS = 0
STATUS_AUTH_TAG_MISMATCH = -1073700862  # 0xC000A002 (signed 32-bit)


class BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO(ctypes.Structure):
    """
    Windows CNG `BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO` 構造体 (64bit アライメント完全準拠)
    """

    _fields_ = [
        ("cbSize", wintypes.ULONG),
        ("dwInfoVersion", wintypes.ULONG),
        ("pbNonce", ctypes.c_void_p),
        ("cbNonce", wintypes.ULONG),
        ("pbAuthData", ctypes.c_void_p),
        ("cbAuthData", wintypes.ULONG),
        ("pbTag", ctypes.c_void_p),
        ("cbTag", wintypes.ULONG),
        ("pbMacContext", ctypes.c_void_p),
        ("cbMacContext", wintypes.ULONG),
        ("cbAAD", wintypes.ULONG),
        ("cbData", ctypes.c_uint64),
        ("dwFlags", wintypes.ULONG),
    ]


_bcrypt = ctypes.windll.bcrypt

_bcrypt.BCryptOpenAlgorithmProvider.argtypes = [
    ctypes.POINTER(ctypes.c_void_p),  # phAlgorithm
    wintypes.LPCWSTR,                 # pszAlgId
    wintypes.LPCWSTR,                 # pszImplementation
    wintypes.ULONG,                   # dwFlags
]
_bcrypt.BCryptOpenAlgorithmProvider.restype = ctypes.c_long

_bcrypt.BCryptSetProperty.argtypes = [
    ctypes.c_void_p,                  # hObject
    wintypes.LPCWSTR,                 # pszProperty
    ctypes.c_void_p,                  # pbInput
    wintypes.ULONG,                   # cbInput
    wintypes.ULONG,                   # dwFlags
]
_bcrypt.BCryptSetProperty.restype = ctypes.c_long

_bcrypt.BCryptGenerateSymmetricKey.argtypes = [
    ctypes.c_void_p,                  # hAlgorithm
    ctypes.POINTER(ctypes.c_void_p),  # phKey
    ctypes.c_void_p,                  # pbKeyObject
    wintypes.ULONG,                   # cbKeyObject
    ctypes.c_void_p,                  # pbSecret
    wintypes.ULONG,                   # cbSecret
    wintypes.ULONG,                   # dwFlags
]
_bcrypt.BCryptGenerateSymmetricKey.restype = ctypes.c_long

_bcrypt.BCryptEncrypt.argtypes = [
    ctypes.c_void_p,                  # hKey
    ctypes.c_void_p,                  # pbInput
    wintypes.ULONG,                   # cbInput
    ctypes.c_void_p,                  # pPaddingInfo
    ctypes.c_void_p,                  # pbIV
    wintypes.ULONG,                   # cbIV
    ctypes.c_void_p,                  # pbOutput
    wintypes.ULONG,                   # cbOutput
    ctypes.POINTER(wintypes.ULONG),   # pcbResult
    wintypes.ULONG,                   # dwFlags
]
_bcrypt.BCryptEncrypt.restype = ctypes.c_long

_bcrypt.BCryptDecrypt.argtypes = [
    ctypes.c_void_p,                  # hKey
    ctypes.c_void_p,                  # pbInput
    wintypes.ULONG,                   # cbInput
    ctypes.c_void_p,                  # pPaddingInfo
    ctypes.c_void_p,                  # pbIV
    wintypes.ULONG,                   # cbIV
    ctypes.c_void_p,                  # pbOutput
    wintypes.ULONG,                   # cbOutput
    ctypes.POINTER(wintypes.ULONG),   # pcbResult
    wintypes.ULONG,                   # dwFlags
]
_bcrypt.BCryptDecrypt.restype = ctypes.c_long

_bcrypt.BCryptDestroyKey.argtypes = [ctypes.c_void_p]
_bcrypt.BCryptDestroyKey.restype = ctypes.c_long

_bcrypt.BCryptCloseAlgorithmProvider.argtypes = [ctypes.c_void_p, wintypes.ULONG]
_bcrypt.BCryptCloseAlgorithmProvider.restype = ctypes.c_long


def derive_aes256_key(
    password: str,
    salt: bytes,
    iterations: int = PBKDF2_ITERATIONS,
) -> bytes:
    """PBKDF2-HMAC-SHA256 (既定 150,000 回) で 32 バイト (256-bit) の鍵を導出する"""
    if not password:
        raise ValueError("Password must not be empty for PBKDF2 key derivation.")
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        int(iterations),
        dklen=KEY_LEN,
    )


def _open_aes_gcm_key(key_bytes: bytes) -> tuple[ctypes.c_void_p, ctypes.c_void_p]:
    if len(key_bytes) != KEY_LEN:
        raise ValueError(f"AES-256 key must be {KEY_LEN} bytes, got {len(key_bytes)}")
    h_alg = ctypes.c_void_p()
    status = _bcrypt.BCryptOpenAlgorithmProvider(
        ctypes.byref(h_alg),
        BCRYPT_AES_ALGORITHM,
        None,
        0,
    )
    if status != STATUS_SUCCESS:
        raise RuntimeError(f"BCryptOpenAlgorithmProvider failed: 0x{status & 0xFFFFFFFF:08X}")

    try:
        mode_wstr = ctypes.create_unicode_buffer(BCRYPT_CHAIN_MODE_GCM)
        status = _bcrypt.BCryptSetProperty(
            h_alg,
            BCRYPT_CHAINING_MODE,
            ctypes.cast(mode_wstr, ctypes.c_void_p),
            ctypes.sizeof(mode_wstr),
            0,
        )
        if status != STATUS_SUCCESS:
            raise RuntimeError(f"BCryptSetProperty(GCM) failed: 0x{status & 0xFFFFFFFF:08X}")

        h_key = ctypes.c_void_p()
        key_buf = ctypes.create_string_buffer(key_bytes, len(key_bytes))
        status = _bcrypt.BCryptGenerateSymmetricKey(
            h_alg,
            ctypes.byref(h_key),
            None,
            0,
            ctypes.cast(key_buf, ctypes.c_void_p),
            len(key_bytes),
            0,
        )
        if status != STATUS_SUCCESS:
            raise RuntimeError(f"BCryptGenerateSymmetricKey failed: 0x{status & 0xFFFFFFFF:08X}")
        return h_alg, h_key
    except Exception:
        if h_alg:
            _bcrypt.BCryptCloseAlgorithmProvider(h_alg, 0)
        raise


def bcrypt_aes256_gcm_encrypt(plaintext_bytes: bytes, key_bytes: bytes, iv_bytes: bytes) -> bytes:
    """
    Windows `bcrypt.dll` を用いて AES-256-GCM 暗号化を行い、
    Web Crypto API 互換の `ciphertext + tag (16 bytes)` 結合バイト列を返す。
    """
    if len(iv_bytes) != IV_LEN:
        raise ValueError(f"GCM IV must be {IV_LEN} bytes, got {len(iv_bytes)}")

    h_alg, h_key = _open_aes_gcm_key(key_bytes)
    try:
        pt_len = len(plaintext_bytes)
        in_buf = ctypes.create_string_buffer(plaintext_bytes, max(1, pt_len))
        out_buf = ctypes.create_string_buffer(max(1, pt_len))
        iv_buf = ctypes.create_string_buffer(iv_bytes, len(iv_bytes))
        tag_buf = ctypes.create_string_buffer(TAG_LEN)

        auth_info = BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO()
        ctypes.memset(ctypes.byref(auth_info), 0, ctypes.sizeof(auth_info))
        auth_info.cbSize = ctypes.sizeof(BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO)
        auth_info.dwInfoVersion = BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO_VERSION
        auth_info.pbNonce = ctypes.cast(iv_buf, ctypes.c_void_p)
        auth_info.cbNonce = len(iv_bytes)
        auth_info.pbTag = ctypes.cast(tag_buf, ctypes.c_void_p)
        auth_info.cbTag = TAG_LEN

        cb_result = wintypes.ULONG(0)
        status = _bcrypt.BCryptEncrypt(
            h_key,
            ctypes.cast(in_buf, ctypes.c_void_p) if pt_len > 0 else None,
            pt_len,
            ctypes.byref(auth_info),
            None,
            0,
            ctypes.cast(out_buf, ctypes.c_void_p) if pt_len > 0 else None,
            pt_len,
            ctypes.byref(cb_result),
            0,
        )
        if status != STATUS_SUCCESS:
            raise RuntimeError(f"BCryptEncrypt(AES-GCM) failed: 0x{status & 0xFFFFFFFF:08X}")

        ciphertext = ctypes.string_at(out_buf, cb_result.value) if pt_len > 0 else b""
        tag = ctypes.string_at(tag_buf, TAG_LEN)
        return ciphertext + tag
    finally:
        if h_key:
            _bcrypt.BCryptDestroyKey(h_key)
        if h_alg:
            _bcrypt.BCryptCloseAlgorithmProvider(h_alg, 0)


def bcrypt_aes256_gcm_decrypt(ciphertext_and_tag: bytes, key_bytes: bytes, iv_bytes: bytes) -> bytes:
    """
    Windows `bcrypt.dll` を用いて `ciphertext + tag (16 bytes)` を AES-256-GCM 復号・認証検証する。
    パスワード誤りまたはペイロード改ざん時は `ValueError` を送出する。
    """
    if len(iv_bytes) != IV_LEN:
        raise ValueError(f"GCM IV must be {IV_LEN} bytes, got {len(iv_bytes)}")
    if len(ciphertext_and_tag) < TAG_LEN:
        raise ValueError("Ciphertext payload is shorter than GCM authentication tag (16 bytes).")

    ct_len = len(ciphertext_and_tag) - TAG_LEN
    ciphertext = ciphertext_and_tag[:ct_len]
    tag = ciphertext_and_tag[ct_len:]

    h_alg, h_key = _open_aes_gcm_key(key_bytes)
    try:
        in_buf = ctypes.create_string_buffer(ciphertext, max(1, ct_len))
        out_buf = ctypes.create_string_buffer(max(1, ct_len))
        iv_buf = ctypes.create_string_buffer(iv_bytes, len(iv_bytes))
        tag_buf = ctypes.create_string_buffer(tag, TAG_LEN)

        auth_info = BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO()
        ctypes.memset(ctypes.byref(auth_info), 0, ctypes.sizeof(auth_info))
        auth_info.cbSize = ctypes.sizeof(BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO)
        auth_info.dwInfoVersion = BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO_VERSION
        auth_info.pbNonce = ctypes.cast(iv_buf, ctypes.c_void_p)
        auth_info.cbNonce = len(iv_bytes)
        auth_info.pbTag = ctypes.cast(tag_buf, ctypes.c_void_p)
        auth_info.cbTag = TAG_LEN

        cb_result = wintypes.ULONG(0)
        status = _bcrypt.BCryptDecrypt(
            h_key,
            ctypes.cast(in_buf, ctypes.c_void_p) if ct_len > 0 else None,
            ct_len,
            ctypes.byref(auth_info),
            None,
            0,
            ctypes.cast(out_buf, ctypes.c_void_p) if ct_len > 0 else None,
            ct_len,
            ctypes.byref(cb_result),
            0,
        )
        if status != STATUS_SUCCESS:
            raise ValueError(f"AES-256-GCM authentication or decryption failed (status=0x{status & 0xFFFFFFFF:08X})")

        return ctypes.string_at(out_buf, cb_result.value) if ct_len > 0 else b""
    finally:
        if h_key:
            _bcrypt.BCryptDestroyKey(h_key)
        if h_alg:
            _bcrypt.BCryptCloseAlgorithmProvider(h_alg, 0)


def encrypt_json_envelope(
    data: Any,
    password: str,
    iterations: int = PBKDF2_ITERATIONS,
) -> Dict[str, Any]:
    """
    任意の JSON シリアライズ可能オブジェクトを PBKDF2-SHA256 (150,000回) + AES-256-GCM で暗号化し、
    Web Crypto API 復号用の JSON エンベロープ辞書を返す。
    """
    raw_json = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    salt = os.urandom(SALT_LEN)
    iv = os.urandom(IV_LEN)
    key = derive_aes256_key(password, salt, iterations=iterations)
    ct_and_tag = bcrypt_aes256_gcm_encrypt(raw_json, key, iv)
    return {
        "encrypted": True,
        "version": 1,
        "kdf": "PBKDF2-SHA256",
        "iterations": int(iterations),
        "cipher": "AES-256-GCM",
        "salt": base64.b64encode(salt).decode("ascii"),
        "iv": base64.b64encode(iv).decode("ascii"),
        "payload": base64.b64encode(ct_and_tag).decode("ascii"),
    }


def decrypt_json_envelope(envelope: Dict[str, Any], password: str) -> Any:
    """
    `encrypt_json_envelope` で生成された暗号化エンベロープを復号して元の Python オブジェクトを返す。
    """
    if not isinstance(envelope, dict) or not envelope.get("encrypted"):
        raise ValueError("Invalid encrypted envelope.")
    iterations = int(envelope.get("iterations", PBKDF2_ITERATIONS))
    salt = base64.b64decode(str(envelope["salt"]))
    iv = base64.b64decode(str(envelope["iv"]))
    ct_and_tag = base64.b64decode(str(envelope["payload"]))
    key = derive_aes256_key(password, salt, iterations=iterations)
    plain_bytes = bcrypt_aes256_gcm_decrypt(ct_and_tag, key, iv)
    return json.loads(plain_bytes.decode("utf-8"))


def generate_default_pwa_password() -> str:
    """覚えやすく強固な16文字の初期ロック解除パスフレーズを生成する"""
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
    part1 = "".join(secrets.choice(alphabet) for _ in range(4))
    part2 = "".join(secrets.choice(alphabet) for _ in range(4))
    part3 = "".join(secrets.choice(alphabet) for _ in range(4))
    return f"GMOv2-{part1}-{part2}-{part3}"
