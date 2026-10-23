"""
GMOコイン 24時間全自動売買システム v2 — PWA対応ローカル＆モバイルWebダッシュボード (web_dashboard.py)

機能概要:
- PC (`http://127.0.0.1:8080`)・自宅Wi-Fiスマホ (`http://<LAN_IP>:8080`)・外出先GitHub Pages静的スナップショットの
  3環境すべてで動作する Progressive Web App (PWA) 対応トレーディングコンソール。
- 外部パッケージの追加インストール不要（Python標準ライブラリ `http.server.ThreadingHTTPServer`, `zlib`, `struct`, `socket`
  ＋ 既存 `src/` モジュールのみで PNGアイコン・QRコードSVG・Service Worker・Manifest を動的生成）。

提供エンドポイント:
- `GET  /` (`/index.html`)     : モバイルファースト＆PC対応 ダークモードPWAダッシュボード UI
- `GET  /manifest.json`        : PWA Web App Manifest (standalone, portrait, 192/512 PNG & SVG icons)
- `GET  /sw.js`                : Service Worker (アプリシェルキャッシュ ＆ ワンタップ即時キャッシュクリア対応)
- `GET  /icon-192.png`         : 動的生成 192x192 RGBA PNG アプリアイコン (Android Chrome / iOS apple-touch-icon 対応)
- `GET  /icon-512.png`         : 動的生成 512x512 RGBA PNG アプリアイコン (PWA スプラッシュ画面対応)
- `GET  /icon.svg`             : ベクター SVG アプリアイコン
- `GET  /api/qr?text=...`      : ISO/IEC 18004 準拠 QRコード SVG 動的生成エンドポイント
- `GET  /api/status`           : デーモン状態・総資産・パターンA複利枠・期限カウントダウン・LAN IP・建玉・ログ一括取得
- `GET  /api/chart`            : 指定銘柄・時間足のローソク足＋建玉 Entry/SL/TP 水平ライン取得
- `POST /api/control`          : `PAUSE` / `RESUME` / `CANCEL_ENTRIES` / `FLATTEN_ALL` / `FLATTEN_CONFIRM` の安全な発行・即時同期
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import struct
import subprocess
import sys
import threading
import time
import webbrowser
import zlib
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

sys.dont_write_bytecode = True

from dotenv import load_dotenv

from src.circuit_breaker import MultiTierCircuitBreaker, SingleInstanceLock
from src.database_wal import DatabaseWAL, atomic_write_text
from src.dpapi_vault import (
    build_github_basic_auth_header,
    encrypt_dotenv_secrets_inplace,
    get_secret_env,
    load_and_unprotect_dotenv,
)
from src.gmo_client import GMOPublicClient
from src.gmo_private_client import create_broker_client
from src.json_utils import dumps_json, load_json_file
from src.notifier import mask_secrets
from src.pseudo_oco_manager import PseudoOCOManager
from src.pwa_crypto import encrypt_json_envelope, generate_default_pwa_password
from src.remote_control import (
    RemoteControlManager,
    ReplayOrExpiredNonceError,
    UnauthorizedCommandError,
)
from src.validator import DEFAULT_SYMBOL_RULES

logger = logging.getLogger("web_dashboard")

JST = timezone(timedelta(hours=9))
BASE_DIR = Path(__file__).resolve().parent

DEFAULT_INITIAL_CAPITAL_JPY = 20000.0
DEFAULT_DRAWDOWN_KILL_SWITCH_JPY = 17000.0
DEFAULT_MAX_ALLOCATION_PCT = 0.45

SUPPORTED_CHART_SYMBOLS = ("BTC", "ETH", "XRP", "SOL", "DOGE", "BCH", "LTC")
SUPPORTED_CHART_INTERVALS = ("15min", "1hour", "4hour", "1day")

DEFAULT_PHASE1_END_JST = "2027-07-09T00:00:00+09:00"
DEFAULT_PHASE2_START_JST = "2027-07-10T00:00:00+09:00"
DEFAULT_PHASE2_END_JST = "2027-12-10T00:00:00+09:00"
DEFAULT_PRE_DEADLINE_FREEZE_HOURS = 2.0
DEFAULT_GITHUB_PAGES_URL = "https://qanojisan.github.io/gmo-ai-trader-auto/"

_GIT_PUSH_LOCK = threading.Lock()
_LAST_GIT_PUSH_MONO: float = 0.0
DEFAULT_GIT_PUSH_MIN_INTERVAL_SEC: float = 300.0


def push_docs_snapshot_async(
    base_dir: Path,
    force: bool = False,
    min_interval_sec: float = DEFAULT_GIT_PUSH_MIN_INTERVAL_SEC,
) -> None:
    """
    本番ワークスペース (`BASE_DIR`) で `docs/` が更新された際、
    バックグラウンドスレッド＋プロセス間排他ロック (`v2_git_push.lock`) で
    `docs/` の変更のみを GitHub (`origin main`) へ自動コミット＆プッシュする。
    - `force=False`（定時レーダー周期等）の場合は最小間隔 `min_interval_sec` (デフォルト300秒) でスロットリング。
    - `force=True`（PAUSE/RESUME操作時・約定時）の場合はスロットリングをバイパスして即時Push。
    - 単体テストの一時ディレクトリやリモート未設定環境では何もしない。
    """
    global _LAST_GIT_PUSH_MONO
    try:
        resolved = Path(base_dir).resolve()
        if resolved != BASE_DIR.resolve() or not (resolved / ".git").exists():
            return
    except Exception:
        return

    now_mono = time.monotonic()
    if not force and _LAST_GIT_PUSH_MONO > 0.0 and (now_mono - _LAST_GIT_PUSH_MONO) < min_interval_sec:
        return

    def _worker() -> None:
        global _LAST_GIT_PUSH_MONO
        if not _GIT_PUSH_LOCK.acquire(blocking=False):
            return
        file_lock = SingleInstanceLock(resolved / "v2_git_push.lock")
        try:
            if not file_lock.acquire():
                return

            stamp_file = resolved / "v2_git_push.last"
            if not force and stamp_file.exists():
                try:
                    last_epoch = float(stamp_file.read_text(encoding="utf-8").strip() or "0")
                    if (time.time() - last_epoch) < min_interval_sec:
                        return
                except Exception:
                    pass

            # 万一異常終了した git プロセスの古い .git/index.lock (120秒以上前) が残っていれば安全に除去
            git_index_lock = resolved / ".git" / "index.lock"
            if git_index_lock.exists():
                try:
                    if (time.time() - git_index_lock.stat().st_mtime) > 120.0:
                        git_index_lock.unlink(missing_ok=True)
                    else:
                        return
                except Exception:
                    return

            creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            r_remote = subprocess.run(
                ["git", "-C", str(resolved), "remote", "get-url", "origin"],
                capture_output=True,
                text=True,
                timeout=10,
                creationflags=creation_flags,
            )
            if r_remote.returncode != 0 or not r_remote.stdout.strip():
                return

            subprocess.run(
                ["git", "-C", str(resolved), "add", "docs/"],
                capture_output=True,
                text=True,
                timeout=15,
                creationflags=creation_flags,
            )
            r_diff = subprocess.run(
                ["git", "-C", str(resolved), "diff", "--cached", "--quiet", "--", "docs/"],
                capture_output=True,
                text=True,
                timeout=10,
                creationflags=creation_flags,
            )
            if r_diff.returncode == 0:
                return

            subprocess.run(
                [
                    "git",
                    "-C",
                    str(resolved),
                    "commit",
                    "-m",
                    "chore(pwa-snapshot): auto-update v2 status_snapshot.json [skip ci]",
                    "--",
                    "docs/",
                ],
                capture_output=True,
                text=True,
                timeout=20,
                creationflags=creation_flags,
            )
            auth_header = build_github_basic_auth_header(dotenv_path=resolved / ".env")
            push_cmd = ["git", "-C", str(resolved)]
            if auth_header:
                push_cmd.extend(["-c", f"http.extraheader={auth_header}"])
            push_cmd.extend(["push", "origin", "main"])
            r_push = subprocess.run(
                push_cmd,
                capture_output=True,
                text=True,
                timeout=30,
                creationflags=creation_flags,
            )
            if r_push.returncode == 0:
                _LAST_GIT_PUSH_MONO = time.monotonic()
                try:
                    stamp_file.write_text(f"{time.time():.3f}", encoding="utf-8")
                except Exception:
                    pass
        except Exception as exc:
            logger.debug("Auto push docs skipped: %s", mask_secrets(str(exc)))
        finally:
            file_lock.release()
            _GIT_PUSH_LOCK.release()

    threading.Thread(target=_worker, daemon=True).start()


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def _parse_iso_jst(iso_str: str, default_iso: str) -> datetime:
    raw = (iso_str or default_iso).strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=JST)
        return dt.astimezone(JST)
    except Exception:
        dt_def = datetime.fromisoformat(default_iso)
        return dt_def.astimezone(JST)


def detect_lan_ipv4() -> str:
    """
    同一Wi-Fi内のスマートフォンからアクセスするための PC のプライベート LAN IPv4 アドレスを自動検出する。
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(0.5)
            # 実際にパケットは送信されないが、デフォルトルートに対応するローカルインターフェースIPが取得できる
            s.connect(("8.8.8.8", 80))
            ip = str(s.getsockname()[0])
            if ip and not ip.startswith("127.") and not ip.startswith("169.254."):
                return ip
    except Exception:
        pass

    try:
        hostname = socket.gethostname()
        for addr_info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = str(addr_info[4][0])
            if ip and not ip.startswith("127.") and not ip.startswith("169.254."):
                return ip
    except Exception:
        pass
    return "127.0.0.1"


# ============================================================================
# 1. 純Python 動的アプリアイコン生成 (PNG 192x192 / 512x512 & SVG)
# ============================================================================
_ICON_PNG_CACHE: Dict[int, bytes] = {}


def generate_app_icon_svg() -> str:
    """PWA・ブラウザタブ・ホーム画面用のベクターSVGアイコンを返す"""
    return """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512">
  <defs>
    <linearGradient id="bgGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0b0f19"/>
      <stop offset="100%" stop-color="#1e293b"/>
    </linearGradient>
    <linearGradient id="lineGrad" x1="0%" y1="100%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#38bdf8"/>
      <stop offset="100%" stop-color="#10b981"/>
    </linearGradient>
  </defs>
  <rect width="512" height="512" rx="108" fill="url(#bgGrad)" stroke="#38bdf8" stroke-width="12"/>
  <g opacity="0.18" stroke="#94a3b8" stroke-width="3">
    <line x1="64" y1="140" x2="448" y2="140"/>
    <line x1="64" y1="256" x2="448" y2="256"/>
    <line x1="64" y1="372" x2="448" y2="372"/>
  </g>
  <!-- Candlesticks -->
  <g>
    <line x1="128" y1="270" x2="128" y2="390" stroke="#ef4444" stroke-width="10" stroke-linecap="round"/>
    <rect x="110" y="295" width="36" height="65" rx="6" fill="#ef4444"/>
    <line x1="216" y1="210" x2="216" y2="340" stroke="#10b981" stroke-width="10" stroke-linecap="round"/>
    <rect x="198" y="235" width="36" height="75" rx="6" fill="#10b981"/>
    <line x1="304" y1="170" x2="304" y2="290" stroke="#10b981" stroke-width="10" stroke-linecap="round"/>
    <rect x="286" y="190" width="36" height="70" rx="6" fill="#10b981"/>
    <line x1="392" y1="105" x2="392" y2="235" stroke="#38bdf8" stroke-width="10" stroke-linecap="round"/>
    <rect x="374" y="125" width="36" height="80" rx="6" fill="#38bdf8"/>
  </g>
  <!-- Trend curve -->
  <path d="M 96 376 L 192 290 L 288 240 L 412 128" fill="none" stroke="url(#lineGrad)" stroke-width="16" stroke-linecap="round" stroke-linejoin="round"/>
  <circle cx="412" cy="128" r="18" fill="#10b981" stroke="#0b0f19" stroke-width="6"/>
  <text x="256" y="454" text-anchor="middle" fill="#f1f5f9" font-family="Segoe UI, sans-serif" font-weight="800" font-size="48">AI TRADER v2</text>
</svg>"""


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
    return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", crc)


def generate_app_icon_png(size: int = 192) -> bytes:
    """
    外部画像ライブラリ (Pillow等) を使わず、純Python (`zlib` + `struct`) で
    Android Chrome PWA / iOS `apple-touch-icon` 準拠の正方形 RGBA PNG バイナリを高速生成する。
    """
    sz = max(32, min(1024, int(size)))
    if sz in _ICON_PNG_CACHE:
        return _ICON_PNG_CACHE[sz]

    # 高速スキャンライン構築: ダークネイビー背景 (#0b0f19) + スカイブルー外枠 (#38bdf8) + 3本のローソク足バー
    border = max(3, sz // 32)
    bar_w = max(4, sz // 9)

    # 3本の緑/青の上昇バー領域 (x0, x1, y0, y1, R, G, B)
    bars = [
        (int(sz * 0.20), int(sz * 0.20) + bar_w, int(sz * 0.52), int(sz * 0.78), (239, 68, 68)),
        (int(sz * 0.44), int(sz * 0.44) + bar_w, int(sz * 0.36), int(sz * 0.72), (16, 185, 129)),
        (int(sz * 0.68), int(sz * 0.68) + bar_w, int(sz * 0.20), int(sz * 0.64), (56, 189, 248)),
    ]

    rows: List[bytes] = []
    bg_pixel = b"\x0b\x0f\x19\xff"
    border_pixel = b"\x38\xbd\xf8\xff"

    for y in range(sz):
        if y < border or y >= sz - border:
            row_bytes = b"\x00" + (border_pixel * sz)
            rows.append(row_bytes)
            continue

        line = bytearray(1 + sz * 4)
        line[0] = 0  # Filter type 0 (None)
        # 背景塗りつぶし
        line[1:] = bg_pixel * sz
        # 左右ボーダー
        for bx in range(border):
            line[1 + bx * 4 : 1 + (bx + 1) * 4] = border_pixel
            rx = sz - 1 - bx
            line[1 + rx * 4 : 1 + (rx + 1) * 4] = border_pixel

        # ローソク足バー描画
        for x0, x1, y0, y1, (r, g, b) in bars:
            if y0 <= y <= y1:
                px = bytes((r, g, b, 255))
                span = max(0, x1 - x0)
                if span > 0:
                    line[1 + x0 * 4 : 1 + x1 * 4] = px * span
        rows.append(bytes(line))

    raw_data = b"".join(rows)
    ihdr = struct.pack(">IIBBBBB", sz, sz, 8, 6, 0, 0, 0)  # 8-bit RGBA
    compressed = zlib.compress(raw_data, level=6)
    png_bytes = (
        b"\x89PNG\r\n\x1a\n"
        + _png_chunk(b"IHDR", ihdr)
        + _png_chunk(b"IDAT", compressed)
        + _png_chunk(b"IEND", b"")
    )
    _ICON_PNG_CACHE[sz] = png_bytes
    return png_bytes


# ============================================================================
# 2. 純Python ISO/IEC 18004 準拠 QRコード SVG 生成器 (外部ライブラリ不要)
# ============================================================================
# Version 1〜4 (Byte mode, ECC Level L) パラメータ: (version, size, total_codewords, ec_codewords, align_centers)
_QR_VERSION_SPECS = [
    (1, 21, 26, 7, []),
    (2, 25, 44, 10, [6, 18]),
    (3, 29, 70, 15, [6, 22]),
    (4, 33, 100, 20, [6, 26]),
    (5, 37, 134, 26, [6, 30]),
]


def _gf256_mul(a: int, b: int) -> int:
    res = 0
    while b > 0:
        if b & 1:
            res ^= a
        a <<= 1
        if a & 0x100:
            a ^= 0x11D
        b >>= 1
    return res


def _rs_generator_poly(degree: int) -> List[int]:
    poly = [1]
    root = 1
    for _ in range(degree):
        new_poly = [0] * (len(poly) + 1)
        for i, c in enumerate(poly):
            new_poly[i] ^= c
            new_poly[i + 1] ^= _gf256_mul(c, root)
        poly = new_poly
        root = _gf256_mul(root, 2)
    return poly


def _rs_encode(data_words: List[int], ec_len: int) -> List[int]:
    gen = _rs_generator_poly(ec_len)
    rem = [0] * ec_len
    for b in data_words:
        factor = b ^ rem[0]
        rem = rem[1:] + [0]
        for i in range(ec_len):
            rem[i] ^= _gf256_mul(gen[i + 1], factor)
    return rem


def generate_qr_svg(text: str, box_size: int = 6, border: int = 3) -> str:
    """
    指定した URL / テキストを ISO/IEC 18004 準拠の QRコード (Byte Mode, ECC Level L, Mask 0) にエンコードし、
    スマートフォンカメラで100%読み取り可能な `<svg>` 文字列として返す。
    """
    raw_bytes = (text or "http://127.0.0.1:8080").encode("utf-8")[:104]
    ver_spec = _QR_VERSION_SPECS[-1]
    for spec in _QR_VERSION_SPECS:
        _, _, total_cw, ec_cw, _ = spec
        data_cw_cap = total_cw - ec_cw
        if len(raw_bytes) + 2 <= data_cw_cap:
            ver_spec = spec
            break

    version, size, total_cw, ec_cw, align_centers = ver_spec
    data_cw_len = total_cw - ec_cw

    # 1. データビット列構築: Byte mode (0100) + 8-bit length + payload + terminator
    bits: List[int] = [0, 1, 0, 0]
    n_len = len(raw_bytes)
    for i in range(7, -1, -1):
        bits.append((n_len >> i) & 1)
    for b in raw_bytes:
        for i in range(7, -1, -1):
            bits.append((b >> i) & 1)
    for _ in range(min(4, data_cw_len * 8 - len(bits))):
        bits.append(0)
    while len(bits) % 8 != 0:
        bits.append(0)

    data_words: List[int] = []
    for i in range(0, len(bits), 8):
        val = 0
        for b in bits[i : i + 8]:
            val = (val << 1) | b
        data_words.append(val)

    pad_bytes = [0xEC, 0x11]
    pad_idx = 0
    while len(data_words) < data_cw_len:
        data_words.append(pad_bytes[pad_idx % 2])
        pad_idx += 1

    ec_words = _rs_encode(data_words, ec_cw)
    all_words = data_words + ec_words
    all_bits: List[int] = []
    for w in all_words:
        for i in range(7, -1, -1):
            all_bits.append((w >> i) & 1)

    # 2. マトリクス初期化
    matrix: List[List[Optional[int]]] = [[None for _ in range(size)] for _ in range(size)]
    is_func: List[List[bool]] = [[False for _ in range(size)] for _ in range(size)]

    def _set_func(r: int, c: int, val: int) -> None:
        if 0 <= r < size and 0 <= c < size:
            matrix[r][c] = val
            is_func[r][c] = True

    def _draw_finder(top_r: int, left_c: int) -> None:
        for dr in range(-1, 8):
            for dc in range(-1, 8):
                r, c = top_r + dr, left_c + dc
                if 0 <= r < size and 0 <= c < size:
                    if 0 <= dr <= 6 and 0 <= dc <= 6:
                        if dr in (0, 6) or dc in (0, 6) or (2 <= dr <= 4 and 2 <= dc <= 4):
                            _set_func(r, c, 1)
                        else:
                            _set_func(r, c, 0)
                    else:
                        _set_func(r, c, 0)

    _draw_finder(0, 0)
    _draw_finder(0, size - 7)
    _draw_finder(size - 7, 0)

    # Alignment patterns
    if len(align_centers) >= 2:
        for ar in align_centers:
            for ac in align_centers:
                if (ar == 6 and ac == 6) or (ar == 6 and ac == size - 7) or (ar == size - 7 and ac == 6):
                    continue
                for dr in range(-2, 3):
                    for dc in range(-2, 3):
                        val = 1 if (abs(dr) == 2 or abs(dc) == 2 or (dr == 0 and dc == 0)) else 0
                        _set_func(ar + dr, ac + dc, val)

    # Timing patterns
    for i in range(8, size - 8):
        if not is_func[6][i]:
            _set_func(6, i, 1 if i % 2 == 0 else 0)
        if not is_func[i][6]:
            _set_func(i, 6, 1 if i % 2 == 0 else 0)

    # Dark module & Format info reservation
    _set_func(size - 8, 8, 1)
    for i in range(9):
        if not is_func[8][i]:
            _set_func(8, i, 0)
        if not is_func[i][8]:
            _set_func(i, 8, 0)
    for i in range(8):
        if not is_func[8][size - 1 - i]:
            _set_func(8, size - 1 - i, 0)
        if not is_func[size - 1 - i][8]:
            _set_func(size - 1 - i, 8, 0)

    # 3. データビットのジグザグ配置 + Mask 0 ((r + c) % 2 == 0)
    bit_idx = 0
    col = size - 1
    upward = True
    while col > 0:
        if col == 6:
            col -= 1
        row_seq = range(size - 1, -1, -1) if upward else range(size)
        for r in row_seq:
            for dc in (0, -1):
                c = col + dc
                if not is_func[r][c]:
                    b_val = all_bits[bit_idx] if bit_idx < len(all_bits) else 0
                    bit_idx += 1
                    if (r + c) % 2 == 0:
                        b_val ^= 1
                    matrix[r][c] = b_val
        upward = not upward
        col -= 2

    # 4. Format Info bits for ECC Level L (01) + Mask 0 (000) -> 0x77C4 (111011111000100)
    fmt_val = 0x77C4
    fmt_bits = [(fmt_val >> i) & 1 for i in range(15)]  # bit 0..14
    # Around top-left
    tl_coords_r = [8, 8, 8, 8, 8, 8, 8, 8, 7, 5, 4, 3, 2, 1, 0]
    tl_coords_c = [0, 1, 2, 3, 4, 5, 7, 8, 8, 8, 8, 8, 8, 8, 8]
    for i in range(15):
        matrix[tl_coords_r[i]][tl_coords_c[i]] = fmt_bits[14 - i]
    # Top-right & Bottom-left
    for i in range(7):
        matrix[size - 1 - i][8] = fmt_bits[14 - i]
    for i in range(7, 15):
        matrix[8][size - 15 + i] = fmt_bits[14 - i]

    # 5. SVG レンダリング
    total_modules = size + border * 2
    px_size = total_modules * box_size
    rects: List[str] = []
    for r in range(size):
        for c in range(size):
            if matrix[r][c] == 1:
                x = (c + border) * box_size
                y = (r + border) * box_size
                rects.append(f'<rect x="{x}" y="{y}" width="{box_size}" height="{box_size}"/>')

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {px_size} {px_size}" '
        f'width="{px_size}" height="{px_size}" shape-rendering="crispEdges">'
        f'<rect width="100%" height="100%" fill="#ffffff"/>'
        f'<g fill="#0b0f19">{"".join(rects)}</g>'
        f"</svg>"
    )


# ============================================================================
# 3. PWA Manifest & Service Worker 定義
# ============================================================================
def build_pwa_manifest(start_url: str = "./index.html") -> Dict[str, Any]:
    return {
        "name": "GMO AI Trader v2 (Full-Auto)",
        "short_name": "AI Trader v2",
        "description": "GMOコイン 24時間現物全自動売買システム (v2) モバイル＆PC管理アプリ",
        "start_url": start_url,
        "scope": "./",
        "display": "standalone",
        "orientation": "portrait",
        "background_color": "#0b0f19",
        "theme_color": "#0b0f19",
        "icons": [
            {
                "src": "./icon-192.png",
                "sizes": "192x192",
                "type": "image/png",
                "purpose": "any maskable",
            },
            {
                "src": "./icon-512.png",
                "sizes": "512x512",
                "type": "image/png",
                "purpose": "any maskable",
            },
            {
                "src": "./icon.svg",
                "sizes": "any",
                "type": "image/svg+xml",
                "purpose": "any maskable",
            },
        ],
    }


SERVICE_WORKER_JS = r"""// GMO AI Trader v2 - Service Worker (sw.js)
const CACHE_NAME = 'gmo-ai-trader-v2-pwa-v3';
const APP_SHELL = [
  './',
  './index.html',
  './manifest.json',
  './icon-192.png',
  './icon-512.png',
  './icon.svg'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// 画面右上の「🔄 更新」ボタン押下時にキャッシュを即時クリアするメッセージハンドラ
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'CLEAR_CACHE') {
    event.waitUntil(
      caches.keys().then((keys) => Promise.all(keys.map((k) => caches.delete(k)))).then(() => {
        if (event.source && event.source.postMessage) {
          event.source.postMessage({ type: 'CACHE_CLEARED' });
        }
      })
    );
  }
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  const url = new URL(event.request.url);
  // APIエンドポイント・最新スナップショットJSON・外部APIは常にネットワーク優先 (キャッシュ汚染防止)
  if (
    url.origin !== self.location.origin ||
    url.pathname.includes('/api/') ||
    url.pathname.endsWith('status_snapshot.json') ||
    url.pathname.endsWith('chart_snapshots.json')
  ) {
    return;
  }
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response && response.status === 200) {
          const cloned = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, cloned));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
"""


class WebDashboardService:
    """
    WebダッシュボードおよびPWAスナップショット出力のバックエンドサービス。
    """

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        public_client: Optional[GMOPublicClient] = None,
        ticker_cache_ttl_sec: float = 5.0,
        kline_cache_ttl_sec: float = 10.0,
        port: int = 8080,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else BASE_DIR
        load_and_unprotect_dotenv(self.base_dir / ".env")
        self.port = int(port)

        self.portfolio_path = self.base_dir / "portfolio.json"
        self.latest_decision_path = self.base_dir / "latest_decision.json"
        self.capabilities_path = self.base_dir / "capabilities.json"
        self.wal_db_path = self.base_dir / "v2_state_wal.db"
        self.daemon_lock_path = self.base_dir / "v2_daemon.lock"
        self.launcher_lock_path = self.base_dir / "v2_launcher.lock"
        self.log_dir = self.base_dir / "logs"
        self.docs_dir = self.base_dir / "docs"

        self.public_client = public_client or GMOPublicClient(timeout=6, max_retries=2, retry_delay=0.5)
        self.ticker_cache_ttl_sec = float(ticker_cache_ttl_sec)
        self.kline_cache_ttl_sec = float(kline_cache_ttl_sec)

        self._cache_lock = threading.Lock()
        self._cached_tickers: Dict[str, Dict[str, Any]] = {}
        self._cached_tickers_at: float = 0.0
        self._cached_klines: Dict[Tuple[str, str], Tuple[float, List[Dict[str, Any]]]] = {}
        self._last_charts_map: Dict[str, Any] = {}
        self.lan_ip: str = detect_lan_ipv4()
        self.github_pages_url: str = os.getenv("GITHUB_PAGES_URL", DEFAULT_GITHUB_PAGES_URL).strip()
        self.pwa_unlock_password: str = self._ensure_pwa_unlock_password()

        self.db_wal = DatabaseWAL(
            self.wal_db_path,
            portfolio_json_path=self.portfolio_path,
            auto_sync_files=True,
        )
        self._dummy_broker = create_broker_client(
            execution_mode="DRY_RUN",
            base_dir=self.base_dir,
            public_client=self.public_client,
            wal_db=self.db_wal,
        )
        self.oco_manager = PseudoOCOManager(
            broker=self._dummy_broker,
            portfolio_path=self.portfolio_path,
            symbol_rules=DEFAULT_SYMBOL_RULES,
            notifier=None,
            dry_run_notify=True,
            wal_db=self.db_wal,
        )
        self.remote_control = RemoteControlManager(
            oco_manager=self.oco_manager,
            db_wal=self.db_wal,
            allowed_owner_discord_id="",
            nonce_ttl_seconds=60,
        )
        self.circuit_breaker = MultiTierCircuitBreaker(
            symbol_rules=DEFAULT_SYMBOL_RULES,
            l5_kill_switch_equity_jpy=float(os.getenv("DRAWDOWN_KILL_SWITCH_JPY", str(DEFAULT_DRAWDOWN_KILL_SWITCH_JPY))),
        )

    def _ensure_pwa_unlock_password(self) -> str:
        """
        スマホPWAゼロ知識復号用パスワード (`PWA_UNLOCK_PASSWORD`) を取得する。
        未設定の場合は強固な初期パスフレーズを生成し、`.env` が存在すれば DPAPI 暗号化して保存する。
        """
        env_file = self.base_dir / ".env"
        if env_file.exists():
            load_and_unprotect_dotenv(env_file)
        pw = get_secret_env("PWA_UNLOCK_PASSWORD", "").strip()
        if not pw:
            pw = generate_default_pwa_password()
            os.environ["PWA_UNLOCK_PASSWORD"] = pw
            if env_file.exists():
                try:
                    encrypt_dotenv_secrets_inplace(env_file, extra_secrets={"PWA_UNLOCK_PASSWORD": pw})
                except Exception as exc:
                    logger.warning("PWA_UNLOCK_PASSWORD の DPAPI 保存スキップ: %s", mask_secrets(str(exc)))
        return pw

    def get_pwa_unlock_password(self) -> str:
        if not getattr(self, "pwa_unlock_password", ""):
            self.pwa_unlock_password = self._ensure_pwa_unlock_password()
        return self.pwa_unlock_password

    def close(self) -> None:
        try:
            if self.db_wal is not None:
                self.db_wal.close()
        except Exception:
            pass

    def _is_daemon_running(self) -> Tuple[bool, Optional[int]]:
        probe_lock = SingleInstanceLock(self.daemon_lock_path)
        locked_by_daemon = probe_lock.is_locked_by_another_process()
        launcher_pid: Optional[int] = None
        if self.launcher_lock_path.exists():
            try:
                raw_pid = self.launcher_lock_path.read_text(encoding="ascii", errors="ignore").strip()
                if raw_pid.isdigit():
                    launcher_pid = int(raw_pid)
            except Exception:
                launcher_pid = None
        return locked_by_daemon, launcher_pid

    def _get_cached_tickers(self, force: bool = False) -> Dict[str, Dict[str, Any]]:
        now_mono = time.monotonic()
        with self._cache_lock:
            if (
                not force
                and self._cached_tickers
                and (now_mono - self._cached_tickers_at) < self.ticker_cache_ttl_sec
            ):
                return dict(self._cached_tickers)

        try:
            tickers = self.public_client.get_tickers(list(SUPPORTED_CHART_SYMBOLS))
            if tickers:
                with self._cache_lock:
                    self._cached_tickers = dict(tickers)
                    self._cached_tickers_at = now_mono
                return dict(tickers)
        except Exception as exc:
            logger.debug("Public ticker 取得失敗（キャッシュフォールバック使用）: %s", exc)

        with self._cache_lock:
            return dict(self._cached_tickers)

    def _evaluate_campaign_info(self, pf: Dict[str, Any], now_jst: datetime) -> Dict[str, Any]:
        camp_cfg = pf.get("campaign_schedule") if isinstance(pf.get("campaign_schedule"), dict) else {}
        p1_end = _parse_iso_jst(
            str(camp_cfg.get("phase1_end_jst") or os.getenv("PHASE1_END_JST", DEFAULT_PHASE1_END_JST)),
            DEFAULT_PHASE1_END_JST,
        )
        p2_start = _parse_iso_jst(
            str(camp_cfg.get("phase2_start_jst") or os.getenv("PHASE2_START_JST", DEFAULT_PHASE2_START_JST)),
            DEFAULT_PHASE2_START_JST,
        )
        p2_end = _parse_iso_jst(
            str(camp_cfg.get("phase2_end_jst") or os.getenv("PHASE2_END_JST", DEFAULT_PHASE2_END_JST)),
            DEFAULT_PHASE2_END_JST,
        )
        freeze_hours = float(
            camp_cfg.get("pre_deadline_freeze_hours")
            or os.getenv("PRE_DEADLINE_FREEZE_HOURS", str(DEFAULT_PRE_DEADLINE_FREEZE_HOURS))
        )
        p1_freeze = p1_end - timedelta(hours=freeze_hours)
        p2_freeze = p2_end - timedelta(hours=freeze_hours)

        if now_jst < p1_freeze:
            phase = "PHASE1_ACTIVE"
            phase_label_ja = "第1回 運用期間（通常稼働中）"
            target_deadline = p1_end
            target_label = "第1回締め(2027/7/9)"
        elif now_jst < p1_end:
            phase = "PHASE1_PRE_DEADLINE_FREEZE"
            phase_label_ja = "第1回 終了2時間前（新規BUY凍結・SL監視中）"
            target_deadline = p1_end
            target_label = "第1回全決済(2027/7/9)"
        elif now_jst < p2_start:
            phase = "PHASE1_COMPLETED_INTERMISSION"
            phase_label_ja = "第1回満了・検証休止期間（24hインターミッション）"
            target_deadline = p2_start
            target_label = "第2回再開(2027/7/10)"
        elif now_jst < p2_freeze:
            phase = "PHASE2_ACTIVE"
            phase_label_ja = "第2回 運用期間（通常稼働中）"
            target_deadline = p2_end
            target_label = "第2回締め(2027/12/10)"
        elif now_jst < p2_end:
            phase = "PHASE2_PRE_DEADLINE_FREEZE"
            phase_label_ja = "第2回 終了2時間前（新規BUY凍結・SL監視中）"
            target_deadline = p2_end
            target_label = "第2回全決済(2027/12/10)"
        else:
            phase = "PHASE2_COMPLETED_HALT"
            phase_label_ja = "第2回 運用期間満了（全日程完了・待機中）"
            target_deadline = p2_end
            target_label = "全日程完了"

        rem_sec = max(0, int((target_deadline - now_jst).total_seconds()))
        rem_days = rem_sec // 86400
        rem_hours = (rem_sec % 86400) // 3600
        rem_minutes = (rem_sec % 3600) // 60

        if rem_sec > 0:
            countdown_text = f"{target_label} 残り {rem_days}日 {rem_hours:02d}時間 {rem_minutes:02d}分"
        else:
            countdown_text = "第1回・第2回 全運用期間完了"

        return {
            "current_phase": phase,
            "phase_label_ja": phase_label_ja,
            "phase1_end_jst": p1_end.isoformat(timespec="seconds"),
            "phase1_freeze_start_jst": p1_freeze.isoformat(timespec="seconds"),
            "phase2_start_jst": p2_start.isoformat(timespec="seconds"),
            "phase2_end_jst": p2_end.isoformat(timespec="seconds"),
            "phase2_freeze_start_jst": p2_freeze.isoformat(timespec="seconds"),
            "pre_deadline_freeze_hours": freeze_hours,
            "remaining_days": rem_days,
            "remaining_hours": rem_hours,
            "remaining_minutes": rem_minutes,
            "remaining_total_seconds": rem_sec,
            "countdown_text": countdown_text,
            "phase1_settled": bool(pf.get("phase1_settled", False)),
            "phase2_settled": bool(pf.get("phase2_settled", False)),
        }

    def _read_recent_logs(self, max_lines: int = 30) -> List[str]:
        if not self.log_dir.exists():
            return []
        try:
            log_files = sorted(
                self.log_dir.glob("v2_auto_*.log"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if not log_files:
                return []
            content = log_files[0].read_text(encoding="utf-8", errors="replace")
            lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
            return lines[-max_lines:]
        except Exception:
            return []

    def get_status_payload(self, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        """`GET /api/status` 用の全ステータス辞書を構築する"""
        now_jst = _now_jst(now_dt)
        pf = self.oco_manager.load_portfolio()
        latest_dec: Dict[str, Any] = {}
        if self.latest_decision_path.exists():
            try:
                loaded_dec = load_json_file(self.latest_decision_path)
                if isinstance(loaded_dec, dict):
                    latest_dec = loaded_dec
            except Exception:
                latest_dec = {}

        tickers = self._get_cached_tickers()
        daemon_running, launcher_pid = self._is_daemon_running()
        is_paused = bool(pf.get("is_paused", False))
        exec_mode = str(pf.get("execution_mode") or os.getenv("EXECUTION_MODE", "LIVE")).strip().upper()

        active_positions: List[Dict[str, Any]] = []
        quarantined_dust: List[Dict[str, Any]] = []
        positions_eval_jpy = 0.0
        unrealized_pnl_total_jpy = 0.0

        for pos in pf.get("positions", []) or []:
            if not isinstance(pos, dict):
                continue
            sym = str(pos.get("symbol", "")).strip().upper()
            state = str(pos.get("state", "PROTECTED")).strip().upper()
            size = float(pos.get("size", 0) or 0)
            entry_price = float(pos.get("entry_price", 0) or 0)
            t_info = tickers.get(sym, {})
            cur_bid = float(t_info.get("bid") or t_info.get("last") or pos.get("current_price") or entry_price or 0.0)
            cur_last = float(t_info.get("last") or cur_bid or 0.0)

            if state == "DUST_QUARANTINED":
                quarantined_dust.append(
                    {
                        "symbol": sym,
                        "size": size,
                        "state": state,
                        "current_bid": cur_bid,
                        "estimated_value_jpy": round(size * cur_bid, 1),
                        "opened_at": str(pos.get("opened_at", "")),
                    }
                )
                continue

            eval_jpy = size * cur_bid
            cost_jpy = size * entry_price
            unr_pnl_jpy = eval_jpy - cost_jpy
            unr_pnl_pct = ((cur_bid - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
            positions_eval_jpy += eval_jpy
            unrealized_pnl_total_jpy += unr_pnl_jpy

            if pos.get("trailing_activated"):
                trail_stage = "STAGE_2_TRAILING (+2.2% ATR追従)"
            elif pos.get("breakeven_activated"):
                trail_stage = "STAGE_1_BREAKEVEN (+1.2% 建値ロック)"
            else:
                trail_stage = "INITIAL_SL (初期逆指値保護)"

            active_positions.append(
                {
                    "symbol": sym,
                    "side": str(pos.get("side", "BUY")).upper(),
                    "size": size,
                    "entry_price": entry_price,
                    "current_bid": cur_bid,
                    "current_last": cur_last,
                    "eval_jpy": round(eval_jpy, 0),
                    "unrealized_pnl_jpy": round(unr_pnl_jpy, 0),
                    "unrealized_pnl_pct": round(unr_pnl_pct, 2),
                    "stop_loss_price": float(pos.get("stop_loss_price", 0) or 0),
                    "take_profit_price": float(pos.get("take_profit_price", 0) or 0),
                    "sl_order_id": str(pos.get("sl_order_id", "")),
                    "state": state,
                    "trailing_stage": trail_stage,
                    "opened_at": str(pos.get("opened_at", "")),
                }
            )

        seen_dust_syms = {d["symbol"] for d in quarantined_dust}
        for qd in pf.get("quarantined_dust", []) or []:
            if isinstance(qd, dict):
                q_sym = str(qd.get("symbol", "")).strip().upper()
                if q_sym and q_sym not in seen_dust_syms:
                    q_sz = float(qd.get("size", 0) or 0)
                    t_info = tickers.get(q_sym, {})
                    cur_bid = float(t_info.get("bid") or t_info.get("last") or 0.0)
                    quarantined_dust.append(
                        {
                            "symbol": q_sym,
                            "size": q_sz,
                            "state": "DUST_QUARANTINED",
                            "current_bid": cur_bid,
                            "estimated_value_jpy": round(q_sz * cur_bid, 1),
                            "opened_at": str(qd.get("quarantined_at", "")),
                        }
                    )

        pending_orders: List[Dict[str, Any]] = []
        locked_pending_jpy = 0.0
        for p_ord in pf.get("pending_orders", []) or []:
            if not isinstance(p_ord, dict):
                continue
            p_sym = str(p_ord.get("symbol", "")).strip().upper()
            p_limit = float(p_ord.get("limit_price", 0) or 0)
            p_size = float(p_ord.get("size", 0) or 0)
            p_exec = float(p_ord.get("executed_size", 0) or 0)
            rem_size = max(0.0, p_size - p_exec)
            locked_jpy = float(p_ord.get("reserved_jpy", 0) or (rem_size * p_limit))
            locked_pending_jpy += locked_jpy
            pending_orders.append(
                {
                    "order_id": str(p_ord.get("order_id", "")),
                    "symbol": p_sym,
                    "side": str(p_ord.get("side", "BUY")).upper(),
                    "limit_price": p_limit,
                    "size": p_size,
                    "executed_size": p_exec,
                    "remaining_size": rem_size,
                    "reserved_jpy": round(locked_jpy, 0),
                    "stop_loss_price": float(p_ord.get("stop_loss_price", 0) or 0),
                    "take_profit_price": float(p_ord.get("take_profit_price", 0) or 0),
                    "state": str(p_ord.get("state", "PENDING_BUY")),
                    "created_at": str(p_ord.get("created_at", "")),
                }
            )

        init_cap_jpy = float(pf.get("initial_capital_jpy", DEFAULT_INITIAL_CAPITAL_JPY) or DEFAULT_INITIAL_CAPITAL_JPY)
        init_eq_jpy = float(pf.get("initial_equity_jpy", init_cap_jpy) or init_cap_jpy)
        jpy_balance = float(pf.get("jpy_balance", pf.get("cash_jpy", init_cap_jpy)) or 0.0)
        total_equity_jpy = jpy_balance + locked_pending_jpy + positions_eval_jpy

        l3_status = self.circuit_breaker.check_l3_daily_loss(pf, tickers, now_dt=now_jst)
        l4_status = self.circuit_breaker.check_l4_weekly_and_streak(pf, now_dt=now_jst)
        cons_eq_dec, _ = self.circuit_breaker.calculate_conservative_equity_jpy(pf, tickers)
        cons_eq = float(cons_eq_dec)
        peak_eq = max(
            float(pf.get("peak_equity_jpy", 0.0) or 0.0),
            init_cap_jpy,
            total_equity_jpy,
        )
        init_floor_jpy = max(8500.0, round(init_cap_jpy * 0.85, 2))
        if init_cap_jpy >= DEFAULT_INITIAL_CAPITAL_JPY:
            init_floor_jpy = max(float(DEFAULT_DRAWDOWN_KILL_SWITCH_JPY), init_floor_jpy)
        l5_floor_jpy = max(
            init_floor_jpy,
            round(peak_eq * 0.85, 2),
        )
        alloc_pct = float(pf.get("max_allocation_per_trade_pct", DEFAULT_MAX_ALLOCATION_PCT) or DEFAULT_MAX_ALLOCATION_PCT)
        max_pos_jpy = round(total_equity_jpy * alloc_pct, 0)
        risk_1r_normal_jpy = round(max(150.0, total_equity_jpy * 0.015), 0)
        risk_1r_stressed_jpy = round(max(200.0, total_equity_jpy * 0.020), 0)

        closed_raw = [t for t in (pf.get("closed_trades", []) or []) if isinstance(t, dict)]
        equity_curve: List[Dict[str, Any]] = [
            {
                "index": 0,
                "label": "開始",
                "equity_jpy": round(init_cap_jpy, 0),
                "pnl_jpy": 0,
                "symbol": "INIT",
            }
        ]
        running_eq = init_cap_jpy
        realized_sum = 0.0
        for idx, tr in enumerate(closed_raw, start=1):
            pnl = float(tr.get("pnl_jpy", 0) or 0)
            running_eq += pnl
            realized_sum += pnl
            c_at = str(tr.get("closed_at", ""))
            lbl = c_at[5:16].replace("T", " ") if len(c_at) >= 16 else f"#{idx}"
            equity_curve.append(
                {
                    "index": idx,
                    "label": lbl,
                    "equity_jpy": round(running_eq, 0),
                    "pnl_jpy": round(pnl, 0),
                    "symbol": str(tr.get("symbol", "")),
                    "reason": str(tr.get("reason", "")),
                }
            )
        if len(equity_curve) == 1 or abs(total_equity_jpy - running_eq) >= 1.0:
            equity_curve.append(
                {
                    "index": len(equity_curve),
                    "label": "現在",
                    "equity_jpy": round(total_equity_jpy, 0),
                    "pnl_jpy": round(total_equity_jpy - init_cap_jpy, 0),
                    "symbol": "NOW",
                    "reason": "CURRENT",
                }
            )

        regime_info = latest_dec.get("regime_info") if isinstance(latest_dec.get("regime_info"), dict) else {}
        market_regime = str(regime_info.get("regime", "NEUTRAL") or "NEUTRAL").upper()
        campaign_info = self._evaluate_campaign_info(pf, now_jst)

        lan_url = f"http://{self.lan_ip}:{self.port}"
        local_url = f"http://127.0.0.1:{self.port}"

        return {
            "timestamp": now_jst.isoformat(timespec="seconds"),
            "network_info": {
                "lan_ip": self.lan_ip,
                "port": self.port,
                "lan_url": lan_url,
                "local_url": local_url,
                "github_pages_url": self.github_pages_url,
            },
            "daemon": {
                "is_running": daemon_running,
                "launcher_pid": launcher_pid,
                "execution_mode": exec_mode,
                "is_paused": is_paused,
                "market_regime": market_regime,
                "regime_detail": regime_info,
                "circuit_breakers": {
                    "l1_ws_sequence": "OK",
                    "l2_api_health": "OK",
                    "l3_daily_loss": l3_status,
                    "l4_weekly_streak": l4_status,
                    "l5_kill_switch": {
                        "latched": bool(self.circuit_breaker.l5_kill_switch_latched),
                        "conservative_equity_jpy": round(cons_eq, 0),
                        "peak_equity_jpy": round(peak_eq, 0),
                        "threshold_jpy": round(l5_floor_jpy, 0),
                        "headroom_jpy": round(cons_eq - l5_floor_jpy, 0),
                    },
                },
            },
            "equity_summary": {
                "initial_capital_jpy": round(init_cap_jpy, 0),
                "initial_equity_jpy": round(init_eq_jpy, 0),
                "jpy_balance": round(jpy_balance, 0),
                "cash_jpy": round(jpy_balance, 0),
                "available_jpy": round(max(0.0, jpy_balance - locked_pending_jpy), 0),
                "locked_pending_jpy": round(locked_pending_jpy, 0),
                "positions_eval_jpy": round(positions_eval_jpy, 0),
                "total_equity_jpy": round(total_equity_jpy, 0),
                "conservative_liquidation_jpy": round(cons_eq, 0),
                "total_pnl_jpy": round(total_equity_jpy - init_cap_jpy, 0),
                "total_pnl_pct": round(
                    ((total_equity_jpy - init_cap_jpy) / init_cap_jpy * 100.0) if init_cap_jpy > 0 else 0.0,
                    2,
                ),
                "realized_pnl_total_jpy": round(float(pf.get("realized_pnl_total_jpy", realized_sum) or 0.0), 0),
                "unrealized_pnl_total_jpy": round(unrealized_pnl_total_jpy, 0),
                "peak_equity_jpy": round(peak_eq, 0),
                "day_start_equity_jpy": round(float(l3_status.get("daily_starting_equity_jpy", total_equity_jpy)), 0),
                "week_start_equity_jpy": round(float(l4_status.get("weekly_starting_equity_jpy", total_equity_jpy)), 0),
                "l5_kill_switch_jpy": round(l5_floor_jpy, 0),
                "l5_floor_jpy": round(l5_floor_jpy, 0),
                "max_pos_jpy": max_pos_jpy,
                "pattern_a": {
                    "max_allocation_pct": alloc_pct,
                    "max_position_jpy": max_pos_jpy,
                    "max_concurrent_positions": int(pf.get("max_concurrent_positions", 2) or 2),
                    "risk_1r_normal_jpy": risk_1r_normal_jpy,
                    "risk_1r_stressed_jpy": risk_1r_stressed_jpy,
                },
                "equity_curve": equity_curve,
            },
            "campaign": campaign_info,
            "positions": {
                "active": active_positions,
                "quarantined_dust": quarantined_dust,
            },
            "pending_orders": pending_orders,
            "closed_trades": list(reversed(closed_raw[-30:])),
            "latest_decision": latest_dec,
            "logs": self._read_recent_logs(max_lines=30),
            "tickers": tickers,
        }

    def get_chart_payload(self, symbol: str = "BTC", interval: str = "1hour", limit: int = 72) -> Dict[str, Any]:
        """`GET /api/chart?symbol=BTC&interval=1hour` 用のローソク足＋水平オーバーレイ辞書を返す"""
        sym = str(symbol or "BTC").strip().upper()
        if sym not in SUPPORTED_CHART_SYMBOLS and sym not in DEFAULT_SYMBOL_RULES:
            sym = "BTC"
        iv = str(interval or "1hour").strip().lower()
        if iv not in SUPPORTED_CHART_INTERVALS:
            iv = "1hour"
        lim = max(20, min(150, int(limit or 72)))

        now_mono = time.monotonic()
        cache_key = (sym, iv)
        candles: List[Dict[str, Any]] = []

        with self._cache_lock:
            cached = self._cached_klines.get(cache_key)
            if cached and (now_mono - cached[0]) < self.kline_cache_ttl_sec:
                candles = list(cached[1])

        if not candles:
            try:
                df = self.public_client.get_klines(symbol=sym, interval=iv, limit=lim)
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        open_ms = int(float(row.get("open_time", 0) or 0))
                        dt_jst = datetime.fromtimestamp(open_ms / 1000.0, tz=JST) if open_ms > 0 else _now_jst()
                        lbl = dt_jst.strftime("%m/%d %H:%M") if iv != "1day" else dt_jst.strftime("%Y-%m-%d")
                        candles.append(
                            {
                                "time": int(open_ms // 1000),
                                "time_label": lbl,
                                "open": float(row.get("open", 0) or 0),
                                "high": float(row.get("high", 0) or 0),
                                "low": float(row.get("low", 0) or 0),
                                "close": float(row.get("close", 0) or 0),
                                "volume": float(row.get("volume", 0) or 0),
                            }
                        )
                    with self._cache_lock:
                        self._cached_klines[cache_key] = (now_mono, list(candles))
            except Exception as exc:
                logger.warning("Chart klines 取得失敗 (%s, %s): %s", sym, iv, exc)

        tickers = self._get_cached_tickers()
        ticker_info = tickers.get(sym, {})

        if not candles and ticker_info:
            last_p = float(ticker_info.get("last", 0) or 0)
            if last_p > 0:
                now_jst = _now_jst()
                candles.append(
                    {
                        "time": int(now_jst.timestamp()),
                        "time_label": now_jst.strftime("%m/%d %H:%M"),
                        "open": last_p,
                        "high": float(ticker_info.get("high", last_p) or last_p),
                        "low": float(ticker_info.get("low", last_p) or last_p),
                        "close": last_p,
                        "volume": float(ticker_info.get("volume", 0) or 0),
                    }
                )

        pf = self.oco_manager.load_portfolio()
        overlays: Dict[str, Any] = {
            "has_position": False,
            "has_pending_order": False,
            "entry_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
            "limit_price": None,
            "position_size": 0.0,
            "trailing_stage": "",
        }
        for pos in pf.get("positions", []) or []:
            if not isinstance(pos, dict):
                continue
            if str(pos.get("symbol", "")).strip().upper() == sym and str(pos.get("state", "")).upper() != "DUST_QUARANTINED":
                overlays["has_position"] = True
                overlays["entry_price"] = float(pos.get("entry_price", 0) or 0) or None
                overlays["stop_loss_price"] = float(pos.get("stop_loss_price", 0) or 0) or None
                overlays["take_profit_price"] = float(pos.get("take_profit_price", 0) or 0) or None
                overlays["position_size"] = float(pos.get("size", 0) or 0)
                overlays["trailing_stage"] = (
                    "STAGE_2_TRAILING"
                    if pos.get("trailing_activated")
                    else ("STAGE_1_BREAKEVEN" if pos.get("breakeven_activated") else "INITIAL_SL")
                )
                break

        for p_ord in pf.get("pending_orders", []) or []:
            if not isinstance(p_ord, dict):
                continue
            if str(p_ord.get("symbol", "")).strip().upper() == sym:
                overlays["has_pending_order"] = True
                overlays["limit_price"] = float(p_ord.get("limit_price", 0) or 0) or None
                if not overlays["stop_loss_price"]:
                    overlays["stop_loss_price"] = float(p_ord.get("stop_loss_price", 0) or 0) or None
                if not overlays["take_profit_price"]:
                    overlays["take_profit_price"] = float(p_ord.get("take_profit_price", 0) or 0) or None
                break

        return {
            "symbol": sym,
            "interval": iv,
            "ticker": ticker_info,
            "candles": candles,
            "overlays": overlays,
            "timestamp": _now_jst().isoformat(timespec="seconds"),
        }

    def handle_control_command(
        self,
        command: str,
        nonce: Optional[str] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """`POST /api/control` のコマンド処理およびスナップショット即時更新"""
        now_jst = _now_jst(now_dt)
        cmd_clean = str(command or "").strip().upper()
        nonce_clean = str(nonce or "").strip()

        if cmd_clean not in ("PAUSE", "RESUME", "CANCEL_ENTRIES", "FLATTEN_ALL", "FLATTEN_REQUEST", "FLATTEN_CONFIRM"):
            raise ValueError(f"未対応の制御コマンドです: {cmd_clean}")

        full_cmd = f"{cmd_clean} {nonce_clean}".strip() if (cmd_clean == "FLATTEN_CONFIRM" and nonce_clean) else cmd_clean
        daemon_running, _ = self._is_daemon_running()

        res = self.remote_control.enqueue_or_execute_command(
            command_text=full_cmd,
            is_daemon_running=daemon_running,
            actor_id=self.remote_control.allowed_owner_discord_id or "LOCAL_WEB_OWNER",
            now_dt=now_jst,
        )
        pf_after = self.oco_manager.load_portfolio()
        is_paused_after = bool(pf_after.get("is_paused", False))

        # 状態変更後に docs/status_snapshot.json も非同期更新＆GitHub Pagesへ即時Push (force_push=True)
        try:
            self.export_static_pwa_bundle(include_charts=False, now_dt=now_jst, auto_push=True, force_push=True)
        except Exception:
            pass

        if cmd_clean == "PAUSE":
            msg_ja = "⏸️ 新規買いエントリーを一時停止（PAUSE）しました。保有中ポジションと取引所の逆指値（STOP）は安全に維持されます。"
        elif cmd_clean == "RESUME":
            msg_ja = "▶️ 自動売買（新規買いエントリー監視）を再開（RESUME）しました。"
        elif cmd_clean == "CANCEL_ENTRIES":
            msg_ja = (
                "🧹 未約定の待機買い指値クリア（CANCEL_ENTRIES）を要求しました。"
                if daemon_running
                else f"🧹 未約定の待機買い指値を {len(res.get('canceled_buy_order_ids', []))} 件キャンセルしました。"
            )
        elif cmd_clean in ("FLATTEN_ALL", "FLATTEN_REQUEST"):
            msg_ja = f"⚠️ 緊急全決済のワンタイム承認コード（{res.get('nonce')}）を発行しました。60秒以内に最終承認ボタンを押すと執行されます。"
        else:
            msg_ja = "🚨 緊急全決済（FLATTEN_CONFIRM）を承認・要求しました。全待機注文の取消と保有建玉の成行決済を実行します。"

        return {
            "ok": True,
            "command": cmd_clean,
            "message": msg_ja,
            "is_paused": is_paused_after,
            "daemon_running": daemon_running,
            "result": res,
            "timestamp": now_jst.isoformat(timespec="seconds"),
        }

    def export_static_pwa_bundle(
        self,
        output_dir: Optional[Path] = None,
        include_charts: bool = True,
        now_dt: Optional[datetime] = None,
        auto_push: bool = False,
        force_push: bool = False,
    ) -> Dict[str, Any]:
        """
        外出先 (GitHub Pages 等) からスマートフォン PWA として閲覧するための静的アセット一式と
        AES-256-GCM + PBKDF2-SHA256 (150,000回) で暗号化した最新スナップショット
        (`status_snapshot.json`, `chart_snapshots.json`) を `docs/` にアトミック出力する。
        （※平文の口座情報・資産額・建玉・AI判定・ログ・チャートは `docs/` に1文字も出力しない）
        """
        target_dir = Path(output_dir) if output_dir is not None else self.docs_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        local_url = f"http://127.0.0.1:{self.port}"
        gh_url = self.github_pages_url
        pwa_password = self.get_pwa_unlock_password()

        # 1. PWA 静的シェル (index.html, manifest.json, sw.js, icons, static QR codes)
        atomic_write_text(target_dir / "index.html", DASHBOARD_HTML)
        atomic_write_text(target_dir / "manifest.json", dumps_json(build_pwa_manifest(start_url="./index.html"), indent=2))
        atomic_write_text(target_dir / "sw.js", SERVICE_WORKER_JS)
        atomic_write_text(target_dir / "icon.svg", generate_app_icon_svg())
        atomic_write_text(target_dir / "qr-lan.svg", generate_qr_svg(local_url))
        atomic_write_text(target_dir / "qr-gh.svg", generate_qr_svg(gh_url))
        (target_dir / "icon-192.png").write_bytes(generate_app_icon_png(192))
        (target_dir / "icon-512.png").write_bytes(generate_app_icon_png(512))

        # 2. チャートスナップショット収集 (chart_snapshots.json)
        chart_snap_path = target_dir / "chart_snapshots.json"
        charts_map: Dict[str, Any] = dict(self._last_charts_map)
        if include_charts or not chart_snap_path.exists() or not charts_map:
            fresh_charts: Dict[str, Any] = {}
            for sym in ("BTC", "ETH", "XRP", "SOL", "DOGE", "BCH", "LTC"):
                try:
                    fresh_charts[f"{sym}_1hour"] = self.get_chart_payload(symbol=sym, interval="1hour", limit=48)
                except Exception:
                    pass
            if fresh_charts:
                charts_map = fresh_charts
                self._last_charts_map = dict(fresh_charts)

        if include_charts or not chart_snap_path.exists():
            enc_charts = encrypt_json_envelope(charts_map, pwa_password)
            atomic_write_text(chart_snap_path, dumps_json(enc_charts, indent=2))

        # 3. 最新ステータススナップショット (status_snapshot.json — ゼロ知識 AES-256-GCM 暗号化エンベロープのみ)
        status_payload = self.get_status_payload(now_dt=now_dt)
        status_payload.pop("pwa_unlock_password", None)
        status_payload["static_snapshot"] = True
        if isinstance(status_payload.get("network_info"), dict):
            status_payload["network_info"]["lan_ip"] = "127.0.0.1"
            status_payload["network_info"]["lan_url"] = local_url
        if charts_map:
            status_payload["chart_snapshots"] = charts_map
        enc_status = encrypt_json_envelope(status_payload, pwa_password)
        atomic_write_text(target_dir / "status_snapshot.json", dumps_json(enc_status, indent=2))

        if auto_push and target_dir.resolve() == self.docs_dir.resolve():
            push_docs_snapshot_async(self.base_dir, force=force_push)

        return {
            "ok": True,
            "output_dir": str(target_dir),
            "files": [
                "index.html",
                "manifest.json",
                "sw.js",
                "icon-192.png",
                "icon-512.png",
                "icon.svg",
                "qr-lan.svg",
                "qr-gh.svg",
                "status_snapshot.json",
                "chart_snapshots.json",
            ],
        }


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="AI Trader v2">
<meta name="theme-color" content="#0b0f19">
<link rel="manifest" href="./manifest.json">
<link rel="apple-touch-icon" href="./icon-192.png">
<link rel="icon" type="image/png" sizes="192x192" href="./icon-192.png">
<link rel="icon" type="image/svg+xml" href="./icon.svg">
<title>GMO AI Trader v2 — 24/7 自動売買アプリ</title>
<style>
  :root {
    --bg-main: #0b0f19;
    --bg-card: #111827;
    --bg-card-alt: #1e293b;
    --border-color: #26334d;
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --accent-blue: #38bdf8;
    --accent-green: #10b981;
    --accent-red: #ef4444;
    --accent-amber: #f59e0b;
    --accent-purple: #a855f7;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
  body {
    background: var(--bg-main);
    color: var(--text-primary);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Hiragino Sans", "Yu Gothic UI", "Meiryo", sans-serif;
    line-height: 1.5;
    padding-bottom: 92px;
  }
  .topbar {
    background: rgba(15, 23, 42, 0.94);
    backdrop-filter: blur(10px);
    border-bottom: 1px solid var(--border-color);
    padding: calc(10px + env(safe-area-inset-top, 0px)) 16px 10px 16px;
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
    position: sticky;
    top: 0;
    z-index: 100;
  }
  .brand {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }
  .brand-logo {
    width: 30px;
    height: 30px;
    border-radius: 7px;
  }
  .brand h1 {
    font-size: 1.05rem;
    font-weight: 800;
    letter-spacing: 0.01em;
  }
  .badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 10px;
    border-radius: 999px;
    font-size: 0.74rem;
    font-weight: 700;
    border: 1px solid transparent;
  }
  .badge-live { background: rgba(239,68,68,0.18); color: #fca5a5; border-color: rgba(239,68,68,0.45); }
  .badge-dry { background: rgba(56,189,248,0.18); color: #7dd3fc; border-color: rgba(56,189,248,0.45); }
  .badge-active { background: rgba(16,185,129,0.18); color: #6ee7b7; border-color: rgba(16,185,129,0.45); }
  .badge-paused { background: rgba(245,158,11,0.22); color: #fcd34d; border-color: rgba(245,158,11,0.55); }
  .badge-stopped { background: rgba(148,163,184,0.18); color: #cbd5e1; border-color: rgba(148,163,184,0.4); }
  .badge-regime { background: rgba(168,85,247,0.18); color: #d8b4fe; border-color: rgba(168,85,247,0.45); }
  .badge-campaign { background: rgba(56,189,248,0.14); color: #bae6fd; border-color: rgba(56,189,248,0.35); }

  .container {
    max-width: 1480px;
    margin: 0 auto;
    padding: 14px 16px;
    display: flex;
    flex-direction: column;
    gap: 14px;
  }

  /* Control Bar (Desktop & Tablet) */
  .control-bar {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 13px 16px;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.25);
  }
  .control-buttons {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  .btn {
    cursor: pointer;
    border: 1px solid var(--border-color);
    border-radius: 8px;
    padding: 8px 14px;
    font-size: 0.84rem;
    font-weight: 700;
    color: var(--text-primary);
    background: var(--bg-card-alt);
    transition: all 0.15s ease;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 5px;
  }
  .btn:hover:not(:disabled) { filter: brightness(1.12); }
  .btn:active:not(:disabled) { transform: scale(0.98); }
  .btn:disabled { opacity: 0.42; cursor: not-allowed; }
  .btn-pause { background: #b45309; border-color: #f59e0b; color: #fff; }
  .btn-resume { background: #047857; border-color: #10b981; color: #fff; }
  .btn-cancel { background: #1e3a8a; border-color: #3b82f6; color: #dbeafe; }
  .btn-flatten { background: #7f1d1d; border-color: #ef4444; color: #fee2e2; }
  .btn-qr { background: rgba(56,189,248,0.16); border-color: #38bdf8; color: #bae6fd; }
  .btn-refresh { background: #1e293b; border-color: #475569; color: #e2e8f0; }

  .toast {
    position: fixed;
    top: 68px;
    right: 16px;
    left: 16px;
    max-width: 480px;
    margin: 0 auto;
    padding: 11px 16px;
    border-radius: 10px;
    font-size: 0.84rem;
    font-weight: 700;
    background: rgba(15, 23, 42, 0.96);
    border: 1px solid #38bdf8;
    color: #e0f2fe;
    box-shadow: 0 10px 30px rgba(0,0,0,0.6);
    z-index: 300;
    display: none;
    text-align: center;
  }

  /* Summary KPI Grid */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 12px;
  }
  .kpi-card {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 13px 15px;
  }
  .kpi-label {
    font-size: 0.75rem;
    color: var(--text-secondary);
    margin-bottom: 3px;
    display: flex;
    justify-content: space-between;
  }
  .kpi-value {
    font-size: 1.35rem;
    font-weight: 800;
    letter-spacing: 0.01em;
  }
  .kpi-sub {
    font-size: 0.74rem;
    color: var(--text-secondary);
    margin-top: 4px;
  }
  .pos-val { color: var(--accent-green); }
  .neg-val { color: var(--accent-red); }

  /* Main 2-Column Layout */
  .main-grid {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 14px;
  }
  @media (max-width: 1080px) {
    .main-grid { grid-template-columns: 1fr; }
  }
  .card {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 14px 16px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .card-header {
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    border-bottom: 1px solid rgba(255,255,255,0.06);
    padding-bottom: 8px;
  }
  .card-title {
    font-size: 0.92rem;
    font-weight: 700;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  /* Chart Controls */
  .chart-toolbar {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
    align-items: center;
  }
  .chip {
    cursor: pointer;
    padding: 4px 9px;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 700;
    background: var(--bg-card-alt);
    color: var(--text-secondary);
    border: 1px solid var(--border-color);
  }
  .chip.active {
    background: rgba(56,189,248,0.2);
    color: #38bdf8;
    border-color: #38bdf8;
  }
  .chart-canvas-wrap {
    position: relative;
    width: 100%;
    height: 340px;
    background: #090d16;
    border: 1px solid #1e293b;
    border-radius: 8px;
    overflow: hidden;
  }
  .eq-canvas-wrap {
    position: relative;
    width: 100%;
    height: 165px;
    background: #090d16;
    border: 1px solid #1e293b;
    border-radius: 8px;
    overflow: hidden;
  }
  canvas { width: 100%; height: 100%; display: block; }

  /* Tables */
  .table-wrap { overflow-x: auto; }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.8rem;
  }
  th, td {
    padding: 7px 9px;
    text-align: left;
    border-bottom: 1px solid rgba(255,255,255,0.06);
  }
  th { color: var(--text-secondary); font-weight: 600; background: rgba(255,255,255,0.02); }

  /* Log Viewer */
  .log-box {
    background: #070a12;
    border: 1px solid #1e293b;
    border-radius: 8px;
    padding: 10px 12px;
    font-family: "Consolas", "Monaco", monospace;
    font-size: 0.74rem;
    color: #cbd5e1;
    max-height: 210px;
    overflow-y: auto;
    white-space: pre-wrap;
    word-break: break-all;
  }

  /* Mobile Fixed Bottom Action Bar */
  .mobile-bottom-bar {
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    background: rgba(15, 23, 42, 0.96);
    backdrop-filter: blur(12px);
    border-top: 1px solid var(--border-color);
    padding: 8px 10px calc(8px + env(safe-area-inset-bottom, 0px)) 10px;
    display: none;
    grid-template-columns: repeat(4, 1fr);
    gap: 6px;
    z-index: 150;
  }
  .mobile-bottom-bar .btn {
    padding: 9px 4px;
    font-size: 0.74rem;
    border-radius: 8px;
    flex-direction: column;
    gap: 2px;
  }
  @media (max-width: 768px) {
    .control-bar { display: none; }
    .mobile-bottom-bar { display: grid; }
    .kpi-grid { grid-template-columns: repeat(2, 1fr); gap: 8px; }
    .kpi-value { font-size: 1.15rem; }
    .chart-canvas-wrap { height: 260px; }
  }

  /* Modals */
  .modal-backdrop {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.78);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 250;
    padding: 16px;
  }
  .modal {
    background: var(--bg-card);
    border: 1px solid var(--border-color);
    border-radius: 14px;
    max-width: 580px;
    width: 100%;
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 14px;
    max-height: 90vh;
    overflow-y: auto;
    box-shadow: 0 20px 50px rgba(0,0,0,0.65);
  }
  .modal-danger { border: 2px solid var(--accent-red); }
  .nonce-box {
    background: #090d16;
    border: 1px dashed #ef4444;
    padding: 10px;
    border-radius: 8px;
    font-family: monospace;
    font-size: 0.88rem;
    color: #fde047;
    text-align: center;
  }
  .qr-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 14px;
  }
  .qr-card {
    background: #090d16;
    border: 1px solid var(--border-color);
    border-radius: 10px;
    padding: 12px;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
  }
  .qr-svg-holder svg {
    width: 168px;
    height: 168px;
    border-radius: 8px;
  }
  /* Zero-Knowledge Lock Screen (AES-256-GCM) */
  .zk-lock-overlay {
    position: fixed;
    inset: 0;
    z-index: 600;
    background: radial-gradient(circle at top, #172554 0%, #0b0f19 60%, #050811 100%);
    display: none;
    align-items: center;
    justify-content: center;
    padding: 20px;
  }
  .zk-lock-card {
    background: rgba(17, 24, 39, 0.96);
    border: 1px solid #38bdf8;
    border-radius: 16px;
    max-width: 440px;
    width: 100%;
    padding: 24px 22px;
    box-shadow: 0 25px 60px rgba(0,0,0,0.8);
    display: flex;
    flex-direction: column;
    gap: 14px;
    text-align: center;
  }
  .zk-input {
    width: 100%;
    padding: 12px 14px;
    border-radius: 10px;
    border: 1px solid #334155;
    background: #090d16;
    color: #f8fafc;
    font-size: 0.95rem;
    font-family: monospace;
    outline: none;
  }
  .zk-input:focus {
    border-color: #38bdf8;
    box-shadow: 0 0 0 2px rgba(56,189,248,0.25);
  }
</style>
</head>
<body>

<div id="toastMsg" class="toast"></div>

<!-- 🔐 Zero-Knowledge Lock Screen (AES-256-GCM + PBKDF2 150,000回) -->
<div id="zkLockOverlay" class="zk-lock-overlay">
  <div class="zk-lock-card">
    <div style="display:flex; justify-content:center;">
      <img src="icon.svg" alt="icon" style="width:56px; height:56px; border-radius:14px; border:1px solid #38bdf8;">
    </div>
    <h2 style="font-size:1.05rem; font-weight:800; color:#f1f5f9;">
      🔐 GMO AI Trader v2 — Zero-Knowledge Lock (AES-256-GCM)
    </h2>
    <p style="font-size:0.77rem; color:var(--text-secondary); line-height:1.5;">
      スナップショットは <b>AES-256-GCM + PBKDF2-SHA256 (150,000回)</b> で暗号化されています。<br>
      パスワードはブラウザ内の Web Crypto API でのみメモリ復号され、外部へは一切送信されません。
    </p>
    <div style="position:relative; display:flex; gap:6px;">
      <input id="zkPasswordInput" type="password" class="zk-input" placeholder="ロック解除パスワードを入力..." autocomplete="current-password" onkeydown="if(event.key==='Enter')submitZkUnlock()">
      <button type="button" class="btn" style="padding:0 12px; flex-shrink:0;" onclick="toggleZkInputVisibility()">👁️</button>
    </div>
    <label style="display:flex; align-items:center; justify-content:center; gap:8px; font-size:0.78rem; color:#cbd5e1; cursor:pointer; user-select:none;">
      <input id="zkRememberCheck" type="checkbox" checked style="width:16px; height:16px; accent-color:#38bdf8;">
      <span>この端末にパスワードを記憶する（次回から自動解除）</span>
    </label>
    <div id="zkErrorMsg" style="display:none; font-size:0.8rem; font-weight:700; color:#fca5a5; background:rgba(239,68,68,0.15); border:1px solid rgba(239,68,68,0.4); border-radius:8px; padding:8px;">
      パスワードが正しくありません
    </div>
    <button id="btnZkUnlock" class="btn btn-resume" style="padding:12px; font-size:0.9rem; width:100%;" onclick="submitZkUnlock()">
      🔓 ロック解除
    </button>
    <div style="font-size:0.72rem; color:#64748b;">
      ※ パスワードはPCダッシュボード（127.0.0.1:8080）の「📱 スマホアプリ接続」から確認できます
    </div>
  </div>
</div>

<header class="topbar">
  <div class="brand">
    <img src="icon.svg" alt="icon" class="brand-logo">
    <h1>GMO AI Trader v2</h1>
    <span id="badgeMode" class="badge badge-live">LIVE</span>
    <span id="badgeDaemon" class="badge badge-active">🟢 稼働中</span>
    <span id="badgeRegime" class="badge badge-regime">NEUTRAL</span>
  </div>
  <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
    <span id="badgeCampaign" class="badge badge-campaign">🏁 第1回締め計算中...</span>
    <button class="btn btn-qr" style="padding:5px 10px; font-size:0.76rem;" onclick="openQrModal()">📱 スマホアプリ接続</button>
    <button class="btn btn-refresh" style="padding:5px 10px; font-size:0.76rem;" onclick="forceRefreshPwa()">🔄 更新</button>
    <button id="btnLockPwa" class="btn" style="padding:5px 10px; font-size:0.76rem; display:none; background:rgba(239,68,68,0.16); border-color:#ef4444; color:#fca5a5;" onclick="lockPwaNow()">🔒 ロック</button>
    <span id="clockJst" style="font-size:0.76rem; color:var(--text-secondary); font-family:monospace;">--:-- JST</span>
  </div>
</header>

<main class="container">

  <!-- PC・タブレット向け操作コントロールバー -->
  <section class="control-bar">
    <div>
      <div style="font-size:0.88rem; font-weight:700;">⚡ リアルタイム運用制御（デーモン無停止・0秒同期）</div>
      <div id="controlStatusDesc" style="font-size:0.76rem; color:var(--text-secondary);">
        一時停止（PAUSE）中も保有建玉の逆指値（STOP）と利確（TP）監視は継続されます。
      </div>
    </div>
    <div class="control-buttons">
      <button id="btnPause" class="btn btn-pause" onclick="sendControl('PAUSE')">⏸️ 一時停止 (PAUSE)</button>
      <button id="btnResume" class="btn btn-resume" onclick="sendControl('RESUME')">▶️ 売買再開 (RESUME)</button>
      <button id="btnCancelEntries" class="btn btn-cancel" onclick="confirmCancelEntries()">🧹 待機買い指値クリア</button>
      <button id="btnFlattenAll" class="btn btn-flatten" onclick="openFlattenModal()">🚨 緊急全決済 (2段階承認)</button>
    </div>
  </section>

  <!-- 資産・パターンA複利・防衛ライン KPI (初期HTMLはゼロ知識プレースホルダ) -->
  <section class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-label"><span>💰 現在の総資産</span><span id="kpiInitCap">初期 -- 円</span></div>
      <div id="kpiTotalEquity" class="kpi-value">-- 円</div>
      <div id="kpiCashDetail" class="kpi-sub">現金: -- 円 / 拘束: -- 円</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label"><span>📊 通算損益</span><span id="kpiTotalPnlPct">--%</span></div>
      <div id="kpiTotalPnl" class="kpi-value pos-val">-- 円</div>
      <div id="kpiRealizedDetail" class="kpi-sub">確定: -- 円 / 含み: -- 円</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label"><span>📈 パターンA複利枠(45%)</span><span>最大2銘柄</span></div>
      <div id="kpiMaxAlloc" class="kpi-value" style="color:var(--accent-blue);">-- 円 / 枠</div>
      <div id="kpiRisk1R" class="kpi-sub">1R許容損失(1.5%): -- 円 (上限2%: -- 円)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label"><span>🛡️ HWM & L5防衛線</span><span id="kpiL5Status">確認中</span></div>
      <div id="kpiL5Floor" class="kpi-value" style="color:var(--accent-amber);">-- 円</div>
      <div id="kpiPeakDetail" class="kpi-sub">最高資産: -- 円 (余裕 -- 円)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label"><span>🚦 L3日次 / L4週次制限</span><span id="kpiCbBadge">確認中</span></div>
      <div id="kpiDailyPnl" class="kpi-value" style="font-size:1.08rem;">日次: -- 円 / 制限 -- 円</div>
      <div id="kpiWeeklyPnl" class="kpi-sub">週次: -- 円 (制限 -- 円) / 連敗: --</div>
    </div>
  </section>

  <!-- メイン2カラム: 左=価格推移チャート、右=資産推移＆AI判定 -->
  <section class="main-grid">
    <div class="card">
      <div class="card-header">
        <div class="card-title">
          <span>📉 GMOコイン 現物チャート</span>
          <span id="chartTickerSummary" style="font-size:0.8rem; color:var(--accent-blue); font-family:monospace;"></span>
        </div>
        <div class="chart-toolbar">
          <div id="symbolChips" style="display:flex; gap:4px; flex-wrap:wrap;"></div>
          <span style="color:#475569;">|</span>
          <div id="intervalChips" style="display:flex; gap:4px;"></div>
        </div>
      </div>
      <div class="chart-canvas-wrap">
        <canvas id="priceChartCanvas"></canvas>
      </div>
      <div style="font-size:0.74rem; color:var(--text-secondary); display:flex; justify-content:space-between; flex-wrap:wrap;">
        <span>💡 保有建玉の Entry(青) / TP利確(緑) / SL逆指値(赤) / 待機指値(黄) を自動描画</span>
        <span id="chartUpdatedTime"></span>
      </div>
    </div>

    <div style="display:flex; flex-direction:column; gap:14px;">
      <div class="card">
        <div class="card-header">
          <div class="card-title">📈 総資産推移 (Equity Curve)</div>
          <span id="campaignPhaseBadge" style="font-size:0.73rem; color:var(--accent-blue);">PHASE1_ACTIVE</span>
        </div>
        <div class="eq-canvas-wrap">
          <canvas id="equityChartCanvas"></canvas>
        </div>
        <div style="font-size:0.73rem; color:var(--text-secondary);">
          ・第1回締め: 2027-07-09 00:00 JST (2時間前凍結 ➔ 全決済)<br>
          ・第2回期間: 2027-07-10 00:00 〜 2027-12-10 00:00 JST
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <div class="card-title">🧠 最新レーダー＆AI判定</div>
          <span id="aiDecAction" class="badge badge-stopped">HOLD</span>
        </div>
        <div id="aiDecMeta" style="font-size:0.75rem; color:var(--text-secondary);"></div>
        <div id="aiDecReasoning" style="font-size:0.79rem; background:#090d16; padding:9px; border-radius:8px; border:1px solid #1e293b; max-height:130px; overflow-y:auto;">
          読み込み中...
        </div>
      </div>
    </div>
  </section>

  <!-- 保有ポジション ＆ 待機注文一覧 -->
  <section class="main-grid">
    <div class="card">
      <div class="card-header">
        <div class="card-title">🛡️ 保有中ポジション (有効建玉 ＆ 逆指値保護状態)</div>
        <span id="activePosCount" style="font-size:0.76rem; color:var(--text-secondary);">0 銘柄</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>銘柄</th>
              <th>数量</th>
              <th>取得単価</th>
              <th>現在Bid</th>
              <th>評価額</th>
              <th>含み損益</th>
              <th>SL / TP</th>
              <th>段階</th>
            </tr>
          </thead>
          <tbody id="activePositionsBody">
            <tr><td colspan="8" style="text-align:center; color:var(--text-secondary);">現在保有中の有効ポジションはありません</td></tr>
          </tbody>
        </table>
      </div>
      <div id="dustQuarantineBox" style="font-size:0.74rem; color:var(--text-secondary); padding-top:4px;"></div>
    </div>

    <div class="card">
      <div class="card-header">
        <div class="card-title">⏳ 待機中指値注文 (`pending_orders`)</div>
        <span id="pendingOrdCount" style="font-size:0.76rem; color:var(--text-secondary);">0 件</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>銘柄</th>
              <th>指値価格</th>
              <th>数量(約定)</th>
              <th>拘束額</th>
              <th>予定SL / TP</th>
              <th>状態</th>
            </tr>
          </thead>
          <tbody id="pendingOrdersBody">
            <tr><td colspan="6" style="text-align:center; color:var(--text-secondary);">現在待機中の指値注文はありません</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>

  <!-- 決済履歴 ＆ リアルタイム稼働ログ -->
  <section class="main-grid">
    <div class="card">
      <div class="card-header">
        <div class="card-title">🏆 直近の確定決済履歴 (`closed_trades`)</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>決済日時</th>
              <th>銘柄</th>
              <th>数量</th>
              <th>Entry ➔ Exit</th>
              <th>確定損益</th>
              <th>理由</th>
            </tr>
          </thead>
          <tbody id="closedTradesBody">
            <tr><td colspan="6" style="text-align:center; color:var(--text-secondary);">まだ確定した決済履歴はありません</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <div class="card-title">📜 リアルタイム稼働ログ (`logs/v2_auto_*.log`)</div>
      </div>
      <div id="logViewer" class="log-box">ログ読み込み中...</div>
    </div>
  </section>

</main>

<!-- スマホ専用 固定ボトムアクションバー (親指操作対応) -->
<nav class="mobile-bottom-bar">
  <button id="btnPauseMobile" class="btn btn-pause" onclick="sendControl('PAUSE')">
    <span>⏸️</span><span>一時停止</span>
  </button>
  <button id="btnResumeMobile" class="btn btn-resume" onclick="sendControl('RESUME')">
    <span>▶️</span><span>売買再開</span>
  </button>
  <button id="btnCancelMobile" class="btn btn-cancel" onclick="confirmCancelEntries()">
    <span>🧹</span><span>指値クリア</span>
  </button>
  <button id="btnFlattenMobile" class="btn btn-flatten" onclick="openFlattenModal()">
    <span>🚨</span><span>緊急全決済</span>
  </button>
</nav>

<!-- スマホアプリ接続・QRコードモーダル -->
<div id="qrModal" class="modal-backdrop" onclick="if(event.target===this)closeQrModal()">
  <div class="modal">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <h3 style="font-size:1.05rem;">📱 スマートフォン専用アプリ（PWA）接続・インストール</h3>
      <button class="btn" style="padding:4px 10px;" onclick="closeQrModal()">✕ 閉じる</button>
    </div>
    <p style="font-size:0.8rem; color:var(--text-secondary);">
      スマホのカメラで以下のQRコードを読み取って開き、ブラウザメニューから<b>「ホーム画面に追加（アプリをインストール）」</b>を選ぶと、フルスクリーンのv2専用アプリとして起動できます（自宅Wi-Fi・外出先4G/5G 共通）。
    </p>
    <div class="qr-card" style="max-width:340px; margin:0 auto; width:100%;">
      <div style="font-size:0.85rem; font-weight:700; color:#d8b4fe;">📱 GMO AI Trader v2 スマホ専用アプリ（GitHub Pages PWA）</div>
      <div id="qrGhHolder" class="qr-svg-holder">読み込み中...</div>
      <a id="qrGhLink" href="https://qanojisan.github.io/gmo-ai-trader-auto/" target="_blank" style="font-family:monospace; font-size:0.82rem; color:#38bdf8; word-break:break-all;">https://qanojisan.github.io/gmo-ai-trader-auto/</a>
      <div style="font-size:0.74rem; color:var(--text-secondary);">最新スナップショット＆リアルタイムチャートを高速表示（PC操作は <code style="color:#6ee7b7;">http://127.0.0.1:8080</code>）</div>
    </div>
    <!-- PCローカル表示時のみスマホロック解除パスワードを表示 -->
    <div id="pwaPasswordCard" style="display:none; background:#090d16; border:1px solid #38bdf8; border-radius:10px; padding:12px; flex-direction:column; gap:8px;">
      <div style="font-size:0.82rem; font-weight:700; color:#7dd3fc;">🔐 スマホPWA ロック解除パスワード (AES-256-GCM ゼロ知識復号用)</div>
      <div style="display:flex; gap:6px; align-items:center;">
        <input id="pwaPasswordDisplay" type="password" readonly class="zk-input" style="padding:8px 10px; font-size:0.88rem;" value="">
        <button type="button" id="btnTogglePwaPw" class="btn" style="padding:8px 10px; font-size:0.76rem; flex-shrink:0;" onclick="togglePwaPasswordVisibility()">👁️ 表示</button>
        <button type="button" id="btnCopyPwaPw" class="btn btn-qr" style="padding:8px 10px; font-size:0.76rem; flex-shrink:0;" onclick="copyPwaPassword()">📋 コピー</button>
      </div>
      <div style="font-size:0.73rem; color:var(--text-secondary);">
        ※ 初回のみスマホ画面で上記パスワードを入力してください（「端末に記憶」ONで次回から0秒自動解除されます）。
      </div>
    </div>
    <div style="background:#090d16; border:1px solid #1e293b; border-radius:8px; padding:10px 12px; font-size:0.76rem; color:var(--text-secondary);">
      <b>📲 スマホへのアプリ追加手順：</b><br>
      ・<b>iPhone (Safari)</b>：下部中央の「共有ボタン（□に↑）」➔ <b>「ホーム画面に追加」</b><br>
      ・<b>Android (Chrome)</b>：右上メニュー「⋮」➔ <b>「アプリをインストール」または「ホーム画面に追加」</b>
    </div>
  </div>
</div>

<!-- 2段階承認 FLATTEN_ALL モーダル -->
<div id="flattenModal" class="modal-backdrop" onclick="if(event.target===this)closeFlattenModal()">
  <div class="modal modal-danger">
    <h3 style="color:#fca5a5;">🚨 緊急全決済 (FLATTEN_ALL) — 2段階承認</h3>
    <p style="font-size:0.84rem; color:var(--text-secondary);">
      この操作を実行すると、<b>全ての未約定買い指値をキャンセル</b>し、<b>現在保有中の全ポジションを即時成行（MARKET）で全決済</b>した上で、新規エントリーを一時停止（PAUSE）します。
    </p>
    <div id="flattenNonceDisplay" class="nonce-box">ワンタイム承認コード発行中...</div>
    <div style="display:flex; justify-content:flex-end; gap:10px;">
      <button class="btn" onclick="closeFlattenModal()">キャンセル</button>
      <button id="btnConfirmFlattenNow" class="btn btn-flatten" disabled onclick="confirmFlattenAll()">⚠️ 本当に全決済を実行する</button>
    </div>
  </div>
</div>

<script>
// Service Worker 登録
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('./sw.js').catch(() => {});
  });
}

const SYMBOLS = ["BTC", "ETH", "XRP", "SOL", "DOGE", "BCH", "LTC"];
const INTERVALS = [
  { key: "15min", label: "15m" },
  { key: "1hour", label: "1h" },
  { key: "4hour", label: "4h" },
  { key: "1day",  label: "1D" }
];

let selectedSymbol = "BTC";
let selectedInterval = "1hour";
let currentFlattenNonce = "";
let lastChartData = null;
let lastEquityCurve = [];
let isStaticSnapshotMode = false;
let activeApiBase = ".";
let latestStatusData = null;
let latestNetworkInfo = {
  lan_url: "http://127.0.0.1:8080",
  github_pages_url: "https://qanojisan.github.io/gmo-ai-trader-auto/"
};
const PWA_PW_STORAGE_KEY = "gmo_v2_pwa_unlock_pw";
let unlockedPwaPassword = "";
let lastEncryptedStatusEnvelope = null;
let decryptedChartSnapshotsMap = null;
let localServerPwaPassword = "";

function b64ToBytes(b64) {
  const bin = atob(String(b64 || "").trim());
  const arr = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) {
    arr[i] = bin.charCodeAt(i);
  }
  return arr;
}

async function decryptEnvelopeWebCrypto(envelope, password) {
  if (!envelope || !envelope.encrypted) return envelope;
  const pw = String(password || "").trim();
  if (!pw) throw new Error("EMPTY_PASSWORD");
  const enc = new TextEncoder();
  const salt = b64ToBytes(envelope.salt);
  const iv = b64ToBytes(envelope.iv);
  const ctAndTag = b64ToBytes(envelope.payload);
  const iterations = Number(envelope.iterations || 150000);

  const baseKey = await window.crypto.subtle.importKey(
    "raw",
    enc.encode(pw),
    { name: "PBKDF2" },
    false,
    ["deriveKey"]
  );
  const aesKey = await window.crypto.subtle.deriveKey(
    {
      name: "PBKDF2",
      salt: salt,
      iterations: iterations,
      hash: "SHA-256"
    },
    baseKey,
    { name: "AES-GCM", length: 256 },
    false,
    ["decrypt"]
  );
  const plainBuf = await window.crypto.subtle.decrypt(
    { name: "AES-GCM", iv: iv, tagLength: 128 },
    aesKey,
    ctAndTag
  );
  const jsonText = new TextDecoder("utf-8").decode(plainBuf);
  return JSON.parse(jsonText);
}

function showZkLockScreen(showError=false) {
  const ov = document.getElementById("zkLockOverlay");
  const errEl = document.getElementById("zkErrorMsg");
  if (ov) ov.style.display = "flex";
  if (errEl) errEl.style.display = showError ? "block" : "none";
}

function hideZkLockScreen() {
  const ov = document.getElementById("zkLockOverlay");
  const errEl = document.getElementById("zkErrorMsg");
  if (ov) ov.style.display = "none";
  if (errEl) errEl.style.display = "none";
}

function toggleZkInputVisibility() {
  const inp = document.getElementById("zkPasswordInput");
  if (!inp) return;
  inp.type = inp.type === "password" ? "text" : "password";
}

async function submitZkUnlock() {
  const inp = document.getElementById("zkPasswordInput");
  const rem = document.getElementById("zkRememberCheck");
  const btn = document.getElementById("btnZkUnlock");
  const pw = inp ? inp.value.trim() : "";
  if (!pw) {
    showZkLockScreen(true);
    return;
  }
  if (btn) btn.disabled = true;
  try {
    if (!lastEncryptedStatusEnvelope) {
      const snapRes = await fetch(`./status_snapshot.json?_t=${Date.now()}`, { cache: "no-store" });
      if (snapRes.ok) {
        lastEncryptedStatusEnvelope = await snapRes.json();
      }
    }
    const dec = await decryptEnvelopeWebCrypto(lastEncryptedStatusEnvelope, pw);
    unlockedPwaPassword = pw;
    if (rem && rem.checked) {
      try { localStorage.setItem(PWA_PW_STORAGE_KEY, pw); } catch (_) {}
    } else {
      try { localStorage.removeItem(PWA_PW_STORAGE_KEY); } catch (_) {}
    }
    hideZkLockScreen();
    const lockBtn = document.getElementById("btnLockPwa");
    if (lockBtn) lockBtn.style.display = "inline-flex";
    latestStatusData = dec;
    if (dec && dec.chart_snapshots) {
      decryptedChartSnapshotsMap = dec.chart_snapshots;
    }
    updateDashboardUI(dec);
    await fetchChart();
    showToast("🔓 ゼロ知識復号が完了しました");
  } catch (_) {
    showZkLockScreen(true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

function lockPwaNow() {
  unlockedPwaPassword = "";
  latestStatusData = null;
  decryptedChartSnapshotsMap = null;
  try { localStorage.removeItem(PWA_PW_STORAGE_KEY); } catch (_) {}
  const inp = document.getElementById("zkPasswordInput");
  if (inp) inp.value = "";
  showZkLockScreen(false);
  showToast("🔒 ロックしました（保存パスワードを消去しました）");
}

function togglePwaPasswordVisibility() {
  const el = document.getElementById("pwaPasswordDisplay");
  const btn = document.getElementById("btnTogglePwaPw");
  if (!el) return;
  if (el.type === "password") {
    el.type = "text";
    if (btn) btn.textContent = "🙈 隠す";
  } else {
    el.type = "password";
    if (btn) btn.textContent = "👁️ 表示";
  }
}

async function copyPwaPassword() {
  if (!localServerPwaPassword) return;
  try {
    await navigator.clipboard.writeText(localServerPwaPassword);
    showToast("📋 スマホロック解除パスワードをコピーしました");
  } catch (_) {
    const el = document.getElementById("pwaPasswordDisplay");
    if (el) {
      const prevType = el.type;
      el.type = "text";
      el.select();
      document.execCommand("copy");
      el.type = prevType;
      showToast("📋 スマホロック解除パスワードをコピーしました");
    }
  }
}

async function handleStatusResponseData(rawData, isStatic) {
  activeApiBase = ".";
  isStaticSnapshotMode = Boolean(isStatic);
  if (rawData && rawData.encrypted) {
    isStaticSnapshotMode = true;
    lastEncryptedStatusEnvelope = rawData;
    const lockBtn = document.getElementById("btnLockPwa");
    if (lockBtn) lockBtn.style.display = "inline-flex";
    let pw = unlockedPwaPassword;
    if (!pw) {
      try { pw = localStorage.getItem(PWA_PW_STORAGE_KEY) || ""; } catch (_) {}
    }
    if (!pw) {
      showZkLockScreen(false);
      return;
    }
    try {
      const dec = await decryptEnvelopeWebCrypto(rawData, pw);
      unlockedPwaPassword = pw;
      hideZkLockScreen();
      latestStatusData = dec;
      if (dec && dec.chart_snapshots) {
        decryptedChartSnapshotsMap = dec.chart_snapshots;
      }
      updateDashboardUI(dec);
    } catch (_) {
      unlockedPwaPassword = "";
      try { localStorage.removeItem(PWA_PW_STORAGE_KEY); } catch (_) {}
      showZkLockScreen(false);
    }
    return;
  }
  hideZkLockScreen();
  if (rawData && rawData.pwa_unlock_password) {
    localServerPwaPassword = String(rawData.pwa_unlock_password);
    const pwCard = document.getElementById("pwaPasswordCard");
    const pwInput = document.getElementById("pwaPasswordDisplay");
    if (pwCard) pwCard.style.display = "flex";
    if (pwInput) pwInput.value = localServerPwaPassword;
  }
  latestStatusData = rawData;
  updateDashboardUI(rawData);
}

function fmtJpy(val, showSign=false) {
  const n = Number(val || 0);
  const sign = showSign && n > 0 ? "+" : "";
  return sign + Math.round(n).toLocaleString("ja-JP") + " 円";
}

function fmtPrice(val) {
  const n = Number(val || 0);
  if (n >= 1000) return n.toLocaleString("ja-JP", { maximumFractionDigits: 1 });
  return n.toLocaleString("ja-JP", { maximumFractionDigits: 4 });
}

function showToast(msg, isError=false) {
  const el = document.getElementById("toastMsg");
  el.style.display = "block";
  el.style.borderColor = isError ? "#ef4444" : "#38bdf8";
  el.textContent = msg;
  setTimeout(() => { el.style.display = "none"; }, 5000);
}

function renderToolbarChips(tickers={}) {
  const symWrap = document.getElementById("symbolChips");
  symWrap.innerHTML = "";
  SYMBOLS.forEach(sym => {
    const btn = document.createElement("button");
    btn.className = "chip" + (sym === selectedSymbol ? " active" : "");
    const t = tickers[sym];
    const pStr = t && t.last ? ` ${fmtPrice(t.last)}` : "";
    btn.textContent = sym + pStr;
    btn.onclick = () => {
      selectedSymbol = sym;
      renderToolbarChips(tickers);
      fetchChart();
    };
    symWrap.appendChild(btn);
  });

  const ivWrap = document.getElementById("intervalChips");
  ivWrap.innerHTML = "";
  INTERVALS.forEach(item => {
    const btn = document.createElement("button");
    btn.className = "chip" + (item.key === selectedInterval ? " active" : "");
    btn.textContent = item.label;
    btn.onclick = () => {
      selectedInterval = item.key;
      renderToolbarChips(tickers);
      fetchChart();
    };
    ivWrap.appendChild(btn);
  });
}

async function fetchWithTimeout(url, options={}, timeoutMs=2000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(id);
  }
}

async function fetchStatus() {
  // 1. GitHub Pages (*.github.io) または HTTPS 静的配信環境の場合は、
  //    Mixed Content ブロックや LAN タイムアウト待ちを完全にスキップし、待ち時間 0 秒で status_snapshot.json を取得・メモリ復号
  if (location.hostname.endsWith("github.io") || location.protocol === "https:") {
    try {
      const snapRes = await fetch(`./status_snapshot.json?_t=${Date.now()}`, { cache: "no-store" });
      if (snapRes.ok) {
        const snapData = await snapRes.json();
        await handleStatusResponseData(snapData, true);
        return;
      }
    } catch (err) {
      console.error("Snapshot fetch error:", err);
    }
    return;
  }

  // 2. PC ローカル環境 (http://127.0.0.1:8080 等): 同一オリジン /api/status へ直接通信 (ロック画面なし・0秒表示)
  try {
    const res = await fetchWithTimeout(`./api/status?_t=${Date.now()}`, { cache: "no-store" }, 3500);
    const ct = (res.headers.get("content-type") || "").toLowerCase();
    if (res.ok && ct.includes("json")) {
      const data = await res.json();
      await handleStatusResponseData(data, false);
      return;
    }
  } catch (_) {}

  // 3. フォールバック: status_snapshot.json
  try {
    const snapRes = await fetch(`./status_snapshot.json?_t=${Date.now()}`, { cache: "no-store" });
    if (snapRes.ok) {
      const snapData = await snapRes.json();
      await handleStatusResponseData(snapData, true);
    }
  } catch (_) {}
}

async function fetchChart() {
  try {
    // 1. ローカル/LAN バックエンド接続時
    if (!isStaticSnapshotMode) {
      const res = await fetchWithTimeout(
        `${activeApiBase}/api/chart?symbol=${encodeURIComponent(selectedSymbol)}&interval=${encodeURIComponent(selectedInterval)}&_t=${Date.now()}`,
        { cache: "no-store" },
        3500
      );
      if (res.ok) {
        const data = await res.json();
        lastChartData = data;
        drawPriceChart(data);
        return;
      }
    }

    // 2. 外出先 (GitHub Pages) の場合: GMO Public API からの直接取得を試み、CORS制限時は chart_snapshots.json へ自動フォールバック
    try {
      const jstNow = new Date(Date.now() + 9 * 3600 * 1000);
      const yStr = String(jstNow.getUTCFullYear());
      const mStr = String(jstNow.getUTCMonth() + 1).padStart(2, "0");
      const dStr = String(jstNow.getUTCDate()).padStart(2, "0");
      const dateParam = selectedInterval === "1day" ? yStr : `${yStr}${mStr}${dStr}`;
      const gmoRes = await fetchWithTimeout(
        `https://api.coin.z.com/public/v1/klines?symbol=${encodeURIComponent(selectedSymbol)}&interval=${encodeURIComponent(selectedInterval)}&date=${dateParam}`,
        { cache: "no-store" },
        2200
      );
      if (gmoRes.ok) {
        const gmoJson = await gmoRes.json();
        const rawList = (gmoJson && gmoJson.data) || [];
        if (rawList.length > 0) {
          const candles = rawList.slice(-72).map(r => {
            const ms = Number(r.openTime || 0);
            const dt = new Date(ms + 9 * 3600 * 1000);
            const lbl = `${String(dt.getUTCMonth()+1).padStart(2,"0")}/${String(dt.getUTCDate()).padStart(2,"0")} ${String(dt.getUTCHours()).padStart(2,"0")}:${String(dt.getUTCMinutes()).padStart(2,"0")}`;
            return {
              open_time: ms,
              time_label: lbl,
              open: Number(r.open),
              high: Number(r.high),
              low: Number(r.low),
              close: Number(r.close),
              volume: Number(r.volume || 0)
            };
          });
          const tMap = (latestStatusData && latestStatusData.tickers) || {};
          const tk = tMap[selectedSymbol] || { last: candles[candles.length - 1].close, bid: candles[candles.length - 1].close, ask: candles[candles.length - 1].close };
          const liveChart = {
            symbol: selectedSymbol,
            interval: selectedInterval,
            timestamp: new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 19) + "+09:00",
            ticker: tk,
            candles: candles,
            overlays: {}
          };
          lastChartData = liveChart;
          drawPriceChart(liveChart);
          return;
        }
      }
    } catch (_) {}

    // 3. 静的スナップショット (chart_snapshots.json / 復号済みキャッシュ) フォールバック
    if (decryptedChartSnapshotsMap) {
      const chCached = decryptedChartSnapshotsMap[`${selectedSymbol}_${selectedInterval}`] || decryptedChartSnapshotsMap[`${selectedSymbol}_1hour`] || decryptedChartSnapshotsMap["BTC_1hour"];
      if (chCached) {
        lastChartData = chCached;
        drawPriceChart(chCached);
        return;
      }
    }
    const snapRes = await fetch(`./chart_snapshots.json?_t=${Date.now()}`, { cache: "no-store" });
    if (snapRes.ok) {
      const rawMap = await snapRes.json();
      let map = rawMap;
      if (rawMap && rawMap.encrypted) {
        let pw = unlockedPwaPassword;
        if (!pw) {
          try { pw = localStorage.getItem(PWA_PW_STORAGE_KEY) || ""; } catch (_) {}
        }
        if (!pw) return;
        map = await decryptEnvelopeWebCrypto(rawMap, pw);
        decryptedChartSnapshotsMap = map;
      }
      const ch = map && (map[`${selectedSymbol}_${selectedInterval}`] || map[`${selectedSymbol}_1hour`] || map["BTC_1hour"]);
      if (ch) {
        lastChartData = ch;
        drawPriceChart(ch);
      }
    }
  } catch (err) {
    console.error("Chart fetch error:", err);
  }
}

function updateDashboardUI(data) {
  if (!data || !data.daemon) return;
  const d = data.daemon;
  const eq = data.equity_summary || {};
  const camp = data.campaign || {};

  if (data.network_info) {
    latestNetworkInfo = data.network_info;
  }

  document.getElementById("clockJst").textContent = (data.timestamp || "").slice(11, 19) + " JST";

  const bMode = document.getElementById("badgeMode");
  bMode.textContent = isStaticSnapshotMode ? `${d.execution_mode || "LIVE"} (外出先閲覧)` : (d.execution_mode || "LIVE");
  bMode.className = "badge " + (d.execution_mode === "LIVE" ? "badge-live" : "badge-dry");

  const bDaemon = document.getElementById("badgeDaemon");
  const btnPause = document.getElementById("btnPause");
  const btnResume = document.getElementById("btnResume");
  const btnPauseM = document.getElementById("btnPauseMobile");
  const btnResumeM = document.getElementById("btnResumeMobile");

  if (d.is_paused) {
    bDaemon.textContent = "⏸️ 一時停止中 (PAUSED)";
    bDaemon.className = "badge badge-paused";
    btnPause.disabled = true; btnResume.disabled = false;
    btnPauseM.disabled = true; btnResumeM.disabled = false;
  } else if (d.is_running) {
    bDaemon.textContent = "🟢 稼働中 (ACTIVE)";
    bDaemon.className = "badge badge-active";
    btnPause.disabled = false; btnResume.disabled = true;
    btnPauseM.disabled = false; btnResumeM.disabled = true;
  } else {
    bDaemon.textContent = "⚪ 待機中";
    bDaemon.className = "badge badge-stopped";
    btnPause.disabled = false; btnResume.disabled = false;
    btnPauseM.disabled = false; btnResumeM.disabled = false;
  }

  document.getElementById("badgeRegime").textContent = `${d.market_regime || "NEUTRAL"}`;
  document.getElementById("badgeCampaign").textContent = `🏁 ${camp.countdown_text || ""}`;
  document.getElementById("campaignPhaseBadge").textContent = `${camp.current_phase || "PHASE1_ACTIVE"}`;

  document.getElementById("kpiInitCap").textContent = `初期 ${fmtJpy(eq.initial_capital_jpy)}`;
  document.getElementById("kpiTotalEquity").textContent = fmtJpy(eq.total_equity_jpy);
  document.getElementById("kpiCashDetail").textContent =
    `現金: ${fmtJpy(eq.jpy_balance)} / 拘束: ${fmtJpy(eq.locked_pending_jpy)} / 建玉: ${fmtJpy(eq.positions_eval_jpy)}`;

  const totalPnl = Number(eq.total_pnl_jpy || 0);
  const elPnl = document.getElementById("kpiTotalPnl");
  elPnl.textContent = fmtJpy(totalPnl, true);
  elPnl.className = "kpi-value " + (totalPnl >= 0 ? "pos-val" : "neg-val");
  const pnlPct = Number(eq.total_pnl_pct || 0);
  document.getElementById("kpiTotalPnlPct").textContent = (pnlPct >= 0 ? "+" : "") + pnlPct.toFixed(2) + "%";
  document.getElementById("kpiRealizedDetail").textContent =
    `確定: ${fmtJpy(eq.realized_pnl_total_jpy, true)} / 含み: ${fmtJpy(eq.unrealized_pnl_total_jpy, true)}`;

  const pa = eq.pattern_a || {};
  document.getElementById("kpiMaxAlloc").textContent = `${fmtJpy(pa.max_position_jpy)} / 枠`;
  document.getElementById("kpiRisk1R").textContent =
    `1R許容損失(1.5%): ${fmtJpy(pa.risk_1r_normal_jpy)} (上限2%: ${fmtJpy(pa.risk_1r_stressed_jpy)})`;

  const l5 = (d.circuit_breakers || {}).l5_kill_switch || {};
  document.getElementById("kpiL5Floor").textContent = fmtJpy(eq.l5_kill_switch_jpy);
  document.getElementById("kpiPeakDetail").textContent =
    `最高資産(HWM): ${fmtJpy(eq.peak_equity_jpy)} (余裕 ${fmtJpy(l5.headroom_jpy, true)})`;
  document.getElementById("kpiL5Status").textContent = l5.latched ? "🚨 L5発動" : "🛡️ 正常";

  const l3 = (d.circuit_breakers || {}).l3_daily_loss || {};
  const l4 = (d.circuit_breakers || {}).l4_weekly_streak || {};
  document.getElementById("kpiDailyPnl").textContent =
    `日次: ${fmtJpy(l3.daily_total_pnl_jpy, true)} / 制限 ${fmtJpy(l3.daily_loss_limit_jpy)}`;
  document.getElementById("kpiWeeklyPnl").textContent =
    `週次: ${fmtJpy(l4.weekly_pnl_jpy, true)} (制限 ${fmtJpy(l4.weekly_loss_limit_jpy)}) / 連敗: ${l4.consecutive_losses || 0}回`;
  document.getElementById("kpiCbBadge").textContent = (l3.blocked || l4.blocked || l5.latched) ? "⛔ 制限中" : "🟢 稼働許可";

  renderToolbarChips(data.tickers || {});

  lastEquityCurve = eq.equity_curve || [];
  drawEquityChart(lastEquityCurve);

  const dec = data.latest_decision || {};
  const act = String(dec.action || "HOLD").toUpperCase();
  const elAct = document.getElementById("aiDecAction");
  elAct.textContent = `${act} (${dec.symbol || "NONE"})`;
  elAct.className = "badge " + (act === "BUY" ? "badge-active" : (act === "SELL" ? "badge-live" : "badge-stopped"));
  document.getElementById("aiDecMeta").textContent =
    `判定: ${(dec.timestamp || "--").replace("T", " ")} | 確信度: ${dec.confidence !== undefined ? dec.confidence : "--"}`;
  document.getElementById("aiDecReasoning").textContent = dec.reasoning || "判定履歴なし";

  const actPos = (data.positions && data.positions.active) || [];
  document.getElementById("activePosCount").textContent = `${actPos.length} / ${(pa.max_concurrent_positions || 2)} 枠`;
  const posBody = document.getElementById("activePositionsBody");
  if (actPos.length === 0) {
    posBody.innerHTML = `<tr><td colspan="8" style="text-align:center; color:var(--text-secondary);">現在保有中の有効ポジションはありません</td></tr>`;
  } else {
    posBody.innerHTML = actPos.map(p => {
      const cls = p.unrealized_pnl_jpy >= 0 ? "pos-val" : "neg-val";
      return `<tr>
        <td><b>${p.symbol}</b></td>
        <td>${p.size}</td>
        <td>${fmtPrice(p.entry_price)} 円</td>
        <td>${fmtPrice(p.current_bid)} 円</td>
        <td>${fmtJpy(p.eval_jpy)}</td>
        <td class="${cls}"><b>${fmtJpy(p.unrealized_pnl_jpy, true)} (${p.unrealized_pnl_pct >= 0 ? "+" : ""}${p.unrealized_pnl_pct}%)</b></td>
        <td>SL: <span style="color:#fca5a5;">${fmtPrice(p.stop_loss_price)}</span> / TP: <span style="color:#6ee7b7;">${fmtPrice(p.take_profit_price)}</span></td>
        <td><span class="badge badge-campaign">${p.trailing_stage}</span></td>
      </tr>`;
    }).join("");
  }

  const dustList = (data.positions && data.positions.quarantined_dust) || [];
  const dustBox = document.getElementById("dustQuarantineBox");
  if (dustList.length > 0) {
    dustBox.innerHTML = `📦 最小注文単位未満の隔離端数 (DUST_QUARANTINED — 保有枠・SL対象外): ` +
      dustList.map(d => `${d.symbol}: ${d.size} (約 ${d.estimated_value_jpy} 円)`).join(" / ");
  } else {
    dustBox.textContent = "";
  }

  const pend = data.pending_orders || [];
  document.getElementById("pendingOrdCount").textContent = `${pend.length} 件`;
  const pendBody = document.getElementById("pendingOrdersBody");
  if (pend.length === 0) {
    pendBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:var(--text-secondary);">現在待機中の指値注文はありません</td></tr>`;
  } else {
    pendBody.innerHTML = pend.map(o => `<tr>
      <td><b>${o.symbol}</b></td>
      <td>${fmtPrice(o.limit_price)} 円</td>
      <td>${o.size} (${o.executed_size})</td>
      <td>${fmtJpy(o.reserved_jpy)}</td>
      <td>SL: ${fmtPrice(o.stop_loss_price)} / TP: ${fmtPrice(o.take_profit_price)}</td>
      <td><span class="badge badge-paused">${o.state}</span></td>
    </tr>`).join("");
  }

  const closed = data.closed_trades || [];
  const closedBody = document.getElementById("closedTradesBody");
  if (closed.length === 0) {
    closedBody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:var(--text-secondary);">まだ確定した決済履歴はありません</td></tr>`;
  } else {
    closedBody.innerHTML = closed.slice(0, 12).map(t => {
      const pnl = Number(t.pnl_jpy || 0);
      const cls = pnl >= 0 ? "pos-val" : "neg-val";
      return `<tr>
        <td>${String(t.closed_at || "").replace("T", " ").slice(0, 19)}</td>
        <td><b>${t.symbol || ""}</b></td>
        <td>${t.size || 0}</td>
        <td>${fmtPrice(t.entry_price)} ➔ ${fmtPrice(t.exit_price)}</td>
        <td class="${cls}"><b>${fmtJpy(pnl, true)}</b></td>
        <td>${t.reason || ""}</td>
      </tr>`;
    }).join("");
  }

  const logLines = data.logs || [];
  const logEl = document.getElementById("logViewer");
  logEl.textContent = logLines.length ? logLines.join("\n") : "ログはまだありません";
  logEl.scrollTop = logEl.scrollHeight;
}

function drawPriceChart(chartData) {
  const canvas = document.getElementById("priceChartCanvas");
  if (!canvas || !chartData) return;
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = rect.height;

  ctx.fillStyle = "#090d16";
  ctx.fillRect(0, 0, W, H);

  const candles = chartData.candles || [];
  const t = chartData.ticker || {};
  const ov = chartData.overlays || {};

  document.getElementById("chartTickerSummary").textContent =
    t.last ? `${chartData.symbol}/JPY  ${fmtPrice(t.last)} 円 (Bid:${fmtPrice(t.bid)} / Ask:${fmtPrice(t.ask)})` : `${chartData.symbol}/JPY`;
  document.getElementById("chartUpdatedTime").textContent =
    `更新: ${(chartData.timestamp || "").replace("T", " ").slice(0, 19)}`;

  if (candles.length === 0) {
    ctx.fillStyle = "#94a3b8";
    ctx.font = "13px sans-serif";
    ctx.fillText("ローソク足データがありません", W / 2 - 80, H / 2);
    return;
  }

  const padL = 12, padR = 82, padT = 18, padB = 26;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;

  let minP = Infinity, maxP = -Infinity;
  candles.forEach(c => {
    if (c.low < minP) minP = c.low;
    if (c.high > maxP) maxP = c.high;
  });
  [ov.entry_price, ov.stop_loss_price, ov.take_profit_price, ov.limit_price].forEach(p => {
    if (p && p > 0) {
      if (p < minP) minP = p;
      if (p > maxP) maxP = p;
    }
  });
  if (maxP <= minP) { maxP = minP + 10; minP = Math.max(0, minP - 10); }
  const margin = (maxP - minP) * 0.08;
  minP -= margin;
  maxP += margin;

  const yOf = (price) => padT + plotH - ((price - minP) / (maxP - minP)) * plotH;

  ctx.strokeStyle = "rgba(148,163,184,0.1)";
  ctx.lineWidth = 1;
  ctx.fillStyle = "#64748b";
  ctx.font = "10px monospace";
  for (let i = 0; i <= 4; i++) {
    const pVal = minP + ((maxP - minP) * i) / 4;
    const y = yOf(pVal);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    ctx.fillText(fmtPrice(pVal), padL + plotW + 5, y + 3);
  }

  const n = candles.length;
  const stepX = plotW / Math.max(1, n);
  const bodyW = Math.max(2, Math.min(11, stepX * 0.68));

  candles.forEach((c, idx) => {
    const cx = padL + idx * stepX + stepX / 2;
    const isUp = c.close >= c.open;
    const col = isUp ? "#10b981" : "#ef4444";
    ctx.strokeStyle = col;
    ctx.fillStyle = col;

    ctx.beginPath();
    ctx.moveTo(cx, yOf(c.high));
    ctx.lineTo(cx, yOf(c.low));
    ctx.stroke();

    const yOpen = yOf(c.open);
    const yClose = yOf(c.close);
    const topY = Math.min(yOpen, yClose);
    const bh = Math.max(1.5, Math.abs(yClose - yOpen));
    ctx.fillRect(cx - bodyW / 2, topY, bodyW, bh);

    if (idx === 0 || idx === Math.floor(n / 2) || idx === n - 1) {
      ctx.fillStyle = "#64748b";
      ctx.font = "10px monospace";
      ctx.fillText(c.time_label || "", Math.max(padL, cx - 26), H - 7);
    }
  });

  const drawHorizontalOverlay = (price, color, label) => {
    if (!price || price <= 0) return;
    const y = yOf(price);
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.setLineDash([5, 4]);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    ctx.fillStyle = color;
    ctx.font = "bold 10px monospace";
    ctx.fillText(`${label}: ${fmtPrice(price)}`, padL + 6, Math.max(12, y - 4));
    ctx.restore();
  };

  drawHorizontalOverlay(ov.entry_price, "#38bdf8", "ENTRY");
  drawHorizontalOverlay(ov.take_profit_price, "#10b981", "TP利確");
  drawHorizontalOverlay(ov.stop_loss_price, "#ef4444", "SL逆指値");
  drawHorizontalOverlay(ov.limit_price, "#f59e0b", "待機指値");
}

function drawEquityChart(curve) {
  const canvas = document.getElementById("equityChartCanvas");
  if (!canvas || !curve || curve.length === 0) return;
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = rect.height;

  ctx.fillStyle = "#090d16";
  ctx.fillRect(0, 0, W, H);

  const padL = 12, padR = 70, padT = 16, padB = 20;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;

  let minE = Infinity, maxE = -Infinity;
  curve.forEach(pt => {
    const v = Number(pt.equity_jpy || 0);
    if (v < minE) minE = v;
    if (v > maxE) maxE = v;
  });
  if (maxE <= minE) { maxE = minE + 100; minE = Math.max(0, minE - 100); }
  const margin = (maxE - minE) * 0.15;
  minE -= margin;
  maxE += margin;

  const yOf = (val) => padT + plotH - ((val - minE) / (maxE - minE)) * plotH;
  const xOf = (idx) => padL + (curve.length <= 1 ? plotW / 2 : (idx / (curve.length - 1)) * plotW);

  ctx.beginPath();
  curve.forEach((pt, idx) => {
    const x = xOf(idx), y = yOf(pt.equity_jpy);
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#38bdf8";
  ctx.lineWidth = 2.2;
  ctx.stroke();

  curve.forEach((pt, idx) => {
    const x = xOf(idx), y = yOf(pt.equity_jpy);
    ctx.fillStyle = "#38bdf8";
    ctx.beginPath();
    ctx.arc(x, y, 3.2, 0, Math.PI * 2);
    ctx.fill();
    if (idx === 0 || idx === curve.length - 1) {
      ctx.fillStyle = "#cbd5e1";
      ctx.font = "10px monospace";
      ctx.fillText(fmtJpy(pt.equity_jpy), padL + plotW + 5, y + 4);
    }
  });
}

const READONLY_PWA_GUIDE_MSG = "📱 外出先（GitHub Pages閲覧専用モード）です。操作はPCダッシュボード（127.0.0.1:8080）から実行してください";

async function sendControl(command, nonce="") {
  if (isStaticSnapshotMode) {
    showToast(READONLY_PWA_GUIDE_MSG, true);
    return null;
  }
  try {
    const res = await fetch(`${activeApiBase}/api/control`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command, nonce })
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      showToast(`エラー: ${data.error || "コマンド実行失敗"}`, true);
      return null;
    }
    showToast(data.message || `コマンド ${command} を実行しました`);
    await fetchStatus();
    return data;
  } catch (err) {
    showToast(`通信エラー: ${err}`, true);
    return null;
  }
}

function confirmCancelEntries() {
  if (isStaticSnapshotMode) {
    showToast(READONLY_PWA_GUIDE_MSG, true);
    return;
  }
  if (!confirm("現在待機中の未約定買い指値（PENDING_BUY）をすべてキャンセルしますか？\n（※保有中ポジションと逆指値STOP注文は維持されます）")) {
    return;
  }
  sendControl("CANCEL_ENTRIES");
}

async function openFlattenModal() {
  if (isStaticSnapshotMode) {
    showToast(READONLY_PWA_GUIDE_MSG, true);
    return;
  }
  const modal = document.getElementById("flattenModal");
  const box = document.getElementById("flattenNonceDisplay");
  const btn = document.getElementById("btnConfirmFlattenNow");
  modal.style.display = "flex";
  box.textContent = "ワンタイム承認コードを発行中...";
  btn.disabled = true;
  currentFlattenNonce = "";

  const data = await sendControl("FLATTEN_ALL");
  if (data && data.result && data.result.nonce) {
    currentFlattenNonce = data.result.nonce;
    box.textContent = `承認コード: ${currentFlattenNonce}\n(有効期限: ${(data.result.expires_at || "").replace("T", " ")})`;
    btn.disabled = false;
  } else {
    box.textContent = "承認コードの発行に失敗しました";
  }
}

function closeFlattenModal() {
  document.getElementById("flattenModal").style.display = "none";
  currentFlattenNonce = "";
}

async function confirmFlattenAll() {
  if (!currentFlattenNonce) return;
  const nonce = currentFlattenNonce;
  closeFlattenModal();
  await sendControl("FLATTEN_CONFIRM", nonce);
}

async function openQrModal() {
  document.getElementById("qrModal").style.display = "flex";
  const ghUrl = (latestNetworkInfo && latestNetworkInfo.github_pages_url) || "https://qanojisan.github.io/gmo-ai-trader-auto/";
  const ghLink = document.getElementById("qrGhLink");
  if (ghLink) {
    ghLink.href = ghUrl;
    ghLink.textContent = ghUrl;
  }

  try {
    let r2 = null;
    if (!isStaticSnapshotMode) {
      r2 = await fetchWithTimeout(`${activeApiBase}/api/qr?text=${encodeURIComponent(ghUrl)}`, {}, 2000).catch(() => null);
    }
    if (r2 && r2.ok) {
      const t2 = await r2.text();
      if (t2.includes("<svg")) {
        document.getElementById("qrGhHolder").innerHTML = t2;
        return;
      }
    }
    const f2 = await fetch("./qr-gh.svg");
    if (f2.ok) document.getElementById("qrGhHolder").innerHTML = await f2.text();
  } catch (_) {}
}

function closeQrModal() {
  document.getElementById("qrModal").style.display = "none";
}

async function forceRefreshPwa() {
  try {
    if ('serviceWorker' in navigator && navigator.serviceWorker.controller) {
      navigator.serviceWorker.controller.postMessage({ type: 'CLEAR_CACHE' });
    }
    if ('caches' in window) {
      const keys = await caches.keys();
      await Promise.all(keys.map(k => caches.delete(k)));
    }
  } catch (_) {}
  await Promise.all([fetchStatus(), fetchChart()]);
  showToast("🔄 キャッシュをクリアし、最新データを取得しました");
}

window.addEventListener("resize", () => {
  if (lastChartData) drawPriceChart(lastChartData);
  if (lastEquityCurve) drawEquityChart(lastEquityCurve);
});

fetchStatus();
fetchChart();
setInterval(() => {
  fetchStatus();
  fetchChart();
}, 15000);
</script>
</body>
</html>
"""


def make_dashboard_handler(service: WebDashboardService):
    """`WebDashboardService` をバインドした HTTP リクエストハンドラクラスを生成する"""

    class DashboardRequestHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            logger.debug("HTTP %s - %s", self.address_string(), format % args)

        def _send_cors_headers(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept")
            self.send_header("Access-Control-Allow-Private-Network", "true")

        def _send_bytes(self, status_code: int, content_type: str, data: bytes, cache_control: str = "no-store") -> None:
            self.send_response(status_code)
            self._send_cors_headers()
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", cache_control)
            self.end_headers()
            self.wfile.write(data)

        def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
            raw = dumps_json(payload, indent=None).encode("utf-8")
            self._send_bytes(status_code, "application/json; charset=utf-8", raw)

        def do_OPTIONS(self) -> None:
            self.send_response(HTTPStatus.NO_CONTENT)
            self._send_cors_headers()
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            qs = parse_qs(parsed.query)

            if path in ("/", "/index.html"):
                self._send_bytes(HTTPStatus.OK, "text/html; charset=utf-8", DASHBOARD_HTML.encode("utf-8"))
                return

            if path == "/manifest.json":
                manifest = build_pwa_manifest(start_url="./index.html")
                self._send_bytes(
                    HTTPStatus.OK,
                    "application/manifest+json; charset=utf-8",
                    dumps_json(manifest, indent=2).encode("utf-8"),
                )
                return

            if path == "/sw.js":
                self._send_bytes(
                    HTTPStatus.OK,
                    "application/javascript; charset=utf-8",
                    SERVICE_WORKER_JS.encode("utf-8"),
                )
                return

            if path == "/icon-192.png":
                self._send_bytes(HTTPStatus.OK, "image/png", generate_app_icon_png(192), cache_control="public, max-age=3600")
                return

            if path == "/icon-512.png":
                self._send_bytes(HTTPStatus.OK, "image/png", generate_app_icon_png(512), cache_control="public, max-age=3600")
                return

            if path == "/icon.svg":
                self._send_bytes(
                    HTTPStatus.OK,
                    "image/svg+xml; charset=utf-8",
                    generate_app_icon_svg().encode("utf-8"),
                    cache_control="public, max-age=3600",
                )
                return

            if path == "/qr-lan.svg":
                svg = generate_qr_svg(f"http://{service.lan_ip}:{service.port}")
                self._send_bytes(HTTPStatus.OK, "image/svg+xml; charset=utf-8", svg.encode("utf-8"))
                return

            if path == "/qr-gh.svg":
                svg = generate_qr_svg(service.github_pages_url)
                self._send_bytes(HTTPStatus.OK, "image/svg+xml; charset=utf-8", svg.encode("utf-8"))
                return

            if path == "/api/qr":
                txt = (qs.get("text", [f"http://{service.lan_ip}:{service.port}"])[0] or "").strip()
                svg = generate_qr_svg(txt)
                self._send_bytes(HTTPStatus.OK, "image/svg+xml; charset=utf-8", svg.encode("utf-8"))
                return

            if path == "/api/status":
                try:
                    data = service.get_status_payload()
                    data["pwa_unlock_password"] = service.get_pwa_unlock_password()
                    self._send_json(HTTPStatus.OK, data)
                except Exception as exc:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
                return

            if path == "/status_snapshot.json":
                try:
                    snap_file = service.docs_dir / "status_snapshot.json"
                    if snap_file.exists():
                        self._send_bytes(HTTPStatus.OK, "application/json; charset=utf-8", snap_file.read_bytes())
                    else:
                        data = service.get_status_payload()
                        data["static_snapshot"] = True
                        enc = encrypt_json_envelope(data, service.get_pwa_unlock_password())
                        self._send_json(HTTPStatus.OK, enc)
                except Exception as exc:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
                return

            if path == "/chart_snapshots.json":
                try:
                    c_file = service.docs_dir / "chart_snapshots.json"
                    if c_file.exists():
                        self._send_bytes(HTTPStatus.OK, "application/json; charset=utf-8", c_file.read_bytes())
                    else:
                        enc = encrypt_json_envelope({}, service.get_pwa_unlock_password())
                        self._send_json(HTTPStatus.OK, enc)
                except Exception as exc:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
                return

            if path == "/api/chart":
                sym = (qs.get("symbol", ["BTC"])[0] or "BTC").strip().upper()
                iv = (qs.get("interval", ["1hour"])[0] or "1hour").strip().lower()
                try:
                    data = service.get_chart_payload(symbol=sym, interval=iv)
                    self._send_json(HTTPStatus.OK, data)
                except Exception as exc:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Not found: {path}"})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"

            if path == "/api/control":
                try:
                    length = int(self.headers.get("Content-Length", "0") or 0)
                    raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
                    body = json.loads(raw_body) if raw_body.strip() else {}
                    cmd = str(body.get("command", "")).strip().upper()
                    nonce = str(body.get("nonce", "")).strip()
                    res = service.handle_control_command(command=cmd, nonce=nonce)
                    self._send_json(HTTPStatus.OK, res)
                except (ValueError, ReplayOrExpiredNonceError, UnauthorizedCommandError) as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                except Exception as exc:
                    self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Not found: {path}"})

    return DashboardRequestHandler


def create_dashboard_http_server(
    host: str = "127.0.0.1",
    port: int = 8080,
    service: Optional[WebDashboardService] = None,
) -> Tuple[ThreadingHTTPServer, WebDashboardService]:
    svc = service or WebDashboardService(port=int(port))
    svc.port = int(port)
    handler_cls = make_dashboard_handler(svc)
    server = ThreadingHTTPServer((host, int(port)), handler_cls)
    return server, svc


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="GMO AI Trader v2 PWA対応Webダッシュボード")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="バインドホスト (既定: 127.0.0.1 — ローカルループバック専用)")
    parser.add_argument("--port", type=int, default=8080, help="バインドポート (既定: 8080)")
    parser.add_argument("--open-browser", action="store_true", help="起動時に既定のブラウザでダッシュボードを開く")
    parser.add_argument("--export-static", action="store_true", help="docs/ へ静的PWA一式と最新スナップショットを出力して終了する")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    svc = WebDashboardService(port=args.port)
    try:
        svc.export_static_pwa_bundle(include_charts=True)
    except Exception as exp_exc:
        logger.warning("初回PWAスナップショット出力警告: %s", exp_exc)

    if args.export_static:
        logger.info("📄 docs/ への静的PWAスナップショット出力完了")
        svc.close()
        return 0

    server, _ = create_dashboard_http_server(host=args.host, port=args.port, service=svc)
    actual_port = server.server_address[1]
    local_url = f"http://127.0.0.1:{actual_port}"
    logger.info("🌐 GMO AI Trader v2 PWA Webダッシュボード起動 (bind=%s:%d): PC=%s", args.host, actual_port, local_url)

    if args.open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(local_url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Webダッシュボードを停止します...")
    finally:
        server.server_close()
        svc.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
