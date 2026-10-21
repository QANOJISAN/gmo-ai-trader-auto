"""
JSON 安全入出力ユーティリティ (json_utils.py)

- `sanitize_json_value()`: 辞書・リストを再帰的に走査し、NaN / Infinity などの非有限 float や
  numpy 型を JSON 互換の値へ置換する（ブラウザ側 `JSON.parse` が `NaN` トークンで壊れるのを防止）。
- `dumps_json()`: サニタイズ後に `allow_nan=False` でシリアライズし、さらに `1e-05` のような
  指数表記の数値を `0.00001` の通常小数表記へ展開する（文字列リテラル内は変更しない）。
- `load_json_file()`: BOM 付き UTF-8（Windows PowerShell 等で保存されたファイル）も読めるよう
  `utf-8-sig` で読み込む（書き込みは従来通り BOM なし UTF-8）。
"""

from __future__ import annotations

import json
import math
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

# JSON 文字列リテラル、または指数表記の数値トークンにマッチ（文字列を先に消費して誤置換を防ぐ）
_JSON_TOKEN_RE = re.compile(r'"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?[eE][+-]?\d+')


def sanitize_json_value(obj: Any, default: float = 0.0) -> Any:
    """非有限 float（NaN / Infinity）や numpy 型を JSON 互換の安全な値へ再帰的に置換する"""
    if obj is None or isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else default
    if isinstance(obj, int):
        return obj
    if isinstance(obj, Decimal):
        if not obj.is_finite():
            return default
        as_float = float(obj)
        return as_float if math.isfinite(as_float) else default
    if isinstance(obj, dict):
        return {k: sanitize_json_value(v, default) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [sanitize_json_value(v, default) for v in obj]
    # numpy のスカラー型（np.float64 は float のサブクラスのため上で処理済み）
    item = getattr(obj, "item", None)
    if callable(item) and type(obj).__module__.startswith("numpy"):
        try:
            return sanitize_json_value(item(), default)
        except Exception:
            return default
    return obj


def _expand_exponent_token(match: "re.Match[str]") -> str:
    token = match.group(0)
    if token.startswith('"'):
        return token
    try:
        # L-1: 0.00001 + 0.00002 -> 3.0000000000000004e-05 のような IEEE 754 浮動小数点ノイズを除去して展開
        f_val = float(token)
        if math.isfinite(f_val):
            cleaned = Decimal(f"{f_val:.12g}").normalize()
        else:
            cleaned = Decimal(token).normalize()
        if cleaned == cleaned.to_integral():
            return str(int(cleaned))
        return format(cleaned, "f")
    except (InvalidOperation, ValueError, OverflowError):
        return token


def dumps_json(obj: Any, indent: int | None = 2) -> str:
    """NaN / Infinity を含まない（allow_nan=False 保証）・指数表記なしの JSON 文字列を返す"""
    text = json.dumps(sanitize_json_value(obj), ensure_ascii=False, indent=indent, allow_nan=False)
    return _JSON_TOKEN_RE.sub(_expand_exponent_token, text)


def write_json_file(path: Path, obj: Any, indent: int | None = 2) -> None:
    """BOM なし UTF-8 で安全に JSON を書き込む"""
    with Path(path).open("w", encoding="utf-8") as f:
        f.write(dumps_json(obj, indent=indent))


def load_json_file(path: Path) -> Any:
    """BOM 付き UTF-8 にも耐性のある JSON 読み込み"""
    with Path(path).open("r", encoding="utf-8-sig") as f:
        return json.load(f)
