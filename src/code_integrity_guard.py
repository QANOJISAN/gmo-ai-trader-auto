"""
DPAPI 鍵付き HMAC-SHA256 実行コード改ざん検知 ＆ 外部通信先（Egress）厳格ホワイトリスト (code_integrity_guard.py)

【Round 20：対AIクラッキング・ゼロトラスト要塞化（実装4）】
1. 実行コード改ざん検知 (`update_signed_integrity_manifest` / `verify_signed_integrity_manifest` / `CodeIntegrityGuard`):
   - `e:\\gmo-ai-trader-auto` 内の全コアファイル（`auto_trader.py`, `web_dashboard.py`, `generate_report.py`,
     `src/*.py`, `launch_hidden.vbs`, `run_v2_daemon.ps1`, `run_v2_web.ps1`）の SHA-256 ハッシュを計算し、
     Windows DPAPI (`Crypt32.dll`) で保護されたマシン固有の署名鍵と `ephemeral_unprotected_bytes` (JIT復号 + 0x00 Zeroize)
     を用いて HMAC-SHA256 署名したマニフェスト (`v2_code_integrity.manifest.json`) を生成・検証する。
   - 万一マルウェアやクラッキングによって `.py` や `.ps1`・`.vbs` が1バイトでも不正に書き換えられていた場合、
     または署名不一致が検出された場合は `CODE_INTEGRITY_VIOLATION` として新規発注を即時ハードロックする。
2. 外部通信先（Egress）ドメイン厳格ホワイトリスト (`validate_egress_url` / `EgressSecurityViolationError`):
   - システムが外部へ HTTP / WebSocket 通信を行う際、接続先 URL のホスト名が許可リスト
     (`api.coin.z.com`, `ws-api.coin.z.com`, `discord.com`, `discordapp.com`,
      `generativelanguage.googleapis.com`, `api.openai.com`, `github.com`, `127.0.0.1`, `localhost`)
     以外の不審な外部サーバーである場合は、リクエスト送信前に `EgressSecurityViolationError` で物理遮断する。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union
from urllib.parse import urlparse

from src.dpapi_vault import (
    DPAPI_PREFIX,
    ephemeral_unprotected_bytes,
    is_dpapi_protected,
    protect_secret,
)

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

DEFAULT_BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST_FILENAME = "v2_code_integrity.manifest.json"

CORE_ROOT_FILES: tuple[str, ...] = (
    "auto_trader.py",
    "web_dashboard.py",
    "generate_report.py",
    "launch_hidden.vbs",
    "run_v2_daemon.ps1",
    "run_v2_web.ps1",
)

ALLOWED_EGRESS_HOSTS: frozenset[str] = frozenset(
    {
        "api.coin.z.com",
        "ws-api.coin.z.com",
        "discord.com",
        "discordapp.com",
        "ptb.discord.com",
        "canary.discord.com",
        "ptb.discordapp.com",
        "canary.discordapp.com",
        "generativelanguage.googleapis.com",
        "api.groq.com",
        "api.openai.com",
        "github.com",
        "127.0.0.1",
        "localhost",
        "::1",
    }
)


class EgressSecurityViolationError(RuntimeError):
    """未承認の外部ドメイン（Egress ホワイトリスト外）への通信試行を物理遮断する例外"""


class CodeIntegrityViolationError(RuntimeError):
    """実行コードまたはスクリプトの改ざん検知 (`CODE_INTEGRITY_VIOLATION`) 例外"""


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def validate_egress_url(
    url: str,
    allowed_hosts: Optional[Iterable[str]] = None,
) -> str:
    """
    外部通信先 URL のスキームおよびホスト名を厳格ホワイトリストで検証する。
    許可リスト外のホストや不正スキームが指定された場合は、通信実行前に
    `EgressSecurityViolationError` を送出して物理遮断する。
    """
    cleaned = str(url or "").strip()
    if not cleaned:
        raise EgressSecurityViolationError("EGRESS_URL_EMPTY: 通信先 URL が空です。")

    parsed = urlparse(cleaned)
    scheme = (parsed.scheme or "").strip().lower()
    host = (parsed.hostname or "").strip().lower()

    if scheme not in ("https", "http", "wss", "ws"):
        raise EgressSecurityViolationError(
            f"EGRESS_SCHEME_BLOCKED: 未許可のスキーム '{scheme}' (URL={cleaned}) を物理遮断しました。"
        )

    whitelist = {str(h).strip().lower() for h in (allowed_hosts or ALLOWED_EGRESS_HOSTS) if h}
    if not host or host not in whitelist:
        raise EgressSecurityViolationError(
            f"EGRESS_DOMAIN_BLOCKED: 未承認の外部通信先ホスト '{host}' (URL={cleaned}) への通信を物理遮断しました。"
        )

    if host not in ("127.0.0.1", "localhost", "::1") and scheme not in ("https", "wss"):
        raise EgressSecurityViolationError(
            f"EGRESS_INSECURE_SCHEME_BLOCKED: 外部ホスト '{host}' への非暗号化通信 '{scheme}' を物理遮断しました。"
        )

    return cleaned


def collect_core_file_hashes(base_dir: Optional[Union[str, Path]] = None) -> Dict[str, str]:
    """
    `base_dir` 配下の全コアファイル（`auto_trader.py`, `web_dashboard.py`, `generate_report.py`,
    `src/*.py`, `launch_hidden.vbs`, `run_v2_daemon.ps1`, `run_v2_web.ps1`）の SHA-256 ハッシュ辞書を返す。
    """
    root = Path(base_dir) if base_dir is not None else DEFAULT_BASE_DIR
    hashes: Dict[str, str] = {}

    for rel_name in CORE_ROOT_FILES:
        fpath = root / rel_name
        if fpath.exists() and fpath.is_file():
            hashes[rel_name] = hashlib.sha256(fpath.read_bytes()).hexdigest()

    src_dir = root / "src"
    if src_dir.exists() and src_dir.is_dir():
        for py_file in sorted(src_dir.glob("*.py"), key=lambda p: p.name):
            if py_file.is_file():
                rel_posix = f"src/{py_file.name}"
                hashes[rel_posix] = hashlib.sha256(py_file.read_bytes()).hexdigest()

    return dict(sorted(hashes.items(), key=lambda kv: kv[0]))


def _canonicalize_hashes_for_hmac(file_hashes: Dict[str, str]) -> str:
    ordered = {str(k): str(v) for k, v in sorted(file_hashes.items(), key=lambda kv: str(kv[0]))}
    return json.dumps(ordered, separators=(",", ":"), ensure_ascii=True, sort_keys=True)


def _compute_manifest_hmac(dpapi_key: str, file_hashes: Dict[str, str]) -> str:
    canonical = _canonicalize_hashes_for_hmac(file_hashes)
    with ephemeral_unprotected_bytes(dpapi_key) as key_buf:
        if len(key_buf) == 0:
            raise CodeIntegrityViolationError("DPAPI 署名鍵の復号結果が空です。")
        return hmac.new(key_buf, canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def update_signed_integrity_manifest(
    base_dir: Optional[Union[str, Path]] = None,
    manifest_path: Optional[Union[str, Path]] = None,
    now_dt: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    全コアファイルの SHA-256 ハッシュを再計算し、DPAPI 保護されたマシン固有鍵による
    HMAC-SHA256 署名付きマニフェスト (`v2_code_integrity.manifest.json`) を生成・保存する。
    """
    root = Path(base_dir) if base_dir is not None else DEFAULT_BASE_DIR
    m_path = Path(manifest_path) if manifest_path is not None else (root / DEFAULT_MANIFEST_FILENAME)
    now_jst = _now_jst(now_dt)

    dpapi_hmac_key = ""
    if m_path.exists():
        try:
            existing = json.loads(m_path.read_text(encoding="utf-8"))
            cand_key = str(existing.get("dpapi_hmac_key", "")).strip()
            if is_dpapi_protected(cand_key) and cand_key.startswith("DPAPI:AQAA"):
                dpapi_hmac_key = cand_key
        except Exception:
            dpapi_hmac_key = ""

    if not dpapi_hmac_key:
        raw_random_key = f"gmo-v2-integrity-hmac-key-{secrets.token_hex(32)}"
        dpapi_hmac_key = protect_secret(raw_random_key)

    file_hashes = collect_core_file_hashes(root)
    signature = _compute_manifest_hmac(dpapi_hmac_key, file_hashes)

    manifest_data: Dict[str, Any] = {
        "schema_version": "2.0-round20-dpapi-hmac",
        "algorithm": "HMAC-SHA256",
        "updated_at": now_jst.isoformat(timespec="seconds"),
        "file_count": len(file_hashes),
        "dpapi_hmac_key": dpapi_hmac_key,
        "files": file_hashes,
        "hmac_sha256": signature,
    }

    tmp_path = m_path.with_suffix(".manifest.tmp")
    tmp_path.write_text(json.dumps(manifest_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(m_path)
    return manifest_data


def verify_signed_integrity_manifest(
    base_dir: Optional[Union[str, Path]] = None,
    manifest_path: Optional[Union[str, Path]] = None,
    auto_initialize_if_missing: bool = False,
    now_dt: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    署名済みマニフェスト (`v2_code_integrity.manifest.json`) を検証する。
    - ファイルの SHA-256 不一致（改ざん）、必須ファイルの欠落、未知の `src/*.py` 注入、
      または DPAPI 鍵による HMAC-SHA256 署名不一致が検出された場合は
      `ok=False`, `status="CODE_INTEGRITY_VIOLATION"` を返す。
    """
    root = Path(base_dir) if base_dir is not None else DEFAULT_BASE_DIR
    m_path = Path(manifest_path) if manifest_path is not None else (root / DEFAULT_MANIFEST_FILENAME)
    now_jst = _now_jst(now_dt)

    if not m_path.exists():
        if auto_initialize_if_missing:
            update_signed_integrity_manifest(base_dir=root, manifest_path=m_path, now_dt=now_jst)
        else:
            return {
                "ok": False,
                "status": "CODE_INTEGRITY_VIOLATION",
                "reason": "MANIFEST_MISSING",
                "manifest_path": str(m_path),
                "checked_file_count": 0,
                "tampered_files": [],
                "missing_files": [],
                "unexpected_files": [],
                "hmac_valid": False,
                "verified_at": now_jst.isoformat(timespec="seconds"),
            }

    try:
        manifest = json.loads(m_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "ok": False,
            "status": "CODE_INTEGRITY_VIOLATION",
            "reason": f"MANIFEST_CORRUPTED: {exc}",
            "manifest_path": str(m_path),
            "checked_file_count": 0,
            "tampered_files": [],
            "missing_files": [],
            "unexpected_files": [],
            "hmac_valid": False,
            "verified_at": now_jst.isoformat(timespec="seconds"),
        }

    dpapi_key = str(manifest.get("dpapi_hmac_key", "")).strip()
    expected_sig = str(manifest.get("hmac_sha256", "")).strip()
    recorded_files: Dict[str, str] = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}

    if not dpapi_key.startswith(f"{DPAPI_PREFIX}AQAA") or not expected_sig or not recorded_files:
        return {
            "ok": False,
            "status": "CODE_INTEGRITY_VIOLATION",
            "reason": "MANIFEST_KEY_OR_SIGNATURE_INVALID",
            "manifest_path": str(m_path),
            "checked_file_count": len(recorded_files),
            "tampered_files": [],
            "missing_files": [],
            "unexpected_files": [],
            "hmac_valid": False,
            "verified_at": now_jst.isoformat(timespec="seconds"),
        }

    hmac_valid = False
    try:
        computed_sig_on_manifest = _compute_manifest_hmac(dpapi_key, recorded_files)
        hmac_valid = hmac.compare_digest(computed_sig_on_manifest, expected_sig)
    except Exception as exc:
        logger.error("コード整合性マニフェストの HMAC 検証失敗: %s", exc)
        hmac_valid = False

    current_files = collect_core_file_hashes(root)
    tampered_files: List[str] = []
    missing_files: List[str] = []
    unexpected_files: List[str] = []

    for rel_path, exp_hash in recorded_files.items():
        if rel_path not in current_files:
            missing_files.append(rel_path)
        elif not hmac.compare_digest(str(current_files[rel_path]), str(exp_hash)):
            tampered_files.append(rel_path)

    for rel_path in current_files:
        if rel_path not in recorded_files:
            unexpected_files.append(rel_path)

    ok = bool(hmac_valid and not tampered_files and not missing_files and not unexpected_files)
    status = "VERIFIED_OK" if ok else "CODE_INTEGRITY_VIOLATION"
    reason = "OK"
    if not ok:
        reasons = []
        if not hmac_valid:
            reasons.append("HMAC_SIGNATURE_MISMATCH")
        if tampered_files:
            reasons.append(f"TAMPERED:{','.join(tampered_files)}")
        if missing_files:
            reasons.append(f"MISSING:{','.join(missing_files)}")
        if unexpected_files:
            reasons.append(f"UNEXPECTED:{','.join(unexpected_files)}")
        reason = " | ".join(reasons)

    return {
        "ok": ok,
        "status": status,
        "reason": reason,
        "manifest_path": str(m_path),
        "checked_file_count": len(recorded_files),
        "tampered_files": tampered_files,
        "missing_files": missing_files,
        "unexpected_files": unexpected_files,
        "hmac_valid": hmac_valid,
        "verified_at": now_jst.isoformat(timespec="seconds"),
    }


class CodeIntegrityGuard:
    """
    起動時および定時サイクルで DPAPI 鍵付き HMAC-SHA256 コード整合性を検証し、
    改ざん検知時に `CODE_INTEGRITY_VIOLATION` で新規発注を即時ロックするガードクラス。
    """

    def __init__(
        self,
        base_dir: Optional[Union[str, Path]] = None,
        manifest_path: Optional[Union[str, Path]] = None,
        remote_control: Any = None,
        notifier: Any = None,
        dry_run_notify: bool = False,
        auto_initialize_if_missing: bool = True,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else DEFAULT_BASE_DIR
        self.manifest_path = (
            Path(manifest_path)
            if manifest_path is not None
            else (self.base_dir / DEFAULT_MANIFEST_FILENAME)
        )
        self.remote_control = remote_control
        self.notifier = notifier
        self.dry_run_notify = bool(dry_run_notify)
        self.auto_initialize_if_missing = bool(auto_initialize_if_missing)

        self.violation_latched: bool = False
        self.last_result: Optional[Dict[str, Any]] = None

    def check_and_enforce(self, now_dt: Optional[datetime] = None) -> Dict[str, Any]:
        res = verify_signed_integrity_manifest(
            base_dir=self.base_dir,
            manifest_path=self.manifest_path,
            auto_initialize_if_missing=self.auto_initialize_if_missing,
            now_dt=now_dt,
        )
        self.last_result = res
        if not res.get("ok", False):
            self.violation_latched = True
            if self.remote_control is not None:
                try:
                    self.remote_control.is_paused = True
                except Exception:
                    pass
            if self.notifier is not None:
                try:
                    self.notifier.notify_event(
                        title="🚨 CODE_INTEGRITY_VIOLATION: 実行コード改ざん検知・緊急ロック",
                        message=(
                            f"・ステータス: `{res.get('status')}`\n"
                            f"・検知理由: `{res.get('reason')}`\n"
                            f"・改ざん検出ファイル: `{res.get('tampered_files')}`\n"
                            f"・新規発注を即時ハードロックしました（既存建玉のサーバーSTOP注文は維持）。"
                        ),
                        level="CRITICAL",
                        dry_run=self.dry_run_notify,
                    )
                except Exception:
                    pass
        return res
