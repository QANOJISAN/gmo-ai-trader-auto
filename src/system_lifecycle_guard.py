"""
Win32 OSシャットダウン・再起動・UPS電源喪失グレースフル停止 ＆ 非表示サブプロセス実行ガード (system_lifecycle_guard.py)

【主要機能】
1. コンソールポップアップ完全根絶ヘルパー (`build_hidden_subprocess_kwargs`):
   - Windows環境 (`os.name == "nt"`) におけるすべての `subprocess` 実行に対し、
     `CREATE_NO_WINDOW = 0x08000000` および `STARTUPINFO` (`STARTF_USESHOWWINDOW` + `SW_HIDE = 0`) を強制適用。
   - Git操作時は `GIT_TERMINAL_PROMPT=0` と `GCM_INTERACTIVE=never` を標準付与し、対話プロンプトによるハングを防止。
2. Win32 `SYSTEM_POWER_STATUS` 監視 (`GetSystemPowerStatus`):
   - `ctypes.c_ubyte`（符号なし1バイト整数）で構造体を厳密定義。
   - デスクトップPCのAC常時接続（`ACLineStatus == 1` または `BatteryLifePercent == 255`）を誤検知せず、
     UPSバッテリー駆動移行かつ残量低下（`ACLineStatus == 0` かつ `0 <= BatteryLifePercent <= 40`）のみを正確に検知。
3. Win32 シグナル捕捉 (`SetConsoleCtrlHandler`) ＆ 2段階グレースフル停止シーケンス:
   - `CTRL_CLOSE_EVENT (2)`, `CTRL_LOGOFF_EVENT (5)`, `CTRL_SHUTDOWN_EVENT (6)` または UPS低バッテリー検知時に：
     ① 新規エントリーを即時ロックし、未約定の待機買い指値（`PENDING_BUY`）のみを全件キャンセル。
        ※ 保有ポジションを守っている取引所サーバー上の逆指値（`STOP` 注文）は絶対にキャンセルせず維持する。
     ② `DatabaseWAL.checkpoint_truncate()`（`PRAGMA wal_checkpoint(TRUNCATE);`）を実行し、
        `.db-wal` の未反映データをメインDBファイルへ完全フラッシュしてから安全に休止・終了する。
"""

from __future__ import annotations

import atexit
import ctypes
from ctypes import wintypes
import logging
import os
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# Windows サブプロセス非表示フラグ定数
CREATE_NO_WINDOW: int = 0x08000000
STARTF_USESHOWWINDOW: int = 0x00000001
SW_HIDE: int = 0

# Win32 Console Control Event 定数
CTRL_C_EVENT: int = 0
CTRL_BREAK_EVENT: int = 1
CTRL_CLOSE_EVENT: int = 2
CTRL_LOGOFF_EVENT: int = 5
CTRL_SHUTDOWN_EVENT: int = 6

# Windows セッション終了メッセージ定数（識別名マッピング用）
WM_QUERYENDSESSION: int = 0x0011
WM_ENDSESSION: int = 0x0016

CTRL_EVENT_NAMES: Dict[int, str] = {
    CTRL_C_EVENT: "CTRL_C_EVENT",
    CTRL_BREAK_EVENT: "CTRL_BREAK_EVENT",
    CTRL_CLOSE_EVENT: "CTRL_CLOSE_EVENT",
    CTRL_LOGOFF_EVENT: "CTRL_LOGOFF_EVENT",
    CTRL_SHUTDOWN_EVENT: "CTRL_SHUTDOWN_EVENT",
    WM_QUERYENDSESSION: "WM_QUERYENDSESSION",
    WM_ENDSESSION: "WM_ENDSESSION",
}


def _now_jst(now_dt: Optional[datetime] = None) -> datetime:
    if now_dt is None:
        return datetime.now(JST)
    if now_dt.tzinfo is None:
        return now_dt.replace(tzinfo=JST)
    return now_dt.astimezone(JST)


def build_hidden_subprocess_kwargs(
    extra_env: Optional[Dict[str, str]] = None,
    is_git: bool = False,
) -> Dict[str, Any]:
    """
    Windows環境においてコンソールウィンドウ（cmd.exe / powershell.exe / git.exe）が
    一瞬たりともポップアップしないための `subprocess.run` / `Popen` 用キーワード引数を生成する。
    """
    kwargs: Dict[str, Any] = {}
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", CREATE_NO_WINDOW)
        if hasattr(subprocess, "STARTUPINFO"):
            si = subprocess.STARTUPINFO()
            si.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", STARTF_USESHOWWINDOW)
            si.wShowWindow = getattr(subprocess, "SW_HIDE", SW_HIDE)
            kwargs["startupinfo"] = si

    if is_git or extra_env is not None:
        env = os.environ.copy()
        if is_git:
            env["GIT_TERMINAL_PROMPT"] = "0"
            env["GCM_INTERACTIVE"] = "never"
        if extra_env:
            env.update(extra_env)
        kwargs["env"] = env

    return kwargs


class SYSTEM_POWER_STATUS(ctypes.Structure):
    """
    Win32 API `GetSystemPowerStatus` 用の構造体定義。
    1バイトフィールドは必ず符号なし1バイト整数 `ctypes.c_ubyte` で定義し、
    `255 (0xFF)` が `-1` に符号拡張されることを防ぐ。
    """

    _fields_ = [
        ("ACLineStatus", ctypes.c_ubyte),
        ("BatteryFlag", ctypes.c_ubyte),
        ("BatteryLifePercent", ctypes.c_ubyte),
        ("SystemStatusFlag", ctypes.c_ubyte),
        ("BatteryLifeTime", ctypes.c_ulong),
        ("BatteryFullLifeTime", ctypes.c_ulong),
    ]


def get_system_power_status(
    simulated_status: Optional[Dict[str, int]] = None,
    low_battery_threshold_pct: int = 40,
) -> Dict[str, Any]:
    """
    現在のOS電源・UPSバッテリー状態を取得し、緊急グレースフル停止が必要か判定する。

    【判定条件】
    - `ACLineStatus == 0`（商用電源喪失・UPS/バッテリー駆動中）
      かつ `0 <= BatteryLifePercent <= 40` の場合のみ `should_emergency_graceful_stop = True`。
    - `ACLineStatus == 1`（AC常時接続）または `BatteryLifePercent == 255`（バッテリー非搭載デスクトップPC・不明）
      の場合は絶対に誤発動しない（`should_emergency_graceful_stop = False`）。
    """
    threshold = max(1, min(99, int(low_battery_threshold_pct)))

    if simulated_status is not None:
        ac_line = int(simulated_status.get("ACLineStatus", 1)) & 0xFF
        batt_flag = int(simulated_status.get("BatteryFlag", 128)) & 0xFF
        batt_pct = int(simulated_status.get("BatteryLifePercent", 255)) & 0xFF
        sys_flag = int(simulated_status.get("SystemStatusFlag", 0)) & 0xFF
        life_time = int(simulated_status.get("BatteryLifeTime", 0xFFFFFFFF)) & 0xFFFFFFFF
        full_life_time = int(simulated_status.get("BatteryFullLifeTime", 0xFFFFFFFF)) & 0xFFFFFFFF
        api_ok = True
    elif os.name == "nt" and hasattr(ctypes, "windll"):
        status = SYSTEM_POWER_STATUS()
        try:
            res = ctypes.windll.kernel32.GetSystemPowerStatus(ctypes.byref(status))
            api_ok = bool(res != 0)
        except Exception as exc:
            logger.debug("GetSystemPowerStatus 呼び出し例外: %s", exc)
            api_ok = False

        if api_ok:
            ac_line = int(status.ACLineStatus) & 0xFF
            batt_flag = int(status.BatteryFlag) & 0xFF
            batt_pct = int(status.BatteryLifePercent) & 0xFF
            sys_flag = int(status.SystemStatusFlag) & 0xFF
            life_time = int(status.BatteryLifeTime)
            full_life_time = int(status.BatteryFullLifeTime)
        else:
            ac_line = 1
            batt_flag = 128
            batt_pct = 255
            sys_flag = 0
            life_time = 0xFFFFFFFF
            full_life_time = 0xFFFFFFFF
    else:
        ac_line = 1
        batt_flag = 128
        batt_pct = 255
        sys_flag = 0
        life_time = 0xFFFFFFFF
        full_life_time = 0xFFFFFFFF
        api_ok = False

    is_on_battery = bool(ac_line == 0)
    is_battery_known = bool(0 <= batt_pct <= 100)
    is_desktop_ac_or_unknown = bool(ac_line == 1 or batt_pct == 255 or (batt_flag & 128) != 0 and ac_line != 0)
    should_stop = bool(is_on_battery and is_battery_known and (0 <= batt_pct <= threshold))

    if should_stop:
        power_mode = "UPS_BATTERY_CRITICAL_LOW"
    elif is_on_battery:
        power_mode = "ON_BATTERY_NORMAL"
    elif batt_pct == 255:
        power_mode = "DESKTOP_AC_NO_BATTERY"
    else:
        power_mode = "AC_ONLINE"

    return {
        "api_ok": api_ok,
        "ACLineStatus": ac_line,
        "BatteryFlag": batt_flag,
        "BatteryLifePercent": batt_pct,
        "SystemStatusFlag": sys_flag,
        "BatteryLifeTime": life_time,
        "BatteryFullLifeTime": full_life_time,
        "is_on_battery": is_on_battery,
        "is_battery_percent_known": is_battery_known,
        "is_desktop_ac_or_unknown": is_desktop_ac_or_unknown,
        "low_battery_threshold_pct": threshold,
        "should_emergency_graceful_stop": should_stop,
        "power_mode": power_mode,
    }


class SystemLifecycleGuard:
    """
    Win32 OSシャットダウン・再起動シグナル (`SetConsoleCtrlHandler`) および
    UPS電源喪失 (`GetSystemPowerStatus`) を監視し、
    既存ポジションの逆指値 (`STOP`) を100%保護したまま未約定買い指値のキャンセルと
    SQLite WAL `TRUNCATE` チェックポイントを完了させるライフサイクル保護機構。
    """

    def __init__(
        self,
        oco_manager: Any = None,
        remote_control: Any = None,
        db_wal: Any = None,
        notifier: Any = None,
        dry_run_notify: bool = False,
        low_battery_threshold_pct: int = 40,
        register_win32_handler: bool = True,
    ) -> None:
        self.oco_manager = oco_manager
        self.remote_control = remote_control
        self.db_wal = db_wal
        self.notifier = notifier
        self.dry_run_notify = bool(dry_run_notify)
        self.low_battery_threshold_pct = int(low_battery_threshold_pct)

        self._lock = threading.RLock()
        self.shutdown_latched: bool = False
        self.shutdown_reason: str = ""
        self.last_shutdown_result: Optional[Dict[str, Any]] = None
        self.handler_registered: bool = False
        self._ctrl_handler_ref: Optional[Callable[[int], int]] = None

        if register_win32_handler:
            self.register_console_ctrl_handler()

    def register_console_ctrl_handler(self) -> bool:
        """
        Windows `Kernel32.dll` の `SetConsoleCtrlHandler` にグレースフル停止ハンドラを登録する。
        GCによるコールバック回収を防ぐため、インスタンス属性 `self._ctrl_handler_ref` に保持する。
        """
        with self._lock:
            if self.handler_registered:
                return True
            if os.name != "nt" or not hasattr(ctypes, "windll") or not hasattr(ctypes, "WINFUNCTYPE"):
                return False
            try:
                handler_proto = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)

                def _win32_ctrl_callback(ctrl_type: int) -> int:
                    event_name = CTRL_EVENT_NAMES.get(int(ctrl_type), f"CTRL_EVENT_{ctrl_type}")
                    if int(ctrl_type) in (
                        CTRL_C_EVENT,
                        CTRL_BREAK_EVENT,
                        CTRL_CLOSE_EVENT,
                        CTRL_LOGOFF_EVENT,
                        CTRL_SHUTDOWN_EVENT,
                    ):
                        try:
                            self.execute_graceful_shutdown(reason=f"WIN32_{event_name}")
                        except Exception as exc:
                            logger.error("Win32 グレースフル停止ハンドラ例外 (%s): %s", event_name, exc)
                    # OS標準のプロセス終了処理を継続させるため False(0) を返す（CLOSE/LOGOFF/SHUTDOWN時は先行してWALフラッシュ完了済み）
                    return 0

                self._ctrl_handler_ref = handler_proto(_win32_ctrl_callback)
                ok = ctypes.windll.kernel32.SetConsoleCtrlHandler(self._ctrl_handler_ref, True)
                self.handler_registered = bool(ok != 0)
                atexit.register(self.unregister_console_ctrl_handler)
                return self.handler_registered
            except Exception as exc:
                logger.debug("SetConsoleCtrlHandler 登録スキップ: %s", exc)
                return False

    def unregister_console_ctrl_handler(self) -> None:
        with self._lock:
            if not self.handler_registered or self._ctrl_handler_ref is None:
                return
            if os.name == "nt" and hasattr(ctypes, "windll"):
                try:
                    ctypes.windll.kernel32.SetConsoleCtrlHandler(self._ctrl_handler_ref, False)
                except Exception:
                    pass
            self.handler_registered = False

    def handle_os_ctrl_event(
        self,
        ctrl_type: int,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Win32 コンソール制御イベント (`CTRL_CLOSE_EVENT=2`, `CTRL_LOGOFF_EVENT=5`, `CTRL_SHUTDOWN_EVENT=6`,
        `WM_QUERYENDSESSION=0x0011` 等) を受信した際のグレースフル停止を実行する。
        """
        event_name = CTRL_EVENT_NAMES.get(int(ctrl_type), f"CTRL_EVENT_{ctrl_type}")
        return self.execute_graceful_shutdown(
            reason=f"WIN32_{event_name}",
            now_dt=now_dt,
        )

    def execute_graceful_shutdown(
        self,
        reason: str = "OS_SHUTDOWN_OR_UPS_LOW_BATTERY",
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        緊急グレースフル停止シーケンスを冪等（Idempotent）に実行する：
        1. 新規エントリーを即時ロック（`shutdown_latched = True`, `remote_control.is_paused = True`）。
        2. 未約定の待機買い指値（`PENDING_BUY`）のみを全件キャンセルし、
           保有中ポジションを守っている取引所サーバー上の `STOP` 逆指値注文は100%そのまま維持する。
        3. `DatabaseWAL.checkpoint_truncate()`（`PRAGMA wal_checkpoint(TRUNCATE);`）を実行し、
           `.db-wal` をメインDBへ完全フラッシュしてサイズ 0 byte に切り詰める。
        """
        with self._lock:
            now_jst = _now_jst(now_dt)
            if self.shutdown_latched and self.last_shutdown_result is not None:
                cached = dict(self.last_shutdown_result)
                cached["idempotent_already_latched"] = True
                return cached

            self.shutdown_latched = True
            self.shutdown_reason = str(reason or "OS_SHUTDOWN_OR_UPS_LOW_BATTERY")

            canceled_buy_ids: List[str] = []
            preserved_sl_ids: List[str] = []

            # ① 新規エントリーロック ＆ PENDING_BUY のみキャンセル（STOP逆指値は絶対維持）
            if self.remote_control is not None:
                try:
                    self.remote_control.is_paused = True
                except Exception:
                    pass
                try:
                    owner_actor = getattr(self.remote_control, "allowed_owner_discord_id", None) or None
                    rc_res = self.remote_control.cancel_pending_entries_only(
                        actor_id=owner_actor,
                        now_dt=now_jst,
                    )
                    canceled_buy_ids = list(
                        rc_res.get("canceled_buy_order_ids")
                        or rc_res.get("canceled_order_ids")
                        or []
                    )
                    preserved_sl_ids = list(rc_res.get("preserved_sl_order_ids") or [])
                except Exception as rc_exc:
                    logger.error("remote_control.cancel_pending_entries_only 例外: %s", rc_exc)

            if self.oco_manager is not None and not canceled_buy_ids and not preserved_sl_ids:
                try:
                    pf = self.oco_manager.load_portfolio()
                    for p in pf.get("positions", []) or []:
                        if str(p.get("state", "")).strip().upper() == "DUST_QUARANTINED":
                            continue
                        sl_id = str(p.get("sl_order_id", "") or "").strip()
                        if sl_id:
                            preserved_sl_ids.append(sl_id)

                    remaining_pending: List[Dict[str, Any]] = []
                    for ord_item in list(pf.get("pending_orders", []) or []):
                        oid = str(ord_item.get("order_id", "") or "").strip()
                        side = str(ord_item.get("side", "BUY")).strip().upper()
                        if side == "BUY" and oid:
                            try:
                                self.oco_manager.broker.cancel_order(oid)
                            except Exception:
                                pass
                            canceled_buy_ids.append(oid)
                        else:
                            remaining_pending.append(ord_item)

                    if canceled_buy_ids or pf.get("pending_orders"):
                        pf["pending_orders"] = remaining_pending
                        self.oco_manager.save_portfolio(pf, now_dt=now_jst)
                except Exception as oco_exc:
                    logger.error("oco_manager 待機買い指値キャンセル例外: %s", oco_exc)

            # ② SQLite WAL TRUNCATE チェックポイント（.db-wal -> メインDB完全統合）
            wal_checkpoint_res: Dict[str, Any] = {"mode": "SKIPPED"}
            if self.db_wal is not None:
                try:
                    wal_checkpoint_res = self.db_wal.checkpoint_truncate()
                except Exception as wal_exc:
                    logger.error("グレースフル停止時の WAL TRUNCATE 例外: %s", wal_exc)
                    wal_checkpoint_res = {"mode": "ERROR", "error": str(wal_exc)}

            if self.notifier is not None:
                try:
                    self.notifier.notify_event(
                        title=f"🛡️ システム電源/OS保護グレースフル停止 ({self.shutdown_reason})",
                        message=(
                            f"・検知事由: `{self.shutdown_reason}`\n"
                            f"・待機BUY指値キャンセル件数: {len(canceled_buy_ids)} 件 (`{canceled_buy_ids}`)\n"
                            f"・取引所サーバー側で維持された保護STOP注文: {len(preserved_sl_ids)} 件 (`{preserved_sl_ids}`)\n"
                            f"・SQLite WAL チェックポイント: `{wal_checkpoint_res.get('mode', 'UNKNOWN')}`"
                        ),
                        level="WARN",
                        dry_run=self.dry_run_notify,
                    )
                except Exception:
                    pass

            result = {
                "status": "GRACEFUL_SHUTDOWN_COMPLETED",
                "shutdown_latched": True,
                "reason": self.shutdown_reason,
                "new_entries_locked": True,
                "canceled_buy_order_ids": canceled_buy_ids,
                "preserved_sl_order_ids": preserved_sl_ids,
                "wal_checkpoint": wal_checkpoint_res,
                "timestamp": now_jst.isoformat(timespec="seconds"),
                "idempotent_already_latched": False,
            }
            self.last_shutdown_result = result
            logger.warning(
                "🛡️ グレースフル停止シーケンス完了 (reason=%s, canceled_buys=%s, preserved_sls=%s, wal=%s)",
                self.shutdown_reason,
                canceled_buy_ids,
                preserved_sl_ids,
                wal_checkpoint_res,
            )
            return result

    def check_power_and_execute_if_needed(
        self,
        simulated_status: Optional[Dict[str, int]] = None,
        now_dt: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        `GetSystemPowerStatus` を点検し、`ACLineStatus == 0` かつ `0 <= BatteryLifePercent <= 40`
        （UPSバッテリー駆動＆残量40%以下）を検知した場合は即座にグレースフル停止を実行する。
        """
        power_info = get_system_power_status(
            simulated_status=simulated_status,
            low_battery_threshold_pct=self.low_battery_threshold_pct,
        )
        if power_info.get("should_emergency_graceful_stop"):
            batt_pct = power_info.get("BatteryLifePercent", 0)
            shut_res = self.execute_graceful_shutdown(
                reason=f"UPS_ON_BATTERY_LOW_{batt_pct}PCT",
                now_dt=now_dt,
            )
            return {
                "triggered": True,
                "power_status": power_info,
                "shutdown_result": shut_res,
            }
        if self.shutdown_latched and self.last_shutdown_result is not None:
            return {
                "triggered": True,
                "power_status": power_info,
                "shutdown_result": self.last_shutdown_result,
            }
        return {
            "triggered": False,
            "power_status": power_info,
            "shutdown_result": None,
        }
