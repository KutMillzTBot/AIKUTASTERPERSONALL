#!/usr/bin/env python3
"""Automatic retrain scheduler for Supervisor bridge."""

import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if callable(load_dotenv):
    load_dotenv()


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, str(default))).strip()
    try:
        return int(raw)
    except Exception:
        matches = re.findall(r"-?\d+", raw)
        if matches:
            try:
                return int(matches[-1])
            except Exception:
                pass
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, str(default))).strip()
    try:
        return float(raw)
    except Exception:
        return float(default)


BRIDGE_URL = str(os.getenv("BRIDGE_URL", "http://127.0.0.1:5050")).strip().rstrip("/")
SCHEDULE_DAY = max(0, min(6, _env_int("RETRAIN_DAY", 6)))
SCHEDULE_HOUR = max(0, min(23, _env_int("RETRAIN_HOUR", 2)))
MIN_WIN_RATE = _env_float("RETRAIN_WIN_RATE", 0.45)
MIN_NEW_TRADES = max(10, _env_int("RETRAIN_TRADES", 50))
CHECK_INTERVAL = max(30, _env_int("RETRAIN_CHECK_INTERVAL", 300))

last_retrain = None
trades_since_retrain = 0


def bridge(endpoint: str, method: str = "GET", data=None):
    try:
        url = f"{BRIDGE_URL}{endpoint}"
        if method == "GET":
            response = requests.get(url, timeout=8)
        else:
            response = requests.post(url, json=data or {}, timeout=8)
        return response.json() if response.ok else None
    except Exception:
        return None


def do_retrain(reason: str) -> None:
    global last_retrain, trades_since_retrain
    print(f"[RETRAIN] Starting - reason: {reason}")
    payload = {"reason": reason, "timestamp": datetime.now(timezone.utc).isoformat()}
    result = bridge("/retrain", "POST", payload)
    if result:
        print("[RETRAIN] Triggered")
        last_retrain = datetime.now(timezone.utc)
        trades_since_retrain = 0
    else:
        print("[RETRAIN] Bridge not responding")


def check_win_rate_trigger() -> bool:
    data = bridge("/risk/summary")
    if not data:
        return False
    win_rate = float(data.get("win_rate", 1.0))
    trades = int(data.get("total_trades", 0))
    if win_rate < MIN_WIN_RATE and trades >= 20:
        print(f"[RETRAIN] Low win-rate trigger: {win_rate * 100:.1f}% over {trades} trades")
        return True
    return False


def check_trade_count_trigger() -> bool:
    global trades_since_retrain
    data = bridge("/risk/summary")
    if not data:
        return False
    total = int(data.get("total_trades", 0))
    previous = getattr(check_trade_count_trigger, "_prev_total", 0)
    check_trade_count_trigger._prev_total = total
    delta = max(0, total - previous)
    trades_since_retrain += delta
    if trades_since_retrain >= MIN_NEW_TRADES:
        print(f"[RETRAIN] Trade-count trigger: {trades_since_retrain} new trades")
        return True
    return False


def is_scheduled_time() -> bool:
    now = datetime.now(timezone.utc)
    if now.weekday() != SCHEDULE_DAY:
        return False
    if now.hour != SCHEDULE_HOUR:
        return False
    if last_retrain and (now - last_retrain) < timedelta(hours=20):
        return False
    return True


def scheduler_loop() -> None:
    print(
        f"[RETRAIN] Scheduler started: day={SCHEDULE_DAY} hour={SCHEDULE_HOUR} UTC "
        f"min_wr={MIN_WIN_RATE:.2f} min_new_trades={MIN_NEW_TRADES} interval={CHECK_INTERVAL}s"
    )
    while True:
        try:
            if is_scheduled_time():
                do_retrain("scheduled_weekly")
            elif check_win_rate_trigger():
                do_retrain("low_win_rate")
            elif check_trade_count_trigger():
                do_retrain("trade_count")
        except Exception as exc:
            print(f"[RETRAIN] Error: {exc}")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    scheduler_loop()
