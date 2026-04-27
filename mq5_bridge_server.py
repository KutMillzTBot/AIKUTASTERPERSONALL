#!/usr/bin/env python3
"""
SupervisorTrainer bridge server.

Purpose:
- Accept live MT5 updates from SupervisorEA/DataFeeder.
- Serve dashboard endpoints.
- Keep backward-compatible routes used by older scripts.
"""

import glob
import json
import math
import os
import random
import re
import sys
import threading
import time
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests

warnings.filterwarnings("ignore", category=RuntimeWarning)

try:
    from flask import Flask, jsonify, request
    from flask_cors import CORS
except ImportError:
    raise SystemExit("Run: pip install flask flask-cors")

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    from websocket import create_connection
except ImportError:
    create_connection = None

try:
    from supervisor_trainer import SupervisorTrainer

    _TRAINER_AVAILABLE = True
except Exception as exc:
    _TRAINER_AVAILABLE = False
    print(f"[BRIDGE] supervisor_trainer unavailable ({exc}), using mock/wired signal mode")

app = Flask(__name__)
CORS(app)

_t0 = time.time()
_trainer = None
_trainer_lock = threading.Lock()
_wired_failure_state = {"open_until": 0.0}
_market_quote_cache: Dict[str, Dict[str, Any]] = {}

if callable(load_dotenv):
    load_dotenv()

DERIV_APP_ID = str(os.getenv("DERIV_APP_ID", "1089")).strip() or "1089"
DERIV_API_TOKEN = str(os.getenv("DERIV_API_TOKEN", "")).strip()
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
MT_HEARTBEAT_TIMEOUT_SECONDS = max(180, int(os.getenv("MT_HEARTBEAT_TIMEOUT_SECONDS", "900")))
WIRED_SYSTEM_BASE_URL = str(os.getenv("WIRED_SYSTEM_BASE_URL", "http://127.0.0.1:8000")).strip().rstrip("/")
WIRED_PROXY_TIMEOUT_SECONDS = max(1.0, float(os.getenv("WIRED_PROXY_TIMEOUT_SECONDS", "5.0")))
WIRED_SIGNAL_TIMEOUT_SECONDS = max(1.0, float(os.getenv("WIRED_SIGNAL_TIMEOUT_SECONDS", "3.0")))
WIRED_FAILURE_BACKOFF_SECONDS = max(0.0, float(os.getenv("WIRED_FAILURE_BACKOFF_SECONDS", "12.0")))
MARKET_QUOTE_CACHE_TTL_SECONDS = max(1.0, float(os.getenv("MARKET_QUOTE_CACHE_TTL_SECONDS", "3.0")))
MARKET_DAY_OPEN_REFRESH_SECONDS = max(60.0, float(os.getenv("MARKET_DAY_OPEN_REFRESH_SECONDS", "300.0")))

DERIV_SYMBOL_MAP = {
    "EURUSD": "frxEURUSD",
    "GBPUSD": "frxGBPUSD",
    "USDJPY": "frxUSDJPY",
    "GBPJPY": "frxGBPJPY",
    "AUDUSD": "frxAUDUSD",
    "USDCAD": "frxUSDCAD",
    "USDCHF": "frxUSDCHF",
    "NZDUSD": "frxNZDUSD",
    "EURGBP": "frxEURGBP",
    "EURAUD": "frxEURAUD",
    "GBPCHF": "frxGBPCHF",
    "XAUUSD": "frxXAUUSD",
    "XAGUSD": "frxXAGUSD",
    "BTCUSD": "cryBTCUSD",
    "ETHUSD": "cryETHUSD",
    "LTCUSD": "cryLTCUSD",
    "XRPUSD": "cryXRPUSD",
    "SOLUSD": "crySOLUSD",
    "V10": "R_10",
    "V25": "R_25",
    "V50": "R_50",
    "V75": "R_75",
    "V100": "R_100",
    "CRASH500": "CRASH500",
    "BOOM500": "BOOM500",
    "CRASH300": "CRASH300",
    "BOOM300": "BOOM300",
    "CRASH1000": "CRASH1000",
    "BOOM1000": "BOOM1000",
}

SYMBOL_ALIASES = {
    "R_10": "V10",
    "R_25": "V25",
    "R_50": "V50",
    "R_75": "V75",
    "R_100": "V100",
    "VIX75": "V75",
    "VIX 75": "V75",
    "VOLATILITY10INDEX": "V10",
    "VOLATILITY25INDEX": "V25",
    "VOLATILITY50INDEX": "V50",
    "VOLATILITY75INDEX": "V75",
    "VOLATILITY100INDEX": "V100",
    "VOLATILITY 10 INDEX": "V10",
    "VOLATILITY 25 INDEX": "V25",
    "VOLATILITY 50 INDEX": "V50",
    "VOLATILITY 75 INDEX": "V75",
    "VOLATILITY 100 INDEX": "V100",
    "CRASH300INDEX": "CRASH300",
    "BOOM300INDEX": "BOOM300",
    "CRASH500INDEX": "CRASH500",
    "BOOM500INDEX": "BOOM500",
    "CRASH1000INDEX": "CRASH1000",
    "BOOM1000INDEX": "BOOM1000",
    "CRASH 300 INDEX": "CRASH300",
    "BOOM 300 INDEX": "BOOM300",
    "CRASH 500 INDEX": "CRASH500",
    "BOOM 500 INDEX": "BOOM500",
    "CRASH 1000 INDEX": "CRASH1000",
    "BOOM 1000 INDEX": "BOOM1000",
}

TF_TO_SECONDS = {
    "M1": 60,
    "M2": 120,
    "M3": 180,
    "M5": 300,
    "M10": 600,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H2": 7200,
    "H4": 14400,
    "H8": 28800,
    "D1": 86400,
}


def _utc_now_hms() -> str:
    return datetime.utcnow().strftime("%H:%M:%S")


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _normalize_symbol(sym: Any) -> str:
    s = str(sym or "").strip().upper()
    if not s:
        return "EURUSD"
    compact = re.sub(r"[^A-Z0-9]+", "", s)
    return SYMBOL_ALIASES.get(s, SYMBOL_ALIASES.get(compact, s))


def _deriv_symbol(sym: Any) -> str:
    s = _normalize_symbol(sym)
    return DERIV_SYMBOL_MAP.get(s, s)


def _to_unix(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    if not s:
        return 0
    if s.isdigit():
        return int(s)
    try:
        return int(datetime.fromisoformat(s.replace("Z", "")).timestamp())
    except Exception:
        return 0


def _tf_to_seconds(value: Any) -> int:
    if isinstance(value, (int, float)):
        return max(60, int(value))
    s = str(value or "").strip().upper()
    return TF_TO_SECONDS.get(s, 3600)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(v)))


def _wired_url(path: str) -> str:
    return f"{WIRED_SYSTEM_BASE_URL}{path if path.startswith('/') else '/' + path}"


def _wired_request(
    method: str,
    path: str,
    payload: Any = None,
    params: Dict[str, Any] | None = None,
    timeout: float | None = None,
    use_backoff: bool = True,
) -> Dict[str, Any] | None:
    now = time.time()
    if use_backoff and WIRED_FAILURE_BACKOFF_SECONDS > 0 and now < _wired_failure_state["open_until"]:
        return None

    effective_timeout = max(0.5, _safe_float(timeout if timeout is not None else WIRED_PROXY_TIMEOUT_SECONDS, WIRED_PROXY_TIMEOUT_SECONDS))

    try:
        response = requests.request(
            method.upper(),
            _wired_url(path),
            json=payload,
            params=params,
            timeout=effective_timeout,
        )
        response.raise_for_status()
        if use_backoff:
            _wired_failure_state["open_until"] = 0.0
        return response.json()
    except Exception:
        if use_backoff and WIRED_FAILURE_BACKOFF_SECONDS > 0:
            _wired_failure_state["open_until"] = max(_wired_failure_state["open_until"], now + WIRED_FAILURE_BACKOFF_SECONDS)
        return None


def _wired_signal(sym: str) -> Dict[str, Any] | None:
    # Use configurable fetch size
    count = int(os.getenv("CANDLE_FETCH_SIZE", "500"))
    return _wired_request("GET", "/signal", params={"symbol": sym, "timeframe": "M15", "count": count}, timeout=WIRED_SIGNAL_TIMEOUT_SECONDS)


def _wired_models_payload(sym: str) -> Dict[str, Any] | None:
    count = int(os.getenv("CANDLE_FETCH_SIZE", "500"))
    return _wired_request("GET", "/models/status", params={"symbol": sym, "timeframe": "M15", "count": count}, timeout=WIRED_SIGNAL_TIMEOUT_SECONDS)


def _transform_wired_models(payload: Dict[str, Any] | None) -> Dict[str, Dict[str, Any]] | None:
    if not isinstance(payload, dict):
        return None
    rows = payload.get("models")
    if not isinstance(rows, list):
        return None

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        module_name = str(row.get("module", "")).strip().lower()
        key = module_name.replace("model_", "") if module_name.startswith("model_") else module_name
        if not key:
            key = str(row.get("class", "aux_model")).strip().lower().replace("model", "").strip("_") or "aux_model"
        base = _models_meta.get(key, {})
        out[key] = {
            "accuracy": round(_safe_float(base.get("accuracy", 0.5), 0.5), 4),
            "weight": round(_safe_float(row.get("weight", base.get("weight", 1.0)), 1.0), 4),
            "signal": round(_safe_float(row.get("last_score", base.get("signal", 0.5)), 0.5), 4),
            "enabled": bool(row.get("ready", row.get("loaded", True))),
            "version": int(base.get("version", 1)),
            "loaded": bool(row.get("loaded", False)),
            "trained": bool(row.get("trained", False)),
            "ready": bool(row.get("ready", False)),
            "source": "wired",
            "last_error": row.get("last_error"),
        }
    return out


def _wired_guardian_summary() -> Dict[str, Any] | None:
    return _wired_request("GET", "/guardian/summary")


def _wired_guardian_tasks() -> Dict[str, Any] | None:
    return _wired_request("GET", "/guardian/tasks")


def _wired_knowledge_status() -> Dict[str, Any] | None:
    return _wired_request("GET", "/knowledge/status")


def _wired_agentic_status() -> Dict[str, Any] | None:
    return _wired_request("GET", "/agentic/status")


def _wired_orchestrator_status() -> Dict[str, Any] | None:
    return _wired_request("GET", "/orchestrator/status")


def _wired_observability_summary() -> Dict[str, Any] | None:
    return _wired_request("GET", "/observability/summary")


def _wired_reflection_status() -> Dict[str, Any] | None:
    return _wired_request("GET", "/reflection/status")


def _discover_model_names() -> List[str]:
    files = sorted(glob.glob("model_*.py"))
    names = [os.path.splitext(os.path.basename(p))[0].replace("model_", "") for p in files]

    if "ensemble_head" not in names:
        names.append("ensemble_head")

    while len(names) < 16:
        names.append(f"aux_model_{len(names) + 1}")

    return names[:16]


def _build_default_models() -> Dict[str, Dict[str, Any]]:
    models = {}
    for idx, name in enumerate(_discover_model_names()):
        seed = abs(hash(name)) % 100
        acc = _clamp(0.52 + (seed / 500.0), 0.52, 0.79)
        models[name] = {
            "weight": 1.0,
            "accuracy": round(acc, 4),
            "signal": 0.5,
            "enabled": True,
            "version": 2 if idx < 8 else 1,
        }
    return models


_models_meta = _build_default_models()

S: Dict[str, Any] = {
    "account": {
        "balance": 10000.0,
        "equity": 10000.0,
        "margin": 0.0,
        "free_margin": 10000.0,
        "margin_level": 0.0,
        "broker": "Deriv",
        "currency": "USD",
        "leverage": 500,
    },
    "positions": {},
    "quotes": {},
    "history": [],
    "risk": {
        "risk_pct": 1.0,
        "daily_limit_pct": 5.0,
        "max_drawdown_pct": 10.0,
        "daily_pnl": 0.0,
        "open_positions": 0,
        "total_trades": 0,
        "win_rate": 0.0,
        "loss_streak": 0,
        "wins": 0,
        "losses": 0,
    },
    "trading": {
        "enabled": True,
        "paused": False,
        "mode": "auto",
        "symbol": "",
        "selected_symbol": "",
        "selected_symbol_source": "default",
    },
    "backtest": {"status": "idle", "progress": 0, "result": None},
    "log": [],
    "retraining": False,
    "ingest": {
        "counts": {},
        "timeframe_counts": {},
        "total_bars": 0,
        "last_symbol": None,
        "last_time": None,
        "last_timeframe": None,
        "last_batch": 0,
        "last_bar": None,
    },
    "mt": {
        "desired_connected": True,
        "connected": False,
        "last_heartbeat": None,
        "last_source": None,
        "broker_mode": "auto",
        "active_symbol": None,
        "last_feed_symbol": None,
        "watchlist": [],
        "watchlist_updated_at": None,
    },
    "commands": {"next_id": 1, "queue": []},
}


def _log(msg: str, level: str = "info") -> None:
    now = _utc_now_hms()
    S["log"].append({"time": now, "msg": msg, "level": level})
    if len(S["log"]) > 300:
        S["log"] = S["log"][-300:]
    print(f"[BRIDGE {now}] {msg}")


def _touch_mt(source: str) -> None:
    S["mt"]["connected"] = True
    S["mt"]["last_source"] = source
    S["mt"]["last_heartbeat"] = _utc_now_iso()


def _mt_connected() -> bool:
    if not S["mt"]["desired_connected"]:
        return False
    hb = S["mt"].get("last_heartbeat")
    if not hb:
        return False
    try:
        dt = datetime.fromisoformat(hb.replace("Z", ""))
        return (datetime.utcnow() - dt).total_seconds() <= MT_HEARTBEAT_TIMEOUT_SECONDS
    except Exception:
        return False


def _queue_mt_command(command: str, **payload: Any) -> Dict[str, Any]:
    cmd_id = str(S["commands"]["next_id"])
    S["commands"]["next_id"] += 1
    item = {
        "id": cmd_id,
        "cmd": command,
        "created_at": _utc_now_iso(),
        "status": "queued",
    }
    item.update(payload)
    S["commands"]["queue"].append(item)
    S["commands"]["queue"] = S["commands"]["queue"][-100:]
    _log(f"Queued MT command {command}#{cmd_id}")
    return item


def _selected_symbol() -> str:
    return _normalize_symbol(S["trading"].get("selected_symbol", S["trading"].get("symbol", "EURUSD")))


def _selected_symbol_source() -> str:
    return str(S["trading"].get("selected_symbol_source", "default") or "default").lower()


def _set_selected_symbol(sym: Any, source: str = "manual") -> str:
    selected = _normalize_symbol(sym)
    S["trading"]["selected_symbol"] = selected
    S["trading"]["symbol"] = selected
    S["trading"]["selected_symbol_source"] = str(source or "manual").lower()
    return selected


def _merge_symbol_lists(*groups: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    for group in groups:
        if isinstance(group, dict):
            values = group.keys()
        elif isinstance(group, list):
            values = group
        else:
            values = []
        for item in values:
            sym = _normalize_symbol(item)
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
    return out


def _effective_mt_watchlist() -> List[str]:
    watchlist = _merge_symbol_lists(S["mt"].get("watchlist", []))
    if watchlist:
        return watchlist
    return _merge_symbol_lists([S["mt"].get("active_symbol"), _selected_symbol()])


def _voice_fallback_reply(text: str) -> str:
    symbol = _selected_symbol()
    account = S["account"]
    mt_status = "attached" if _mt_connected() else "waiting for MT5 heartbeat"
    signal_text = "signal unavailable"
    try:
        signal_text = _normalize_signal_payload(_mock_signal(symbol), symbol).get("signal", "signal unavailable")
    except Exception:
        pass
    return (
        f"Rich fallback active. "
        f"Current symbol: {symbol}. "
        f"Bridge mode: {S['trading']['mode']}. "
        f"MT link: {mt_status}. "
        f"Balance: {account.get('balance', 0):.2f} {account.get('currency', 'USD')}. "
        f"Equity: {account.get('equity', 0):.2f}. "
        f"Current signal snapshot: {signal_text}. "
        f"Prompt received: {text}"
    )


def _deriv_ws_call(payload: Dict[str, Any], timeout_seconds: float = 20.0) -> Dict[str, Any]:
    if create_connection is None:
        raise RuntimeError("websocket-client not installed (pip install websocket-client)")

    ws = create_connection(DERIV_WS_URL, timeout=max(1.0, float(timeout_seconds)))
    try:
        if DERIV_API_TOKEN:
            ws.send(json.dumps({"authorize": DERIV_API_TOKEN}))
            auth = json.loads(ws.recv())
            if auth.get("error"):
                msg = auth["error"].get("message", "authorize failed")
                raise RuntimeError(f"Deriv authorize failed: {msg}")

        ws.send(json.dumps(payload))
        resp = json.loads(ws.recv())
        if resp.get("error"):
            msg = resp["error"].get("message", "Deriv API error")
            raise RuntimeError(msg)
        return resp
    finally:
        try:
            ws.close()
        except Exception:
            pass


def _deriv_fetch_candles(
    symbol: str,
    timeframe: Any = "H1",
    count: int = None,
    start: Any = None,
    end: Any = None,
) -> Dict[str, Any]:
    # Configurable fetch size
    DEFAULT_CANDLE_COUNT = int(os.getenv("CANDLE_FETCH_SIZE", "500"))
    MAX_CANDLE_COUNT = int(os.getenv("MAX_CANDLE_FETCH_SIZE", "2000"))
    min_required = int(os.getenv("MIN_CLEAN_CANDLES", "120"))
    dsym = _deriv_symbol(symbol)
    granularity = _tf_to_seconds(timeframe)
    fetch_count = count if count is not None else DEFAULT_CANDLE_COUNT
    fetch_count = max(min_required, min(MAX_CANDLE_COUNT, _safe_int(fetch_count, DEFAULT_CANDLE_COUNT)))
    start_unix = _to_unix(start)
    end_unix = _to_unix(end)

    bars = []
    attempts = 0
    while attempts < 3:
        payload: Dict[str, Any] = {
            "ticks_history": dsym,
            "adjust_start_time": 1,
            "style": "candles",
            "granularity": granularity,
            "count": fetch_count,
            "end": end_unix if end_unix > 0 else "latest",
        }
        if start_unix > 0:
            payload["start"] = start_unix

        raw = _deriv_ws_call(payload)
        candles = raw.get("candles", [])
        bars = []
        for c in candles:
            epoch = _safe_int(c.get("epoch"), 0)
            o = _safe_float(c.get("open"), 0.0)
            h = _safe_float(c.get("high"), 0.0)
            l = _safe_float(c.get("low"), 0.0)
            cl = _safe_float(c.get("close"), 0.0)
            bars.append({"t": epoch, "o": o, "h": h, "l": l, "c": cl, "v": 0})
        # If not enough clean candles, try fetching more
        if len(bars) >= min_required:
            break
        fetch_count = min(fetch_count * 2, MAX_CANDLE_COUNT)
        attempts += 1
        _log(f"Not enough clean candles for {symbol} {timeframe}, retrying with count={fetch_count}", "warning")

    if len(bars) < min_required:
        _log(f"Candle fetch failed for {symbol} {timeframe}: only {len(bars)} candles after {attempts} attempts", "error")

    return {
        "symbol": _normalize_symbol(symbol),
        "deriv_symbol": dsym,
        "timeframe": str(timeframe).upper(),
        "granularity": granularity,
        "start": start_unix or None,
        "end": end_unix or None,
        "bars": bars,
        "count": len(bars),
    }


def _estimate_spread(symbol: str, mid_price: float) -> float:
    sym = _normalize_symbol(symbol)
    mid = max(0.00001, _safe_float(mid_price, 0.0))
    if sym.startswith("V") or sym.startswith("R_") or "CRASH" in sym or "BOOM" in sym:
        return max(0.02, mid * 0.00035)
    if sym.startswith("XAU"):
        return max(0.05, mid * 0.00008)
    if sym.startswith("XAG"):
        return max(0.01, mid * 0.00015)
    if "BTC" in sym or "ETH" in sym or "LTC" in sym or "XRP" in sym or "SOL" in sym:
        return max(0.5, mid * 0.00025)
    if mid >= 1000:
        return 0.5
    if mid >= 100:
        return 0.05
    if mid >= 10:
        return 0.01
    if mid >= 1:
        return 0.0002
    return 0.00002


def _get_market_quote(symbol: str, force: bool = False) -> Dict[str, Any] | None:
    sym = _normalize_symbol(symbol)
    now = time.time()
    cached = _market_quote_cache.get(sym, {})
    fetched_at = _safe_float(cached.get("fetched_at", 0.0), 0.0)
    if cached and not force and (now - fetched_at) <= MARKET_QUOTE_CACHE_TTL_SECONDS:
        out = dict(cached)
        out.pop("fetched_at", None)
        return out

    dsym = _deriv_symbol(sym)
    try:
        tick_resp = _deriv_ws_call({"ticks": dsym, "subscribe": 0}, timeout_seconds=1.2)
    except Exception:
        tick_resp = None

    tick = tick_resp.get("tick", {}) if isinstance(tick_resp, dict) else {}
    mid = _safe_float(tick.get("quote", tick.get("last", tick.get("close", 0.0))), 0.0)
    if mid <= 0.0:
        mid = _safe_float(cached.get("mid", 0.0), 0.0)
    if mid <= 0.0:
        return None

    bid = _safe_float(tick.get("bid", 0.0), 0.0)
    ask = _safe_float(tick.get("ask", 0.0), 0.0)
    if bid <= 0.0 or ask <= 0.0 or ask < bid:
        spread = _estimate_spread(sym, mid)
        bid = max(0.0, mid - (spread / 2.0))
        ask = max(bid, mid + (spread / 2.0))

    day_open = _safe_float(cached.get("day_open", 0.0), 0.0)
    day_open_refreshed = _safe_float(cached.get("day_open_refreshed_at", 0.0), 0.0)
    day_anchor = str(cached.get("day_anchor_date", ""))
    today_utc = datetime.utcnow().strftime("%Y-%m-%d")
    if day_open <= 0.0 or day_anchor != today_utc:
        day_open = mid
        day_open_refreshed = now
        day_anchor = today_utc

    if force and (now - day_open_refreshed) >= MARKET_DAY_OPEN_REFRESH_SECONDS:
        try:
            daily_resp = _deriv_ws_call(
                {
                    "ticks_history": dsym,
                    "adjust_start_time": 1,
                    "style": "candles",
                    "granularity": 86400,
                    "count": 2,
                    "end": "latest",
                },
                timeout_seconds=2.2,
            )
            candles = daily_resp.get("candles", []) if isinstance(daily_resp, dict) else []
            if candles:
                maybe_open = _safe_float(candles[-1].get("open", candles[-1].get("close", 0.0)), 0.0)
                if maybe_open > 0.0:
                    day_open = maybe_open
                    day_open_refreshed = now
                    day_anchor = today_utc
        except Exception:
            pass

    if day_open <= 0.0:
        day_open = mid
        day_open_refreshed = now

    daily_change = mid - day_open
    daily_change_pct = (daily_change / day_open) * 100.0 if day_open > 0.0 else 0.0
    epoch = _safe_int(tick.get("epoch", 0), 0)
    ts_iso = datetime.utcfromtimestamp(epoch).isoformat() + "Z" if epoch > 0 else _utc_now_iso()

    out = {
        "symbol": sym,
        "deriv_symbol": dsym,
        "bid": round(bid, 8),
        "ask": round(ask, 8),
        "mid": round(mid, 8),
        "last_price": round(mid, 8),
        "day_open": round(day_open, 8),
        "daily_change": round(daily_change, 8),
        "daily_change_pct": round(daily_change_pct, 4),
        "timestamp": ts_iso,
        "source": "deriv_quote",
        "synthetic_bidask": not (tick.get("bid") is not None and tick.get("ask") is not None),
        "day_open_refreshed_at": datetime.utcfromtimestamp(int(day_open_refreshed)).isoformat() + "Z" if day_open_refreshed > 0 else _utc_now_iso(),
        "day_anchor_date": day_anchor or today_utc,
    }
    _market_quote_cache[sym] = {**out, "fetched_at": now}
    return out


def _models_meta_list() -> List[Dict[str, Any]]:
    out = []
    for name, d in _models_meta.items():
        out.append(
            {
                "model": name,
                "accuracy": float(d.get("accuracy", 0.5)),
                "weight": float(d.get("weight", 1.0)),
                "signal": float(d.get("signal", 0.5)),
                "enabled": bool(d.get("enabled", True)),
                "version": int(d.get("version", 1)),
            }
        )
    return out


def _signal_text_from_any(action: Any) -> str:
    if isinstance(action, (int, float)):
        if action > 0:
            return "BUY"
        if action < 0:
            return "SELL"
        return "HOLD"

    s = str(action or "").upper()
    if "STRONG BUY" in s:
        return "STRONG BUY"
    if "STRONG SELL" in s:
        return "STRONG SELL"
    if "BUY" in s:
        return "BUY"
    if "SELL" in s:
        return "SELL"
    return "HOLD"


def _action_num_from_signal_text(text: str) -> int:
    s = _signal_text_from_any(text)
    if "BUY" in s:
        return 1
    if "SELL" in s:
        return -1
    return 0


def _normalize_signal_payload(payload: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    payload = dict(payload or {})
    signal_text = _signal_text_from_any(
        payload.get("signal") if payload.get("signal") is not None else payload.get("action")
    )
    action_num = payload.get("action")
    if not isinstance(action_num, (int, float)):
        action_num = _action_num_from_signal_text(signal_text)

    sl = _safe_float(payload.get("sl", payload.get("stop_loss", 0.0)))
    tp = _safe_float(payload.get("tp", payload.get("take_profit", 0.0)))
    ob50 = _safe_float(payload.get("ob50", payload.get("ob_50", 0.0)))

    ict = payload.get("ict") if isinstance(payload.get("ict"), dict) else {}
    if not ict:
        ict = {}
    ict.setdefault("ob50", ob50)
    ict.setdefault("ob_top", _safe_float(payload.get("ob_top", ob50)))
    ict.setdefault("ob_bottom", _safe_float(payload.get("ob_bottom", ob50)))
    ict.setdefault("fvg_top", _safe_float(payload.get("fvg_top", ob50)))
    ict.setdefault("fvg_bottom", _safe_float(payload.get("fvg_bottom", ob50)))
    ict.setdefault("sl", sl)
    ict.setdefault("tp", tp)
    ict.setdefault("bos", payload.get("bos", "None"))

    raw_contrib = payload.get("model_contributions")
    if not isinstance(raw_contrib, dict):
        raw_contrib = {}
    if not raw_contrib and isinstance(payload.get("models"), dict):
        for name, item in payload["models"].items():
            if not isinstance(item, dict):
                continue
            model_key = str(name).strip().lower()
            model_key = re.sub(r"[^a-z0-9]+", "_", model_key).strip("_")
            if model_key.endswith("_ict"):
                model_key = model_key[:-4]
            raw_contrib[model_key] = item.get("signal", 0.5)
    contrib = {}
    for name, d in _models_meta.items():
        contrib[name] = round(
            _clamp(_safe_float(raw_contrib.get(name, d.get("signal", 0.5)), 0.5), 0.0, 1.0), 4
        )

    payload["symbol"] = _normalize_symbol(payload.get("symbol", symbol))
    payload["score"] = _clamp(_safe_float(payload.get("score", 0.5)), 0.0, 1.0)
    payload["signal"] = signal_text
    payload["action"] = int(action_num)
    payload["action_text"] = signal_text
    payload["sl"] = sl
    payload["tp"] = tp
    payload["ob50"] = ob50
    payload["ob_50"] = ob50
    payload["ob_top"] = _safe_float(payload.get("ob_top", ict.get("ob_top", ob50)))
    payload["ob_bottom"] = _safe_float(payload.get("ob_bottom", ict.get("ob_bottom", ob50)))
    payload["ict"] = ict
    payload["model_contributions"] = contrib
    payload["head_decision"] = payload.get("head_decision", signal_text)
    payload["head_lot"] = payload.get("head_lot", 0)
    payload["head_reason"] = payload.get("head_reason", "")
    payload["timestamp"] = payload.get("timestamp", _utc_now_iso())
    return payload


def _get_trainer():
    global _trainer
    if not _TRAINER_AVAILABLE:
        return None
    if _trainer is None:
        with _trainer_lock:
            if _trainer is None:
                try:
                    _trainer = SupervisorTrainer(symbol=S["trading"]["symbol"])
                    _trainer.train()
                    _log("Trainer ready")
                except Exception as exc:
                    _log(f"Trainer init error: {exc}", "error")
    return _trainer


def _mock_signal(sym: str) -> Dict[str, Any]:
    score = 0.5 + 0.15 * math.sin(time.time() / 65 + hash(sym) % 9)
    score = _clamp(score, 0.05, 0.95)

    prices = {
        "EURUSD": 1.0850,
        "GBPUSD": 1.2650,
        "USDJPY": 149.50,
        "XAUUSD": 2150.0,
        "XAGUSD": 24.5,
        "BTCUSD": 65000.0,
        "CRASH500": 900.0,
        "BOOM500": 900.0,
        "V75": 600.0,
        "V100": 820.0,
    }
    p = prices.get(sym, 1.0)
    atr = p * 0.002

    if score > 0.65:
        sig = "STRONG BUY"
    elif score > 0.55:
        sig = "BUY"
    elif score < 0.35:
        sig = "STRONG SELL"
    elif score < 0.45:
        sig = "SELL"
    else:
        sig = "HOLD"

    sl = round(p - atr * 1.5 if "BUY" in sig else p + atr * 1.5, 5)
    tp = round(p + atr * 3.0 if "BUY" in sig else p - atr * 3.0, 5)
    ob50 = round((p + sl) / 2, 5)
    ob_top = round(ob50 + atr * 0.5, 5)
    ob_bottom = round(ob50 - atr * 0.5, 5)

    contrib = {}
    for name in _models_meta.keys():
        val = _clamp(score + random.uniform(-0.08, 0.08), 0.0, 1.0)
        contrib[name] = round(val, 4)
        _models_meta[name]["signal"] = round(val, 4)

    return {
        "symbol": sym,
        "score": round(score, 4),
        "action": _action_num_from_signal_text(sig),
        "action_text": sig,
        "signal": sig,
        "sl": sl,
        "tp": tp,
        "ob50": ob50,
        "ob_50": ob50,
        "ob_top": ob_top,
        "ob_bottom": ob_bottom,
        "ict": {
            "ob50": ob50,
            "ob_top": ob_top,
            "ob_bottom": ob_bottom,
            "fvg_top": round(ob50 + atr * 0.3, 5),
            "fvg_bottom": round(ob50 - atr * 0.3, 5),
            "sl": sl,
            "tp": tp,
            "bos": "None",
        },
        "model_contributions": contrib,
        "timestamp": _utc_now_iso(),
    }


@app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": _utc_now_iso()})


@app.route("/status")
def status():
    models = _models_meta_list()
    weights = {m["model"]: m["weight"] for m in models}
    symbol = _normalize_symbol(request.args.get("symbol", _selected_symbol()))

    total_pnl = sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values())
    ingest_count = int(S["ingest"]["counts"].get(symbol, 0))
    mt_live = _mt_connected()

    return jsonify(
        {
            "running": True,
            "mode": S["trading"]["mode"],
            "symbol": symbol,
            "selected_symbol": _selected_symbol(),
            "selected_symbol_source": _selected_symbol_source(),
            "trading_enabled": S["trading"]["enabled"],
            "paused": S["trading"]["paused"],
            "uptime": str(timedelta(seconds=int(time.time() - _t0))),
            "trainer_ok": _TRAINER_AVAILABLE,
            "models": models,
            "weights": weights,
            "model_count": len(models),
            "ingested_bars": ingest_count,
            "ingest_total_bars": int(S["ingest"]["total_bars"]),
            "ingest_last_symbol": S["ingest"]["last_symbol"],
            "ingest_last_time": S["ingest"]["last_time"],
            "portfolio": {
                "score": 100 if len(S["positions"]) == 0 else 65,
                "status": "Empty" if len(S["positions"]) == 0 else "Active",
                "open_trades": len(S["positions"]),
                "total_pnl": total_pnl,
            },
            "risk": S["risk"],
            "mt_connected": mt_live,
            "mt_last_heartbeat": S["mt"]["last_heartbeat"],
            "mt_source": S["mt"]["last_source"],
            "mt_symbol": S["mt"].get("active_symbol"),
            "mt_watchlist": _effective_mt_watchlist(),
            "broker_mode": S["mt"]["broker_mode"],
            "broker_detected": S["account"].get("broker", ""),
            "deriv_feed_ready": bool(DERIV_API_TOKEN),
        }
    )


@app.route("/health")
def health():
    active_symbols = sorted(set(_merge_symbol_lists([_selected_symbol()], S["ingest"]["counts"], _effective_mt_watchlist())))
    return jsonify(
        {
            "status": "running",
            "symbols": active_symbols,
            "active_symbols": active_symbols,
            "ingest_symbols": list(S["ingest"]["counts"].keys()),
            "ingest_counts": S["ingest"]["counts"],
            "ingest_timeframe_counts": S["ingest"]["timeframe_counts"],
            "selected_symbol": _selected_symbol(),
            "selected_symbol_source": _selected_symbol_source(),
            "last_symbol": S["ingest"]["last_symbol"] or S["mt"].get("active_symbol") or _selected_symbol(),
            "last_symbol_source": "ingest" if S["ingest"]["last_symbol"] else ("mt" if S["mt"].get("active_symbol") else "selected"),
            "last_symbol_time": S["ingest"]["last_time"],
            "last_timeframe": S["ingest"]["last_timeframe"],
            "mt_symbol": S["mt"].get("active_symbol"),
            "mt_watchlist": _effective_mt_watchlist(),
            "managers": ["Risk", "Execution", "Portfolio", "Head"],
            "portfolio": {
                "score": 100 if len(S["positions"]) == 0 else 65,
                "status": "Empty" if len(S["positions"]) == 0 else "Active",
                "open_trades": len(S["positions"]),
                "total_pnl": sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values()),
            },
            "risk": S["risk"],
            "mt_connected": _mt_connected(),
            "mt_last_heartbeat": S["mt"]["last_heartbeat"],
            "broker_mode": S["mt"]["broker_mode"],
            "broker_detected": S["account"].get("broker", ""),
            "deriv_feed_ready": bool(DERIV_API_TOKEN),
        }
    )


@app.route("/account")
def account():
    return jsonify(S["account"])


@app.route("/signal")
def signal():
    sym = _normalize_symbol(request.args.get("symbol", _selected_symbol()))

    payload = _wired_signal(sym)
    if payload:
        payload.setdefault("source", "wired_manager_pipeline")
    else:
        trainer = _get_trainer()
        if trainer:
            try:
                payload = trainer.get_signal(sym)
            except Exception as exc:
                _log(f"get_signal error: {exc}", "error")

    if payload is None:
        payload = _mock_signal(sym)

    normalized = _normalize_signal_payload(payload, sym)
    quote = S["quotes"].get(sym) or _market_quote_cache.get(sym)
    if isinstance(quote, dict):
        normalized["bid"] = quote.get("bid")
        normalized["ask"] = quote.get("ask")
        normalized["mid"] = quote.get("mid")
        normalized["last_price"] = quote.get("last_price")
        normalized["day_open"] = quote.get("day_open")
        normalized["daily_change"] = quote.get("daily_change")
        normalized["daily_change_pct"] = quote.get("daily_change_pct")
    if isinstance(payload.get("models"), dict):
        for raw_name, item in payload["models"].items():
            if not isinstance(item, dict):
                continue
            candidates = {
                re.sub(r"[^a-z0-9]+", "_", str(raw_name).strip().lower()).strip("_"),
                re.sub(r"[^a-z0-9]+", "_", str(raw_name).strip().lower()).replace("_ict", "").replace("_detector", "").replace("_matrix", ""),
            }
            for candidate in candidates:
                if candidate in _models_meta:
                    _models_meta[candidate]["signal"] = round(_safe_float(item.get("signal", 0.5), 0.5), 4)
                    _models_meta[candidate]["weight"] = round(_safe_float(item.get("weight", _models_meta[candidate]["weight"]), _models_meta[candidate]["weight"]), 4)
                    _models_meta[candidate]["enabled"] = bool(item.get("ready", True))
    for name, contrib in normalized["model_contributions"].items():
        if name in _models_meta:
            _models_meta[name]["signal"] = round(_safe_float(contrib, 0.5), 4)

    return jsonify(normalized)


@app.route("/signals/all")
def signals_all():
    syms = _merge_symbol_lists(S["mt"].get("watchlist", []), [S["mt"].get("active_symbol")], ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "CRASH500", "BOOM500", "V75", "V100"])
    out = {}
    trainer = _get_trainer()
    for sym in syms:
        payload = _wired_signal(sym)
        if payload is None and trainer:
            try:
                payload = trainer.get_signal(sym)
            except Exception:
                payload = None
        if payload is None:
            payload = _mock_signal(sym)
        out[sym] = _normalize_signal_payload(payload, sym)
    return jsonify(out)


@app.route("/positions")
def positions():
    return jsonify(S["positions"])


@app.route("/history")
def history():
    limit = _safe_int(request.args.get("limit", 20), 20)
    return jsonify(S["history"][-limit:])


@app.route("/head/log")
def head_log():
    recent = S["log"][-40:]
    return jsonify(
        {
            "recent_decisions": len(recent),
            "trades": 0,
            "vetoes": 0,
            "holds": 0,
            "log": recent[-10:],
        }
    )


@app.route("/risk/summary")
def risk_summary():
    return jsonify(S["risk"])


@app.route("/risk/set", methods=["POST"])
def risk_set():
    data = request.get_json(silent=True) or {}
    for k, v in data.items():
        if k in S["risk"]:
            S["risk"][k] = v
    return jsonify({"status": "ok", "risk": S["risk"]})


@app.route("/portfolio/health")
def portfolio_health():
    r = S["risk"]
    wr = _safe_float(r.get("win_rate", 0.5), 0.5)
    bal = _safe_float(S["account"].get("balance", 1), 1.0) or 1.0
    dd = _safe_float(r.get("daily_pnl", 0), 0.0) / bal
    streak = _safe_int(r.get("loss_streak", 0), 0)

    if len(S["positions"]) == 0 and _safe_int(r.get("total_trades", 0), 0) == 0:
        score, status_text = 1.0, "Empty"
    else:
        score = _clamp(wr - abs(dd) - streak * 0.05, 0.0, 1.0)
        status_text = "Good" if score > 0.7 else "Caution" if score > 0.4 else "At Risk"

    return jsonify(
        {
            "score": round(score, 4),
            "status": status_text,
            "open_trades": len(S["positions"]),
            "total_pnl": sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values()),
        }
    )


@app.route("/trading/enable", methods=["POST"])
def trading_enable():
    data = request.get_json(silent=True) or {}
    S["trading"]["enabled"] = bool(data.get("enabled", True))
    _log(f"Trading {'ENABLED' if S['trading']['enabled'] else 'DISABLED'}")
    return jsonify({"status": "ok", "enabled": S["trading"]["enabled"]})


@app.route("/trading/pause", methods=["POST"])
def trading_pause():
    S["trading"]["paused"] = True
    _log("Trading PAUSED")
    return jsonify({"status": "paused"})


@app.route("/trading/resume", methods=["POST"])
def trading_resume():
    S["trading"]["paused"] = False
    _log("Trading RESUMED")
    return jsonify({"status": "resumed"})


@app.route("/trading/mode", methods=["POST"])
def trading_mode():
    data = request.get_json(silent=True) or {}
    mode = str(data.get("mode", "auto")).lower()
    if mode not in {"auto", "semi", "off"}:
        mode = "auto"
    S["trading"]["mode"] = mode
    _log(f"Mode -> {mode.upper()}")
    return jsonify({"status": "ok", "mode": mode})


@app.route("/trading/symbol", methods=["GET", "POST"])
def trading_symbol():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        sym = _normalize_symbol(data.get("symbol", _selected_symbol()))
        if sym != _selected_symbol():
            _log(f"Symbol switched -> {sym}")
        _set_selected_symbol(sym, source="manual")
    return jsonify({"status": "ok", "symbol": _selected_symbol()})


@app.route("/trades/close_all", methods=["POST"])
def close_all():
    pnl = sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values())
    manual_keys = [k for k, p in S["positions"].items() if str(p.get("source", "")).lower() == "manual" or str(k).startswith("man-")]
    for k in manual_keys:
        S["history"].append(S["positions"].pop(k))

    queued = False
    if _mt_connected():
        _queue_mt_command("close_all", symbol=S["trading"]["symbol"])
        queued = True

    S["risk"]["open_positions"] = len(S["positions"])
    S["risk"]["daily_pnl"] = sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values())
    _log(f"Close-all requested manual_closed={len(manual_keys)} queued_mt={queued} PnL={pnl:.2f}")
    return jsonify({"closed": len(manual_keys), "queued_mt": queued, "total_pnl": round(pnl, 2)})


@app.route("/trades/close_symbol", methods=["POST"])
def close_symbol():
    data = request.get_json(silent=True) or {}
    sym = _normalize_symbol(data.get("symbol", ""))
    keys = [
        k for k, p in S["positions"].items()
        if _normalize_symbol(p.get("symbol")) == sym and (str(p.get("source", "")).lower() == "manual" or str(k).startswith("man-"))
    ]
    for k in keys:
        S["history"].append(S["positions"].pop(k))
    queued = False
    if _mt_connected():
        _queue_mt_command("close_symbol", symbol=sym)
        queued = True
    S["risk"]["open_positions"] = len(S["positions"])
    S["risk"]["daily_pnl"] = sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values())
    return jsonify({"status": "ok", "closed": len(keys), "queued_mt": queued})


@app.route("/trades/close_ticket", methods=["POST"])
def close_ticket():
    data = request.get_json(silent=True) or {}
    tid = str(data.get("ticket", ""))
    if tid in S["positions"] and (str(S["positions"][tid].get("source", "")).lower() == "manual" or str(tid).startswith("man-")):
        S["history"].append(S["positions"].pop(tid))
        S["risk"]["open_positions"] = len(S["positions"])
        S["risk"]["daily_pnl"] = sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values())
        return jsonify({"status": "closed", "queued_mt": False})
    if _mt_connected():
        _queue_mt_command("close_ticket", ticket=tid, symbol=S["trading"]["symbol"])
        return jsonify({"status": "queued", "queued_mt": True})
    return jsonify({"status": "not_found"}), 404


@app.route("/trades/manual", methods=["POST"])
def trades_manual():
    data = request.get_json(silent=True) or {}
    sym = _normalize_symbol(data.get("symbol", _selected_symbol()))
    order_type = str(data.get("order_type", data.get("type", "BUY_MARKET"))).upper()
    side = str(data.get("side", "")).upper()
    if not side:
        side = "SELL" if order_type.startswith("SELL") else "BUY"
    lot = _safe_float(data.get("lot", 0.01), 0.01)
    entry = _safe_float(data.get("entry", 0.0), 0.0)
    limit_price = _safe_float(data.get("limit_price", 0.0), 0.0)
    sl = _safe_float(data.get("sl", 0.0), 0.0)
    tp = _safe_float(data.get("tp", 0.0), 0.0)
    use_sl = bool(data.get("use_sl", True))
    use_tp = bool(data.get("use_tp", True))
    trailing_start_rr = _safe_float(data.get("trailing_start_rr", 0.5), 0.5)
    trailing_step_rr = _safe_float(data.get("trailing_step_rr", 0.2), 0.2)

    valid_types = {
        "BUY",
        "SELL",
        "BUY_MARKET",
        "SELL_MARKET",
        "BUY_LIMIT",
        "SELL_LIMIT",
        "BUY_STOP",
        "SELL_STOP",
        "BUY_STOP_LIMIT",
        "SELL_STOP_LIMIT",
    }
    if order_type not in valid_types or side not in {"BUY", "SELL"}:
        return jsonify({"status": "error", "error": "invalid_side"}), 400
    if entry <= 0 or lot <= 0:
        return jsonify({"status": "error", "error": "invalid_entry_or_lot"}), 400

    if use_sl and use_tp and side == "BUY":
        if sl >= entry or tp <= entry:
            return jsonify({"status": "error", "error": "invalid_sl_tp_for_buy"}), 400
    elif use_sl and use_tp and side == "SELL":
        if sl <= entry or tp >= entry:
            return jsonify({"status": "error", "error": "invalid_sl_tp_for_sell"}), 400

    if not use_sl:
        sl = 0.0
    if not use_tp:
        tp = 0.0

    if "LIMIT" in order_type and limit_price <= 0 and "STOP_LIMIT" in order_type:
        return jsonify({"status": "error", "error": "missing_limit_price"}), 400

    tid = f"man-{int(time.time() * 1000)}"
    S["positions"][tid] = {
        "ticket": tid,
        "symbol": sym,
        "type": side,
        "order_type": order_type,
        "lot": round(lot, 4),
        "entry": entry,
        "limit_price": limit_price,
        "sl": sl,
        "tp": tp,
        "pnl": 0.0,
        "source": "manual",
        "note": data.get("note", "Manual trade"),
        "trailing_start_rr": trailing_start_rr,
        "trailing_step_rr": trailing_step_rr,
        "date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _set_selected_symbol(sym, source="manual")
    S["risk"]["open_positions"] = len(S["positions"])
    S["risk"]["daily_pnl"] = sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values())
    queued_mt = False
    if _mt_connected():
        _queue_mt_command(
            "place_order",
            symbol=sym,
            side=side,
            order_type=order_type,
            lot=round(lot, 4),
            entry=entry,
            limit_price=limit_price,
            sl=sl,
            tp=tp,
            trailing_start_rr=trailing_start_rr,
            trailing_step_rr=trailing_step_rr,
        )
        queued_mt = True
    _touch_mt("manual_trade")
    _log(f"Manual {order_type} opened {sym} ticket={tid}")
    return jsonify({"status": "ok", "ticket": tid, "symbol": sym, "order_type": order_type, "queued_mt": queued_mt})


@app.route("/models/status")
def models_status():
    sym = _normalize_symbol(request.args.get("symbol", _selected_symbol()))
    wired = _transform_wired_models(_wired_models_payload(sym))
    if wired:
        for name, row in wired.items():
            if name in _models_meta:
                _models_meta[name].update(row)
            else:
                _models_meta[name] = row
    return jsonify(_models_meta)


@app.route("/models/toggle", methods=["POST"])
def models_toggle():
    data = request.get_json(silent=True) or {}
    model = data.get("model")
    if model in _models_meta:
        _models_meta[model]["enabled"] = bool(data.get("enabled", True))
        _log(f"Model {model} {'ON' if _models_meta[model]['enabled'] else 'OFF'}")
    return jsonify({"status": "ok"})


@app.route("/models/reset_weights", methods=["POST"])
def models_reset():
    for name in _models_meta:
        _models_meta[name]["weight"] = 1.0
    _log("All model weights reset to 1.0")
    return jsonify({"status": "ok"})


@app.route("/retrain", methods=["POST"])
def retrain():
    if S["retraining"]:
        return jsonify({"status": "already_running"})

    data = request.get_json(silent=True) or {}
    reason = str(data.get("reason", "manual"))
    _log(f"Retrain triggered - {reason}")
    S["retraining"] = True

    def _run():
        try:
            trainer = _get_trainer()
            if trainer:
                trainer.train()
            _log("Retrain complete")
        except Exception as exc:
            _log(f"Retrain error: {exc}", "error")
        finally:
            S["retraining"] = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started"})


def _append_backtest_trades(trades: List[Dict[str, Any]]) -> None:
    if not trades:
        return
    S["history"].extend(trades)
    S["history"] = S["history"][-600:]


def _series_ema(values: List[float], period: int) -> List[float]:
    out: List[float] = []
    if not values:
        return out
    alpha = 2.0 / (max(1, period) + 1.0)
    ema = float(values[0])
    for value in values:
        v = _safe_float(value, ema)
        ema = (alpha * v) + ((1.0 - alpha) * ema)
        out.append(ema)
    return out


def _series_atr(bars: List[Dict[str, Any]], period: int = 14) -> List[float]:
    out: List[float] = []
    if not bars:
        return out
    tr_window: List[float] = []
    prev_close = None
    window = max(2, int(period))
    for bar in bars:
        high = _safe_float(bar.get("h"), 0.0)
        low = _safe_float(bar.get("l"), 0.0)
        close = _safe_float(bar.get("c"), 0.0)
        if prev_close is None:
            tr = max(0.0, high - low)
        else:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_window.append(max(0.0, tr))
        if len(tr_window) > window:
            tr_window.pop(0)
        out.append(sum(tr_window) / len(tr_window))
        prev_close = close
    return out


def _load_backtest_bars(symbol: str, days: int) -> tuple[List[Dict[str, Any]], str]:
    sym = _normalize_symbol(symbol)
    count_target = max(240, min(2000, days * 96 + 220))
    bars: List[Dict[str, Any]] = []
    source = "none"

    try:
        payload = _deriv_fetch_candles(sym, timeframe="M15", count=count_target)
        if isinstance(payload, dict):
            bars = payload.get("bars", []) or []
            if bars:
                source = "deriv_m15"
    except Exception as exc:
        _log(f"Backtest candle fetch error ({sym}): {exc}", "warning")

    if len(bars) < 120:
        trainer = _get_trainer()
        if trainer:
            try:
                df = trainer.fetch_data(symbol=sym, interval="1h", period="1y", min_bars=max(120, days * 12))
                fallback: List[Dict[str, Any]] = []
                for idx, row in df.iterrows():
                    if hasattr(idx, "timestamp"):
                        ts = int(idx.timestamp())
                    else:
                        ts = _to_unix(str(idx))
                    fallback.append(
                        {
                            "t": ts,
                            "o": _safe_float(row.get("Open"), row.get("Close", 0.0)),
                            "h": _safe_float(row.get("High"), row.get("Close", 0.0)),
                            "l": _safe_float(row.get("Low"), row.get("Close", 0.0)),
                            "c": _safe_float(row.get("Close"), 0.0),
                            "v": _safe_int(row.get("Volume"), 0),
                        }
                    )
                if len(fallback) > len(bars):
                    bars = fallback
                    source = "trainer_yfinance"
            except Exception as exc:
                _log(f"Backtest trainer fetch error ({sym}): {exc}", "warning")

    cleaned: List[Dict[str, Any]] = []
    seen_ts = set()
    for bar in sorted(bars, key=lambda x: _safe_int(x.get("t"), 0)):
        ts = _safe_int(bar.get("t"), 0)
        close = _safe_float(bar.get("c"), 0.0)
        if ts <= 0 or close <= 0 or ts in seen_ts:
            continue
        high = _safe_float(bar.get("h"), close)
        low = _safe_float(bar.get("l"), close)
        opened = _safe_float(bar.get("o"), close)
        cleaned.append(
            {
                "t": ts,
                "o": opened,
                "h": max(high, low, close, opened),
                "l": min(high, low, close, opened),
                "c": close,
                "v": _safe_int(bar.get("v"), 0),
            }
        )
        seen_ts.add(ts)
    return cleaned, source


def _run_backtest_simulation(symbol: str, days: int) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    sym = _normalize_symbol(symbol)
    bars, data_source = _load_backtest_bars(sym, days)
    if len(bars) < 120:
        return (
            {
                "symbol": sym,
                "days": days,
                "trades": 0,
                "win_rate": 0.0,
                "roi": 0.0,
                "max_dd": 0.0,
                "sharpe": 0.0,
                "bars": len(bars),
                "source": data_source or "none",
                "error": "insufficient_bars",
            },
            [],
        )

    closes = [_safe_float(b["c"], 0.0) for b in bars]
    highs = [_safe_float(b["h"], 0.0) for b in bars]
    lows = [_safe_float(b["l"], 0.0) for b in bars]
    times = [_safe_int(b["t"], 0) for b in bars]

    ema_fast = _series_ema(closes, 9)
    ema_slow = _series_ema(closes, 21)
    atr = _series_atr(bars, period=14)

    account_balance = 10000.0
    position_notional = max(500.0, account_balance * 0.1)
    equity_curve = [account_balance]
    trade_returns: List[float] = []
    trades_for_history: List[Dict[str, Any]] = []

    position: Dict[str, Any] | None = None

    def _close_position(idx: int, exit_price: float, reason: str) -> None:
        nonlocal account_balance, position
        if position is None:
            return
        direction = 1.0 if position["side"] == "BUY" else -1.0
        entry = max(1e-8, float(position["entry"]))
        ret = direction * ((float(exit_price) - entry) / entry)
        pnl = ret * position_notional
        account_balance += pnl
        equity_curve.append(account_balance)
        trade_returns.append(ret)

        closed_at = datetime.utcfromtimestamp(times[idx]).strftime("%Y-%m-%d %H:%M:%S")
        trades_for_history.append(
            {
                "ticket": f"bt-{times[idx]}-{len(trades_for_history) + 1}",
                "date": closed_at,
                "symbol": sym,
                "type": position["side"],
                "lot": 0.01,
                "entry": round(entry, 5),
                "sl": round(float(position["sl"]), 5),
                "tp": round(float(position["tp"]), 5),
                "pnl": round(pnl, 2),
                "source": "backtest",
                "note": f"Backtest ({reason})",
            }
        )
        position = None

    for i in range(22, len(bars)):
        fast_prev, slow_prev = ema_fast[i - 1], ema_slow[i - 1]
        fast_now, slow_now = ema_fast[i], ema_slow[i]
        cross_up = fast_now > slow_now and fast_prev <= slow_prev
        cross_down = fast_now < slow_now and fast_prev >= slow_prev

        if position is not None:
            low_i = lows[i]
            high_i = highs[i]
            close_i = closes[i]
            side = position["side"]
            sl = float(position["sl"])
            tp = float(position["tp"])
            if side == "BUY":
                if low_i <= sl and high_i >= tp:
                    _close_position(i, sl, "SL/TP same candle")
                elif low_i <= sl:
                    _close_position(i, sl, "SL")
                elif high_i >= tp:
                    _close_position(i, tp, "TP")
                elif cross_down:
                    _close_position(i, close_i, "Trend Flip")
            else:
                if high_i >= sl and low_i <= tp:
                    _close_position(i, sl, "SL/TP same candle")
                elif high_i >= sl:
                    _close_position(i, sl, "SL")
                elif low_i <= tp:
                    _close_position(i, tp, "TP")
                elif cross_up:
                    _close_position(i, close_i, "Trend Flip")

        if position is None and (cross_up or cross_down):
            side = "BUY" if cross_up else "SELL"
            entry = closes[i]
            atr_i = max(_safe_float(atr[i], 0.0), entry * 0.0012)
            if side == "BUY":
                sl = entry - atr_i * 1.4
                tp = entry + atr_i * 2.8
            else:
                sl = entry + atr_i * 1.4
                tp = entry - atr_i * 2.8
            position = {"side": side, "entry": entry, "sl": sl, "tp": tp, "opened_at": times[i]}

    if position is not None:
        _close_position(len(bars) - 1, closes[-1], "End Of Data")

    trade_count = len(trades_for_history)
    wins = sum(1 for t in trades_for_history if _safe_float(t.get("pnl"), 0.0) > 0)
    win_rate = (wins / trade_count) if trade_count else 0.0

    start_balance = 10000.0
    roi = ((account_balance - start_balance) / start_balance) * 100.0

    peak = equity_curve[0] if equity_curve else start_balance
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            dd = ((peak - value) / peak) * 100.0
            max_dd = max(max_dd, dd)

    if len(trade_returns) > 1:
        mean_ret = sum(trade_returns) / len(trade_returns)
        variance = sum((r - mean_ret) ** 2 for r in trade_returns) / max(1, len(trade_returns) - 1)
        std_ret = math.sqrt(max(variance, 0.0))
        sharpe = (mean_ret / std_ret) * math.sqrt(len(trade_returns)) if std_ret > 1e-9 else 0.0
    else:
        sharpe = 0.0

    result = {
        "symbol": sym,
        "days": days,
        "trades": trade_count,
        "win_rate": round(_clamp(win_rate, 0.0, 1.0), 4),
        "roi": round(roi, 2),
        "max_dd": round(max_dd, 2),
        "sharpe": round(sharpe, 2),
        "bars": len(bars),
        "source": data_source or "none",
    }
    return result, trades_for_history


@app.route("/backtest/run", methods=["POST"])
def backtest_run():
    data = request.get_json(silent=True) or {}
    sym = _normalize_symbol(data.get("symbol", "EURUSD"))
    days = max(1, _safe_int(data.get("days", 30), 30))
    S["backtest"] = {"status": "running", "progress": 0, "result": None}
    _log(f"Backtest: {sym} {days}d")

    def _run():
        try:
            S["backtest"]["progress"] = 15
            result, trades = _run_backtest_simulation(sym, days)
            S["backtest"]["progress"] = 90
            _append_backtest_trades(trades)
            S["backtest"].update({"status": "complete", "progress": 100, "result": result})
            _log(
                f"Backtest done symbol={sym} trades={result.get('trades', 0)} "
                f"WR={_safe_float(result.get('win_rate'), 0.0) * 100:.1f}% "
                f"ROI={_safe_float(result.get('roi'), 0.0):.2f}% "
                f"source={result.get('source', 'none')}"
            )
        except Exception as exc:
            S["backtest"].update(
                {
                    "status": "error",
                    "progress": 100,
                    "result": {"symbol": sym, "days": days, "error": str(exc)},
                }
            )
            _log(f"Backtest failed ({sym}): {exc}", "error")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/backtest")
def backtest_legacy():
    sym = _normalize_symbol(request.args.get("symbol", S["trading"]["symbol"]))
    if request.args.get("days"):
        days = max(1, _safe_int(request.args.get("days"), 30))
    elif request.args.get("start") and request.args.get("end"):
        days = 30
    else:
        days = 30

    S["backtest"] = {"status": "running", "progress": 0, "result": None}
    _log(f"Backtest: {sym} {days}d (legacy)")

    def _run():
        try:
            S["backtest"]["progress"] = 15
            result, trades = _run_backtest_simulation(sym, days)
            S["backtest"]["progress"] = 90
            _append_backtest_trades(trades)
            S["backtest"].update({"status": "complete", "progress": 100, "result": result})
            _log(
                f"Backtest done symbol={sym} trades={result.get('trades', 0)} "
                f"WR={_safe_float(result.get('win_rate'), 0.0) * 100:.1f}% "
                f"ROI={_safe_float(result.get('roi'), 0.0):.2f}% "
                f"source={result.get('source', 'none')} (legacy)"
            )
        except Exception as exc:
            S["backtest"].update(
                {
                    "status": "error",
                    "progress": 100,
                    "result": {"symbol": sym, "days": days, "error": str(exc)},
                }
            )
            _log(f"Backtest failed ({sym}, legacy): {exc}", "error")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "symbol": sym})


@app.route("/backtest/status")
def backtest_status():
    return jsonify(S["backtest"])


@app.route("/backtest/last")
def backtest_last():
    return jsonify(S["backtest"].get("result") or {})


def _normalize_positions(raw_positions: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(raw_positions, dict):
        return raw_positions
    if isinstance(raw_positions, list):
        out = {}
        for p in raw_positions:
            if not isinstance(p, dict):
                continue
            tid = str(p.get("ticket", p.get("id", len(out) + 1)))
            out[tid] = p
        return out
    return {}


@app.route("/update", methods=["POST"])
def ea_update():
    data = request.get_json(silent=True) or {}

    if "account" in data and isinstance(data["account"], dict):
        S["account"].update(data["account"])

    if "positions" in data:
        S["positions"] = _normalize_positions(data["positions"])

    if "symbol" in data:
        mt_symbol = _normalize_symbol(data["symbol"])
        S["mt"]["active_symbol"] = mt_symbol
        if not S["trading"].get("selected_symbol") or _selected_symbol_source() == "default":
            _set_selected_symbol(mt_symbol, source="mt")

    if "watchlist" in data and isinstance(data["watchlist"], list):
        S["mt"]["watchlist"] = _merge_symbol_lists(data["watchlist"])
        S["mt"]["watchlist_updated_at"] = _utc_now_iso()

    if "quotes" in data and isinstance(data["quotes"], list):
        today_utc = datetime.utcnow().strftime("%Y-%m-%d")
        now_monotonic = time.time()
        for item in data["quotes"]:
            if not isinstance(item, dict):
                continue
            sym = _normalize_symbol(item.get("symbol", ""))
            if not sym:
                continue
            bid = _safe_float(item.get("bid", 0.0), 0.0)
            ask = _safe_float(item.get("ask", 0.0), 0.0)
            mid = _safe_float(item.get("mid", 0.0), 0.0)
            if bid > 0.0 and ask > 0.0:
                mid = (bid + ask) / 2.0
            if mid <= 0.0:
                mid = _safe_float(item.get("last_price", 0.0), 0.0)
            if mid <= 0.0:
                continue
            if bid <= 0.0 or ask <= 0.0 or ask < bid:
                spread = _estimate_spread(sym, mid)
                bid = max(0.0, mid - (spread / 2.0))
                ask = max(bid, mid + (spread / 2.0))

            day_open = _safe_float(item.get("day_open", 0.0), 0.0)
            if day_open <= 0.0:
                day_open = mid
            daily_change = _safe_float(item.get("daily_change", mid - day_open), mid - day_open)
            daily_change_pct = _safe_float(item.get("daily_change_pct", 0.0), 0.0)
            if abs(daily_change_pct) <= 1e-12 and day_open > 0.0:
                daily_change_pct = (daily_change / day_open) * 100.0
            ts = str(item.get("timestamp", _utc_now_iso()))

            quote_payload = {
                "symbol": sym,
                "bid": round(bid, 8),
                "ask": round(ask, 8),
                "mid": round(mid, 8),
                "last_price": round(mid, 8),
                "day_open": round(day_open, 8),
                "daily_change": round(daily_change, 8),
                "daily_change_pct": round(daily_change_pct, 4),
                "timestamp": ts,
                "source": "mt5_quote",
                "synthetic_bidask": False,
                "day_open_refreshed_at": _utc_now_iso(),
                "day_anchor_date": today_utc,
            }
            S["quotes"][sym] = quote_payload
            _market_quote_cache[sym] = {**quote_payload, "fetched_at": now_monotonic}

    if "history" in data and isinstance(data["history"], list):
        seen = {h.get("ticket") for h in S["history"]}
        for trade in data["history"]:
            if not isinstance(trade, dict):
                continue
            ticket = trade.get("ticket")
            if ticket in seen:
                continue

            S["history"].append(trade)
            pnl = _safe_float(trade.get("pnl", 0.0), 0.0)
            S["risk"]["total_trades"] += 1
            if pnl >= 0:
                S["risk"]["wins"] += 1
                S["risk"]["loss_streak"] = 0
            else:
                S["risk"]["losses"] += 1
                S["risk"]["loss_streak"] += 1

            n = max(1, _safe_int(S["risk"]["total_trades"], 1))
            S["risk"]["win_rate"] = S["risk"]["wins"] / n
            seen.add(ticket)

        S["history"] = S["history"][-600:]

    S["risk"]["open_positions"] = len(S["positions"])
    S["risk"]["daily_pnl"] = sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values())

    _touch_mt("update")
    return jsonify({"status": "ok", "symbol": _selected_symbol(), "mt_symbol": S["mt"].get("active_symbol")})


@app.route("/ea/command")
def ea_command():
    symbol = _normalize_symbol(request.args.get("symbol", S["trading"]["symbol"]))
    for item in S["commands"]["queue"]:
        cmd = str(item.get("cmd", "")).lower()
        item_symbol = _normalize_symbol(item.get("symbol", symbol))
        if cmd == "close_all" or item_symbol == symbol or not item.get("symbol"):
            item["status"] = "sent"
            item["last_sent_at"] = _utc_now_iso()
            return jsonify(item)
    return jsonify({"id": "", "cmd": "none"})


@app.route("/ea/command_ack", methods=["POST"])
def ea_command_ack():
    data = request.get_json(silent=True) or {}
    cmd_id = str(data.get("id", ""))
    status = str(data.get("status", "done")).lower()
    for idx, item in enumerate(list(S["commands"]["queue"])):
        if str(item.get("id")) != cmd_id:
            continue
        item["status"] = status
        item["acked_at"] = _utc_now_iso()
        _log(f"MT command ack {item.get('cmd')}#{cmd_id} -> {status}")
        if status in {"done", "executed", "ok"}:
            S["commands"]["queue"].pop(idx)
        return jsonify({"status": "ok"})
    return jsonify({"status": "missing"}), 404


def _parse_ingest_payload() -> Dict[str, Any]:
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data

    raw = request.get_data(cache=False, as_text=True)
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _external_module_path(name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "external_ai", name)


def _integration_status() -> Dict[str, Any]:
    wired_summary = _compact_wired_summary()
    guardian_summary = _wired_guardian_summary()
    knowledge_summary = _wired_knowledge_status()
    agentic_summary = _wired_agentic_status()
    orchestrator_summary = _wired_orchestrator_status()
    observability_summary = _wired_observability_summary()
    reflection_summary = _wired_reflection_status()
    signal_ready = False
    ready_models = 0
    if isinstance(wired_summary, dict):
        signal_engine = wired_summary.get("signal_engine") if isinstance(wired_summary.get("signal_engine"), dict) else {}
        signal_ready = bool(signal_engine.get("signal_ready", False))
        ready_models = _safe_int(signal_engine.get("ready", 0), 0)
    return {
        "telegram_configured": bool(os.getenv("TELEGRAM_TOKEN", "").strip()),
        "voice_available": os.path.exists(os.path.join(_external_module_path("voice_interface"), "voice_runner.py")),
        "vision_available": os.path.exists(os.path.join(_external_module_path("vision"), "market_structure_engine.py")),
        "synthetic_ready": True,
        "wired_pipeline_ready": bool(wired_summary),
        "wired_signal_ready": signal_ready,
        "wired_ready_models": ready_models,
        "wired_base_url": WIRED_SYSTEM_BASE_URL,
        "guardian_ready": bool(guardian_summary),
        "guardian_maintenance_mode": bool((guardian_summary or {}).get("maintenance_mode", False)),
        "guardian_queued_tasks": _safe_int((guardian_summary or {}).get("queued_tasks", 0), 0),
        "knowledge_ready": bool(knowledge_summary),
        "knowledge_memory_items": _safe_int((knowledge_summary or {}).get("memory_items", 0), 0),
        "knowledge_questions": _safe_int(((knowledge_summary or {}).get("stats") or {}).get("questions", 0), 0),
        "agentic_ready": bool(agentic_summary),
        "agentic_memory_items": _safe_int((agentic_summary or {}).get("memory", 0), 0),
        "agentic_history_items": _safe_int((agentic_summary or {}).get("history", 0), 0),
        "agentic_audits": _safe_int(((agentic_summary or {}).get("stats") or {}).get("audits", 0), 0),
        "agentic_repairs": _safe_int(((agentic_summary or {}).get("stats") or {}).get("repairs", 0), 0),
        "orchestrator_ready": bool(orchestrator_summary),
        "orchestrator_runs": _safe_int(((orchestrator_summary or {}).get("stats") or {}).get("runs", 0), 0),
        "orchestrator_tasks": _safe_int(((orchestrator_summary or {}).get("stats") or {}).get("tasks", 0), 0),
        "orchestrator_triggers": _safe_int(((orchestrator_summary or {}).get("stats") or {}).get("triggers", 0), 0),
        "orchestrator_memory_items": _safe_int(((orchestrator_summary or {}).get("trajectory") or {}).get("count", 0), 0),
        "observability_ready": bool(observability_summary),
        "observability_alerts": _safe_int((observability_summary or {}).get("alerts", 0), 0),
        "reflection_ready": bool(reflection_summary),
        "reflection_items": _safe_int((reflection_summary or {}).get("count", 0), 0),
    }


def _compact_wired_summary() -> Dict[str, Any] | None:
    wired_summary = _wired_request("GET", "/system/status")
    if not isinstance(wired_summary, dict):
        return None

    signal_engine = wired_summary.get("signal_engine") if isinstance(wired_summary.get("signal_engine"), dict) else {}
    data_validation = wired_summary.get("data_validation") if isinstance(wired_summary.get("data_validation"), dict) else {}
    guardian = wired_summary.get("guardian") if isinstance(wired_summary.get("guardian"), dict) else {}
    knowledge = wired_summary.get("knowledge") if isinstance(wired_summary.get("knowledge"), dict) else {}
    agentic = wired_summary.get("agentic") if isinstance(wired_summary.get("agentic"), dict) else {}
    orchestrator = wired_summary.get("orchestrator") if isinstance(wired_summary.get("orchestrator"), dict) else {}
    reflection = wired_summary.get("reflection") if isinstance(wired_summary.get("reflection"), dict) else {}

    compact_orchestrator = {
        "stats": orchestrator.get("stats", {}),
        "drift": orchestrator.get("drift", {}),
        "trajectory": orchestrator.get("trajectory", {}),
    }
    last_report = orchestrator.get("last_report")
    if isinstance(last_report, dict):
        compact_orchestrator["last_report"] = {
            "score": last_report.get("score"),
            "health": last_report.get("health"),
            "quality": last_report.get("quality"),
        }

    return {
        "bridge_url": wired_summary.get("bridge_url"),
        "signal_engine": {
            "bridge_url": signal_engine.get("bridge_url"),
            "ready": signal_engine.get("ready"),
            "trained": signal_engine.get("trained"),
            "total_models": signal_engine.get("total_models"),
            "signal_ready": signal_engine.get("signal_ready"),
            "signal_score": signal_engine.get("signal_score"),
            "signal_symbol": signal_engine.get("signal_symbol"),
            "last_fetch": signal_engine.get("last_fetch"),
            "last_validation": signal_engine.get("last_validation"),
            "error": signal_engine.get("error"),
        },
        "data_validation": data_validation,
        "guardian": {
            "alerts": guardian.get("alerts"),
            "last_audit": guardian.get("last_audit"),
            "last_improvement": guardian.get("last_improvement"),
            "maintenance_mode": guardian.get("maintenance_mode"),
            "queued_tasks": guardian.get("queued_tasks"),
        },
        "knowledge": {
            "memory_items": knowledge.get("memory_items"),
            "observations": knowledge.get("observations"),
            "stats": knowledge.get("stats", {}),
        },
        "agentic": {
            "history": agentic.get("history"),
            "memory": agentic.get("memory"),
            "stats": agentic.get("stats", {}),
            "policy": agentic.get("policy", {}),
        },
        "orchestrator": compact_orchestrator,
        "reflection": {
            "count": reflection.get("count", 0),
            "recent": reflection.get("recent", []),
        },
    }


@app.route("/integrations/status")
def integrations_status():
    return jsonify({"status": "ok", **_integration_status()})


@app.route("/ui/snapshot")
def ui_snapshot():
    total_pnl = sum(_safe_float(p.get("pnl", 0.0)) for p in S["positions"].values())
    wired_summary = _compact_wired_summary() or {}
    guardian_summary = _wired_guardian_summary() or {}
    knowledge_summary = _wired_knowledge_status() or {}
    agentic_summary = _wired_agentic_status() or {}
    orchestrator_summary = _wired_orchestrator_status() or {}
    observability_summary = _wired_observability_summary() or {}
    reflection_summary = _wired_reflection_status() or {}
    return jsonify(
        {
            "status": "ok",
            "bridge": {
                "url": f"http://{os.getenv('BRIDGE_HOST', '127.0.0.1')}:{os.getenv('BRIDGE_PORT', '5050')}",
                "symbol": _selected_symbol(),
                "mode": S["trading"]["mode"],
                "running": True,
            },
            "account": S["account"],
            "risk": S["risk"],
            "portfolio": {
                "open_trades": len(S["positions"]),
                "total_pnl": total_pnl,
            },
            "positions": S["positions"],
            "mt": {
                "connected": _mt_connected(),
                "last_heartbeat": S["mt"]["last_heartbeat"],
                "broker_mode": S["mt"]["broker_mode"],
                "symbol": S["mt"].get("active_symbol"),
                "watchlist": _effective_mt_watchlist(),
            },
            "integrations": _integration_status(),
            "wired": wired_summary,
            "guardian": guardian_summary,
            "knowledge": knowledge_summary,
            "agentic": agentic_summary,
            "orchestrator": orchestrator_summary,
            "observability": observability_summary,
            "reflection": reflection_summary,
        }
    )


@app.route("/wired/system/status")
def wired_system_status():
    data = _wired_request("GET", "/system/status")
    if not data:
        return jsonify(
            {
                "status": "degraded",
                "available": False,
                "error": "wired_system_unavailable",
                "signal_engine": {},
            }
        )
    return jsonify(data)


@app.route("/wired/signal")
def wired_signal():
    params = request.args.to_dict(flat=True)
    data = _wired_request("GET", "/signal", params=params)
    if not data:
        return jsonify({"status": "error", "error": "wired_signal_unavailable"}), 502
    return jsonify(data)


@app.route("/wired/models/status")
def wired_models_status():
    params = request.args.to_dict(flat=True)
    data = _wired_request("GET", "/models/status", params=params)
    if not data:
        return jsonify({"status": "error", "error": "wired_models_unavailable"}), 502
    return jsonify(data)


@app.route("/wired/pipeline/evaluate", methods=["GET", "POST"])
def wired_pipeline_evaluate():
    if request.method == "GET":
        data = _wired_request("GET", "/pipeline/evaluate", params=request.args.to_dict(flat=True))
    else:
        data = _wired_request("POST", "/pipeline/evaluate", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "wired_pipeline_unavailable"}), 502
    return jsonify(data)


@app.route("/wired/pipeline/execute", methods=["GET", "POST"])
def wired_pipeline_execute():
    if request.method == "GET":
        data = _wired_request("GET", "/pipeline/execute", params=request.args.to_dict(flat=True))
    else:
        data = _wired_request("POST", "/pipeline/execute", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "wired_execute_unavailable"}), 502
    return jsonify(data)


@app.route("/guardian/summary")
def guardian_summary():
    data = _wired_guardian_summary()
    if not data:
        return jsonify({"status": "degraded", "available": False, "error": "guardian_unavailable", "alerts": 0, "queued_tasks": 0, "last_audit": None})
    return jsonify(data)


@app.route("/guardian/audit", methods=["GET", "POST"])
def guardian_audit():
    data = _wired_request("POST" if request.method == "POST" else "GET", "/guardian/audit", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "guardian_unavailable"}), 502
    return jsonify(data)


@app.route("/guardian/improve", methods=["GET", "POST"])
def guardian_improve():
    data = _wired_request("POST" if request.method == "POST" else "GET", "/guardian/improve", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "guardian_unavailable"}), 502
    return jsonify(data)


@app.route("/guardian/health")
def guardian_health():
    data = _wired_request("GET", "/guardian/health")
    if not data:
        return jsonify({"status": "error", "error": "guardian_unavailable"}), 502
    return jsonify(data)


@app.route("/guardian/security")
def guardian_security():
    data = _wired_request("GET", "/guardian/security")
    if not data:
        return jsonify({"status": "error", "error": "guardian_unavailable"}), 502
    return jsonify(data)


@app.route("/guardian/tasks")
def guardian_tasks():
    data = _wired_guardian_tasks()
    if not data:
        return jsonify({"status": "error", "error": "guardian_unavailable"}), 502
    return jsonify(data)


@app.route("/guardian/tasks/<task_id>/run", methods=["POST"])
def guardian_run_task(task_id: str):
    data = _wired_request("POST", f"/guardian/tasks/{task_id}/run", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "guardian_unavailable"}), 502
    return jsonify(data)


@app.route("/knowledge/status")
def knowledge_status():
    data = _wired_knowledge_status()
    if not data:
        return jsonify({"status": "degraded", "available": False, "error": "knowledge_unavailable", "observations": 0, "memory_items": 0, "stats": {}})
    return jsonify(data)


@app.route("/knowledge/context")
def knowledge_context():
    data = _wired_request("GET", "/knowledge/context")
    if not data:
        return jsonify({"status": "error", "error": "knowledge_unavailable"}), 502
    return jsonify(data)


@app.route("/knowledge/summary")
def knowledge_summary():
    data = _wired_request("GET", "/knowledge/summary")
    if not data:
        return jsonify({"status": "error", "error": "knowledge_unavailable"}), 502
    return jsonify(data)


@app.route("/knowledge/analyze", methods=["POST"])
def knowledge_analyze():
    data = _wired_request("POST", "/knowledge/analyze", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "knowledge_unavailable"}), 502
    return jsonify(data)


@app.route("/knowledge/ask", methods=["POST"])
def knowledge_ask():
    data = _wired_request("POST", "/knowledge/ask", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "knowledge_unavailable"}), 502
    return jsonify(data)


@app.route("/knowledge/improve", methods=["GET", "POST"])
def knowledge_improve():
    if request.method == "GET":
        data = _wired_request("GET", "/knowledge/improve")
    else:
        data = _wired_request("POST", "/knowledge/improve", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "knowledge_unavailable"}), 502
    return jsonify(data)


@app.route("/knowledge/tasks")
def knowledge_tasks():
    data = _wired_request("GET", "/knowledge/tasks")
    if not data:
        return jsonify({"status": "error", "error": "knowledge_unavailable"}), 502
    return jsonify(data)


@app.route("/knowledge/send", methods=["POST"])
def knowledge_send():
    data = _wired_request("POST", "/knowledge/send", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "knowledge_unavailable"}), 502
    return jsonify(data)


@app.route("/agentic/status")
def agentic_status():
    data = _wired_agentic_status()
    if not data:
        return jsonify({"status": "degraded", "available": False, "error": "agentic_unavailable", "history": 0, "memory": 0, "stats": {}})
    return jsonify(data)


@app.route("/agentic/snapshot")
def agentic_snapshot():
    data = _wired_request("GET", "/agentic/snapshot")
    if not data:
        return jsonify({"status": "error", "error": "agentic_unavailable"}), 502
    return jsonify(data)


@app.route("/agentic/evaluate", methods=["GET", "POST"])
def agentic_evaluate():
    if request.method == "GET":
        data = _wired_request("GET", "/agentic/evaluate")
    else:
        data = _wired_request("POST", "/agentic/evaluate", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "agentic_unavailable"}), 502
    return jsonify(data)


@app.route("/agentic/execute", methods=["GET", "POST"])
def agentic_execute():
    if request.method == "GET":
        data = _wired_request("GET", "/agentic/execute")
    else:
        data = _wired_request("POST", "/agentic/execute", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "agentic_unavailable"}), 502
    return jsonify(data)


@app.route("/agentic/train", methods=["GET", "POST"])
def agentic_train():
    if request.method == "GET":
        data = _wired_request("GET", "/agentic/train")
    else:
        data = _wired_request("POST", "/agentic/train", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "agentic_unavailable"}), 502
    return jsonify(data)


@app.route("/agentic/task-bundle", methods=["GET", "POST"])
def agentic_task_bundle():
    if request.method == "GET":
        data = _wired_request("GET", "/agentic/task-bundle")
    else:
        data = _wired_request("POST", "/agentic/task-bundle", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "agentic_unavailable"}), 502
    return jsonify(data)


@app.route("/agentic/ask", methods=["POST"])
def agentic_ask():
    data = _wired_request("POST", "/agentic/ask", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "agentic_unavailable"}), 502
    return jsonify(data)


@app.route("/agentic/search", methods=["GET", "POST"])
def agentic_search():
    if request.method == "GET":
        data = _wired_request("GET", "/agentic/search", params=request.args.to_dict(flat=True))
    else:
        data = _wired_request("POST", "/agentic/search", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "agentic_unavailable"}), 502
    return jsonify(data)


@app.route("/orchestrator/status")
def orchestrator_status():
    data = _wired_orchestrator_status()
    if not data:
        return jsonify({"status": "degraded", "available": False, "error": "orchestrator_unavailable", "trajectory": {"count": 0}, "stats": {}})
    return jsonify(data)


@app.route("/orchestrator/snapshot")
def orchestrator_snapshot():
    data = _wired_request("GET", "/orchestrator/snapshot")
    if not data:
        return jsonify({"status": "error", "error": "orchestrator_unavailable"}), 502
    return jsonify(data)


@app.route("/orchestrator/evaluate", methods=["GET", "POST"])
def orchestrator_evaluate():
    if request.method == "GET":
        data = _wired_request("GET", "/orchestrator/evaluate")
    else:
        data = _wired_request("POST", "/orchestrator/evaluate", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "orchestrator_unavailable"}), 502
    return jsonify(data)


@app.route("/orchestrator/orchestrate", methods=["POST"])
def orchestrator_orchestrate():
    data = _wired_request("POST", "/orchestrator/orchestrate", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "orchestrator_unavailable"}), 502
    return jsonify(data)


@app.route("/orchestrator/execute", methods=["POST"])
def orchestrator_execute():
    data = _wired_request("POST", "/orchestrator/execute", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "orchestrator_unavailable"}), 502
    return jsonify(data)


@app.route("/orchestrator/explain", methods=["POST"])
def orchestrator_explain():
    data = _wired_request("POST", "/orchestrator/explain", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "orchestrator_unavailable"}), 502
    return jsonify(data)


@app.route("/orchestrator/cycle", methods=["GET", "POST"])
def orchestrator_cycle():
    if request.method == "GET":
        data = _wired_request("GET", "/orchestrator/cycle")
    else:
        data = _wired_request("POST", "/orchestrator/cycle", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "orchestrator_unavailable"}), 502
    return jsonify(data)


@app.route("/orchestrator/memory")
def orchestrator_memory():
    data = _wired_request("GET", "/orchestrator/memory", params=request.args.to_dict(flat=True))
    if not data:
        return jsonify({"status": "error", "error": "orchestrator_unavailable"}), 502
    return jsonify(data)


@app.route("/data/validate")
def data_validate_proxy():
    data = _wired_request("GET", "/data/validate", params=request.args.to_dict(flat=True))
    if not data:
        return jsonify({"status": "error", "error": "data_validation_unavailable"}), 502
    return jsonify(data)


@app.route("/observability/summary")
def observability_summary_proxy():
    data = _wired_observability_summary()
    if not data:
        return jsonify({"status": "degraded", "available": False, "error": "observability_unavailable", "health": "offline", "alerts": 0})
    return jsonify(data)


@app.route("/reflection/status")
def reflection_status_proxy():
    data = _wired_reflection_status()
    if not data:
        return jsonify({"status": "degraded", "available": False, "error": "reflection_unavailable", "count": 0, "recent": []})
    return jsonify(data)


@app.route("/reflection/analyze", methods=["POST"])
def reflection_analyze_proxy():
    data = _wired_request("POST", "/reflection/analyze", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "reflection_unavailable"}), 502
    return jsonify(data)


@app.route("/reflection/synthesize", methods=["POST"])
def reflection_synthesize_proxy():
    data = _wired_request("POST", "/reflection/synthesize", payload=request.get_json(silent=True) or {})
    if not data:
        return jsonify({"status": "error", "error": "reflection_unavailable"}), 502
    return jsonify(data)


@app.route("/integrations/voice/chat", methods=["POST"])
def integrations_voice_chat():
    data = request.get_json(silent=True) or {}
    text = str(data.get("text", "")).strip()
    if not text:
        return jsonify({"status": "error", "error": "missing_text"}), 400

    voice_dir = _external_module_path("voice_interface")
    if voice_dir not in sys.path:
        sys.path.insert(0, voice_dir)
    try:
        from voice_runner import send_chat

        reply = send_chat(text)
        return jsonify({"status": "ok", "reply": reply})
    except Exception as exc:
        reply = _voice_fallback_reply(text)
        _log(f"Voice fallback used: {exc}", "warn")
        return jsonify({"status": "ok", "reply": reply, "fallback": True, "error": str(exc)})


@app.route("/integrations/vision/structure", methods=["POST"])
def integrations_vision_structure():
    data = request.get_json(silent=True) or {}
    symbol = _normalize_symbol(data.get("symbol", S["trading"]["symbol"]))
    candles = data.get("candles")
    timeframe = data.get("timeframe", "M15")
    count = _safe_int(data.get("count", 120), 120)
    if not isinstance(candles, list) or not candles:
        candles = _deriv_fetch_candles(symbol, timeframe=timeframe, count=count).get("bars", [])
    if len(candles) < int(os.getenv("MIN_CLEAN_CANDLES", "120")):
        _log(f"[Vision] Not enough clean candles for {symbol} {timeframe}: {len(candles)}", "warning")

    vision_dir = _external_module_path("vision")
    if vision_dir not in sys.path:
        sys.path.insert(0, vision_dir)
    try:
        from market_structure_engine import MarketStructureEngine

        engine = MarketStructureEngine()
        events = engine.detect(symbol, candles)
        payload = []
        for e in events:
            payload.append(
                {
                    "pattern": e.pattern,
                    "direction": e.direction,
                    "confidence": e.confidence,
                    "time": e.time,
                    "symbol": e.symbol,
                    "zone_high": getattr(e, "zone_high", 0.0),
                    "zone_low": getattr(e, "zone_low", 0.0),
                    "top": getattr(e, "top", 0.0),
                    "bottom": getattr(e, "bottom", 0.0),
                }
            )
        return jsonify({"status": "ok", "symbol": symbol, "timeframe": str(timeframe).upper(), "events": payload})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 502


@app.route("/synthetic/run", methods=["POST"])
def synthetic_run():
    data = request.get_json(silent=True) or {}
    symbol = _normalize_symbol(data.get("symbol", S["trading"]["symbol"]))
    runs = max(1, min(25, _safe_int(data.get("runs", 8), 8)))
    days = max(5, min(365, _safe_int(data.get("days", 30), 30)))
    base = _safe_float(data.get("base_balance", S["account"].get("balance", 1000.0)), 1000.0)
    seed = _safe_int(data.get("seed", int(time.time()) % 100000), 1)
    rng = random.Random(seed)
    baseline, _ = _run_backtest_simulation(symbol, min(days, 45))
    baseline_wr = _clamp(_safe_float(baseline.get("win_rate", 0.52), 0.52), 0.15, 0.9)
    baseline_roi = _safe_float(baseline.get("roi", 0.0), 0.0)
    baseline_dd = max(0.5, _safe_float(baseline.get("max_dd", 6.0), 6.0))
    baseline_trades = max(6, _safe_int(baseline.get("trades", max(8, days)), max(8, days)))
    scenarios = []
    for idx in range(runs):
        trade_multiplier = 0.7 + (rng.random() * 0.9)
        trades = max(4, int(round(baseline_trades * trade_multiplier)))
        win_rate = _clamp(rng.gauss(baseline_wr, 0.08), 0.15, 0.9)
        roi = rng.gauss(baseline_roi, max(2.5, abs(baseline_roi) * 0.4 + 1.5))
        max_dd = max(0.5, abs(rng.gauss(baseline_dd, max(1.0, baseline_dd * 0.35))))
        ending_balance = base * (1.0 + roi / 100.0)
        scenarios.append(
            {
                "run": idx + 1,
                "symbol": symbol,
                "days": days,
                "trades": trades,
                "win_rate": round(win_rate, 4),
                "roi": round(roi, 2),
                "max_dd": round(max_dd, 2),
                "ending_balance": round(ending_balance, 2),
                "bias": "bullish" if roi >= 0 else "defensive",
            }
        )
    return jsonify(
        {
            "status": "ok",
            "symbol": symbol,
            "runs": runs,
            "days": days,
            "seed": seed,
            "baseline": {
                "win_rate": round(baseline_wr, 4),
                "roi": round(baseline_roi, 2),
                "max_dd": round(baseline_dd, 2),
                "trades": baseline_trades,
                "source": baseline.get("source", "none"),
                "bars": baseline.get("bars", 0),
            },
            "scenarios": scenarios,
        }
    )


def _normalize_bars(raw_bars: Any, root: Dict[str, Any]) -> List[Dict[str, Any]]:
    bars: List[Dict[str, Any]] = []

    if isinstance(raw_bars, dict):
        raw_bars = [raw_bars]
    elif not isinstance(raw_bars, list):
        raw_bars = []

    if not raw_bars and any(k in root for k in ("o", "open", "c", "close")):
        raw_bars = [root]

    for item in raw_bars:
        if isinstance(item, list) and len(item) >= 6:
            t, o, h, l, c, v = item[0:6]
        elif isinstance(item, dict):
            t = item.get("t", item.get("time", item.get("timestamp")))
            o = item.get("o", item.get("open"))
            h = item.get("h", item.get("high"))
            l = item.get("l", item.get("low"))
            c = item.get("c", item.get("close"))
            v = item.get("v", item.get("volume", item.get("tick_volume", 0)))
        else:
            continue

        bar = {
            "t": _safe_int(t, 0),
            "o": _safe_float(o, 0.0),
            "h": _safe_float(h, 0.0),
            "l": _safe_float(l, 0.0),
            "c": _safe_float(c, 0.0),
            "v": _safe_int(v, 0),
        }
        if bar["t"] <= 0 and all(bar[k] == 0 for k in ("o", "h", "l", "c")):
            continue
        bars.append(bar)

    return bars


def _register_ingest(sym: str, timeframe: str, bars: List[Dict[str, Any]], source: str = "ingest") -> None:
    key = f"{sym}|{timeframe}"
    S["ingest"]["counts"][sym] = _safe_int(S["ingest"]["counts"].get(sym, 0), 0) + len(bars)
    S["ingest"]["timeframe_counts"][key] = _safe_int(S["ingest"]["timeframe_counts"].get(key, 0), 0) + len(bars)
    S["ingest"]["total_bars"] = _safe_int(S["ingest"]["total_bars"], 0) + len(bars)
    S["ingest"]["last_symbol"] = sym
    S["ingest"]["last_time"] = _utc_now_iso()
    S["ingest"]["last_timeframe"] = timeframe
    S["ingest"]["last_batch"] = len(bars)
    S["ingest"]["last_bar"] = bars[-1] if bars else None
    S["mt"]["last_feed_symbol"] = sym
    _touch_mt(source)


@app.route("/ingest", methods=["POST"])
def ingest():
    data = _parse_ingest_payload()
    sym = _normalize_symbol(data.get("symbol", data.get("Symbol", S["trading"]["symbol"])))
    timeframe = str(data.get("timeframe", data.get("tf", data.get("period", "UNKNOWN")))).upper()

    raw_bars = data.get("bars", data.get("data", []))
    bars = _normalize_bars(raw_bars, data)

    key = f"{sym}|{timeframe}"
    _register_ingest(sym, timeframe, bars, source="ingest")

    return jsonify(
        {
            "status": "ok",
            "symbol": sym,
            "timeframe": timeframe,
            "new_bars": len(bars),
            "total_symbol": int(S["ingest"]["counts"][sym]),
            "total_timeframe": int(S["ingest"]["timeframe_counts"][key]),
            "total_all": int(S["ingest"]["total_bars"]),
        }
    )


@app.route("/ingest/status")
def ingest_status():
    return jsonify(
        {
            "status": "ok",
            "ingest": S["ingest"],
            "mt_connected": _mt_connected(),
        }
    )


@app.route("/market/quote")
def market_quote():
    symbol = _normalize_symbol(request.args.get("symbol", _selected_symbol()))
    force = str(request.args.get("force", "0")).strip().lower() in {"1", "true", "yes", "y"}
    quote = S["quotes"].get(symbol) or _market_quote_cache.get(symbol)
    if force or not quote:
        fetched = _get_market_quote(symbol, force=force)
        if fetched:
            quote = fetched
    if not quote:
        return jsonify({"status": "degraded", "available": False, "symbol": symbol, "error": "quote_unavailable"})
    out = dict(quote)
    out.pop("fetched_at", None)
    return jsonify({"status": "ok", **out})


@app.route("/market/quotes")
def market_quotes():
    raw_symbols = request.args.get("symbols", "")
    force = str(request.args.get("force", "0")).strip().lower() in {"1", "true", "yes", "y"}
    requested = [s.strip() for s in str(raw_symbols or "").split(",") if s.strip()]
    if requested:
        symbols = _merge_symbol_lists(requested)
    else:
        symbols = _merge_symbol_lists(S["mt"].get("watchlist", []), [S["mt"].get("active_symbol")], [_selected_symbol()])
    symbols = symbols[:20]
    out: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        quote = S["quotes"].get(sym) or _market_quote_cache.get(sym)
        if force or not quote:
            fetched = _get_market_quote(sym, force=force)
            if fetched:
                quote = fetched
        if quote:
            payload = dict(quote)
            payload.pop("fetched_at", None)
            out[sym] = payload
    return jsonify({"status": "ok", "quotes": out, "count": len(out), "requested": symbols})


@app.route("/deriv/candles", methods=["GET", "POST"])
def deriv_candles():
    data = request.args if request.method == "GET" else (request.get_json(silent=True) or {})
    symbol = _normalize_symbol(data.get("symbol", S["trading"]["symbol"]))
    timeframe = data.get("timeframe", data.get("tf", "H1"))
    count = _safe_int(data.get("count", int(os.getenv("CANDLE_FETCH_SIZE", "500"))), int(os.getenv("CANDLE_FETCH_SIZE", "500")))
    start = data.get("start")
    end = data.get("end")

    try:
        out = _deriv_fetch_candles(symbol=symbol, timeframe=timeframe, count=count, start=start, end=end)
        return jsonify({"status": "ok", **out})
    except Exception as exc:
        _log(f"Deriv candles error: {exc}", "error")
        return jsonify({"status": "error", "error": str(exc)}), 502


@app.route("/deriv/candles/ingest", methods=["POST"])
def deriv_candles_ingest():
    data = request.get_json(silent=True) or {}
    symbol = _normalize_symbol(data.get("symbol", S["trading"]["symbol"]))
    timeframe = str(data.get("timeframe", data.get("tf", "H1"))).upper()
    count = _safe_int(data.get("count", int(os.getenv("CANDLE_FETCH_SIZE", "500"))), int(os.getenv("CANDLE_FETCH_SIZE", "500")))
    start = data.get("start")
    end = data.get("end")

    try:
        out = _deriv_fetch_candles(symbol=symbol, timeframe=timeframe, count=count, start=start, end=end)
        bars = out.get("bars", [])
        _register_ingest(symbol, timeframe, bars, source="deriv")
        return jsonify(
            {
                "status": "ok",
                "symbol": symbol,
                "timeframe": timeframe,
                "fetched": len(bars),
                "total_symbol": int(S["ingest"]["counts"].get(symbol, 0)),
                "total_all": int(S["ingest"]["total_bars"]),
            }
        )
    except Exception as exc:
        _log(f"Deriv ingest error: {exc}", "error")
        return jsonify({"status": "error", "error": str(exc)}), 502


@app.route("/deriv/status")
def deriv_status():
    return jsonify(
        {
            "status": "ok",
            "configured": bool(DERIV_API_TOKEN),
            "app_id": DERIV_APP_ID,
            "ws_url": DERIV_WS_URL,
        }
    )


@app.route("/trade_result", methods=["POST"])
def trade_result():
    data = request.get_json(silent=True) or {}
    sym = _normalize_symbol(data.get("symbol", S["trading"]["symbol"]))
    prediction = _safe_float(data.get("prediction", 0.5), 0.5)
    actual = _safe_int(data.get("actual", 0), 0)
    profit = _safe_float(data.get("profit", 0.0), 0.0)

    S["history"].append(
        {
            "ticket": f"fb-{int(time.time() * 1000)}",
            "date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": sym,
            "type": "BUY" if prediction >= 0.5 else "SELL",
            "lot": 0.01,
            "pnl": round(profit, 2),
            "prediction": prediction,
            "actual": actual,
        }
    )
    S["history"] = S["history"][-600:]

    S["risk"]["total_trades"] += 1
    if profit >= 0:
        S["risk"]["wins"] += 1
        S["risk"]["loss_streak"] = 0
    else:
        S["risk"]["losses"] += 1
        S["risk"]["loss_streak"] += 1
    n = max(1, _safe_int(S["risk"]["total_trades"], 1))
    S["risk"]["win_rate"] = S["risk"]["wins"] / n

    _touch_mt("trade_result")
    _log(f"Feedback received symbol={sym} profit={profit:.2f}")
    return jsonify({"status": "ok"})


@app.route("/mt/status")
def mt_status():
    return jsonify(
        {
            "status": "ok",
            "connected": _mt_connected(),
            "desired_connected": S["mt"]["desired_connected"],
            "last_heartbeat": S["mt"]["last_heartbeat"],
            "last_source": S["mt"]["last_source"],
            "broker_mode": S["mt"]["broker_mode"],
            "broker_detected": S["account"].get("broker", ""),
            "symbol": S["trading"]["symbol"],
            "account": S["account"],
        }
    )


@app.route("/mt/connect", methods=["POST"])
def mt_connect():
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled", True))
    broker_mode = str(data.get("broker_mode", S["mt"]["broker_mode"])).lower()
    if broker_mode not in {"auto", "mt5", "deriv"}:
        broker_mode = "auto"
    S["mt"]["desired_connected"] = enabled
    S["mt"]["broker_mode"] = broker_mode
    if not enabled:
        S["mt"]["connected"] = False
    _log(f"MT desired connection {'ON' if enabled else 'OFF'} mode={broker_mode}")
    return jsonify({"status": "ok", "desired_connected": enabled, "broker_mode": broker_mode})


@app.route("/log")
def get_log():
    limit = max(1, _safe_int(request.args.get("limit", 50), 50))
    return jsonify(S["log"][-limit:])


@app.route("/admin/log", methods=["POST"])
def admin_log():
    data = request.get_json(silent=True) or {}
    _log(f"{data.get('event', '?')} - {data.get('reason', '')}")
    return jsonify({"status": "ok"})


@app.route("/admin/restart", methods=["POST"])
def admin_restart():
    _log("Restart requested")
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    host = os.getenv("BRIDGE_HOST", "127.0.0.1")
    port = int(os.getenv("BRIDGE_PORT", "5050"))
    print("=" * 55)
    print(" SupervisorTrainer Bridge Server")
    print(f" http://{host}:{port}")
    print(" Ctrl+C to stop")
    print("=" * 55)
    threading.Thread(target=_get_trainer, daemon=True).start()
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
