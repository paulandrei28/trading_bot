"""
dashboard_app.py
----------------
Web dashboard for FVG Live Trader.
Runs live_trader.py as a subprocess and streams live state + logs
to the browser via Socket.IO.

Usage:
    pip install flask flask-socketio
    python dashboard_app.py
    # then open http://localhost:5000
"""

from __future__ import annotations

import os
import re
import sys
import json
import threading
import subprocess
from datetime import datetime

from flask import Flask
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = "fvg-dashboard-2026"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ── Global state ──────────────────────────────────────────────────────────────
trader_state: dict = {
    "phase": "IDLE",
    "running": False,
    "or_filter": "UNKNOWN",
    "atr_filter": "UNKNOWN",
    "or_width": None,
    "atr_value": None,
    "trade": None,
}
process: subprocess.Popen | None = None
state_lock = threading.Lock()

# ── Phase → step mapping ──────────────────────────────────────────────────────
TAG_TO_PHASE = {
    "PRE-MARKET": "PRE-MARKET",
    "MARKET OPEN": "WARM-UP",
    "WARM-UP": "WARM-UP",
    "FILTER": "FILTER",
    "OPENING RANGE": "OPENING RANGE",
    "TRADE WINDOW": "SCANNING",
    "SCAN": "SCANNING",
    "SIGNAL": "SIGNAL FOUND",
    "ORDER": "ORDER PLACED",
    "MONITORING": "IN TRADE",
    "RESULT": "TRADE CLOSED",
    "SESSION END": "SESSION END",
    "SESSION SUMMARY": "SESSION END",
    "WEEKEND": "WEEKEND",
    "LATE START": "SESSION END",
}


# ── Log parser ────────────────────────────────────────────────────────────────
def parse_log_line(line: str) -> dict:
    updates: dict = {}

    # Phase from tag
    for tag, phase in TAG_TO_PHASE.items():
        if f"[{tag}]" in line:
            updates["phase"] = phase
            break

    # OR filter
    if "OR filter PASS" in line:
        updates["or_filter"] = "PASS"
    elif ("TODAY SKIPPED" in line and "OR" in line) or "OR filter SKIP" in line:
        updates["or_filter"] = "SKIP"

    # ATR filter
    if "ATR filter PASS" in line:
        updates["atr_filter"] = "PASS"
    elif ("TODAY SKIPPED" in line and "ATR" in line) or "ATR filter SKIP" in line:
        updates["atr_filter"] = "SKIP"

    # OR width
    m = re.search(r"OR width today[:\s]+([\d.]+)", line)
    if m:
        updates["or_width"] = float(m.group(1))

    # ATR value
    m = re.search(r"ATR today[:\s]+([\d.]+)", line)
    if m:
        updates["atr_value"] = float(m.group(1))

    # OR computed high/low
    m = re.search(r"Computed -- high=([\d.]+)\s+low=([\d.]+)\s+width=([\d.]+)", line)
    if m:
        updates["or_high"] = float(m.group(1))
        updates["or_low"] = float(m.group(2))
        updates["or_width"] = float(m.group(3))

    # Signal detected — entry/stop/target
    m = re.search(
        r"\[SIGNAL\] Setup detected -- (\w+) at (\d+:\d+).*?"
        r"entry=([\d.]+)\s+stop=([\d.]+)\s+target=([\d.]+)",
        line,
    )
    if m:
        updates["trade"] = {
            "direction": m.group(1),
            "entry_time": m.group(2),
            "entry": float(m.group(3)),
            "stop": float(m.group(4)),
            "target": float(m.group(5)),
            "status": "SIGNAL",
            "outcome": None,
            "result_r": None,
            "pnl_usd": None,
            "qty": None,
            "max_loss": None,
            "target_gain": None,
            "start_ts": None,
        }

    # Risk / qty line
    m = re.search(
        r"\[SIGNAL\] Risk: \$([\d.]+)/share\s+Qty: (\d+)\s+"
        r"Max loss: \$([\d.]+)\s+Target gain: \$([\d.]+)",
        line,
    )
    if m:
        updates["trade_partial"] = {
            "risk_per_share": float(m.group(1)),
            "qty": int(m.group(2)),
            "max_loss": float(m.group(3)),
            "target_gain": float(m.group(4)),
        }

    # Order placed → trade active
    if "[ORDER] Bracket placed" in line:
        updates["trade_status"] = "ACTIVE"
        updates["trade_start_ts"] = datetime.now().isoformat()

    # Monitoring confirmation
    if "[MONITORING] Trade is live" in line:
        updates["trade_status"] = "ACTIVE"

    # Target hit → WIN
    m = re.search(r"\[RESULT\] TARGET HIT -- \+([\d.]+)R\s+\+\$([\d.]+)", line)
    if m:
        updates["trade_result"] = {
            "outcome": "WIN",
            "result_r": float(m.group(1)),
            "pnl_usd": float(m.group(2)),
        }

    # Stop hit → LOSS
    m = re.search(r"\[RESULT\] STOP HIT -- (-?[\d.]+)R\s+\$(-?[\d.]+)", line)
    if m:
        updates["trade_result"] = {
            "outcome": "LOSS",
            "result_r": float(m.group(1)),
            "pnl_usd": float(m.group(2)),
        }

    return updates


# ── Trader subprocess thread ───────────────────────────────────────────────────
def run_trader_thread(config: dict) -> None:
    global process, trader_state

    script_dir = os.path.dirname(os.path.abspath(__file__))
    trader_path = os.path.join(script_dir, "live_trader.py")

    cmd = [
        sys.executable,
        trader_path,
        "--symbol",
        config["symbol"],
        "--risk",
        str(config["risk"]),
    ]
    if config.get("live"):
        cmd.append("--live")

    with state_lock:
        trader_state.update(
            {
                "phase": "STARTING",
                "running": True,
                "or_filter": "UNKNOWN",
                "atr_filter": "UNKNOWN",
                "or_width": None,
                "atr_value": None,
                "trade": None,
            }
        )
    socketio.emit("state_update", dict(trader_state))

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="replace",
            cwd=script_dir,
        )

        for raw_line in process.stdout:
            line = raw_line.rstrip()
            if not line:
                continue

            ts = datetime.now().strftime("%H:%M:%S")
            socketio.emit("log_line", {"line": line, "ts": ts})

            updates = parse_log_line(line)
            if updates:
                with state_lock:
                    if "phase" in updates:
                        trader_state["phase"] = updates["phase"]
                    for key in (
                        "or_filter",
                        "atr_filter",
                        "or_width",
                        "atr_value",
                        "or_high",
                        "or_low",
                    ):
                        if key in updates:
                            trader_state[key] = updates[key]
                    if "trade" in updates:
                        trader_state["trade"] = updates["trade"]
                    if "trade_partial" in updates and trader_state.get("trade"):
                        trader_state["trade"].update(updates["trade_partial"])
                    if "trade_status" in updates and trader_state.get("trade"):
                        trader_state["trade"]["status"] = updates["trade_status"]
                    if "trade_start_ts" in updates and trader_state.get("trade"):
                        trader_state["trade"]["start_ts"] = updates["trade_start_ts"]
                    if "trade_result" in updates and trader_state.get("trade"):
                        trader_state["trade"].update(updates["trade_result"])
                        trader_state["phase"] = "TRADE CLOSED"

                socketio.emit("state_update", dict(trader_state))

        process.wait()

    except Exception as exc:
        ts = datetime.now().strftime("%H:%M:%S")
        socketio.emit("log_line", {"line": f"[ERROR] {exc}", "ts": ts})

    finally:
        with state_lock:
            trader_state["running"] = False
            if trader_state["phase"] not in (
                "TRADE CLOSED",
                "SESSION END",
                "WEEKEND",
                "STOPPED",
            ):
                trader_state["phase"] = "STOPPED"
        socketio.emit("state_update", dict(trader_state))
        socketio.emit("trader_stopped", {})


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    html_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "dashboard.html"
    )
    with open(html_path, encoding="utf-8") as f:
        return f.read()


# ── Socket events ─────────────────────────────────────────────────────────────
@socketio.on("start_trader")
def handle_start(config):
    if not trader_state["running"]:
        t = threading.Thread(target=run_trader_thread, args=(config,), daemon=True)
        t.start()
        emit("started", {"ok": True})
    else:
        emit("started", {"ok": False, "reason": "Already running"})


@socketio.on("stop_trader")
def handle_stop():
    global process
    if process and trader_state["running"]:
        process.terminate()
        emit("stopping", {})


@socketio.on("get_state")
def handle_get_state():
    emit("state_update", dict(trader_state))


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  FVG Live Trader  --  Dashboard")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60)
    socketio.run(
        app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True
    )
