#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════════════════╗
║          Market Data Warehouse — Desktop Control Panel            ║
║                         Version 1.0.0                            ║
╚═══════════════════════════════════════════════════════════════════╝

A comprehensive GUI for managing every feature of the MDW system:
  • IB historical data ingestion        • CBOE volatility sync
  • Daily incremental updates           • DuckDB rebuild
  • IBC Gateway management              • Preset browser
  • Environment configuration           • Log viewer
  • Scheduler management                • Settings

Requirements:
    pip install customtkinter

Usage:
    python gui.py
    # OR place in repo root and run from there
"""

from __future__ import annotations

import json
import os
import platform
import socket
import subprocess
import sys
import threading
from datetime import datetime, date
from pathlib import Path
from typing import Callable, Optional

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

# ═══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS & PATHS
# ═══════════════════════════════════════════════════════════════════════════════

APP_NAME = "Market Data Warehouse"
VERSION  = "1.0.0"
IS_MAC   = platform.system() == "Darwin"

WAREHOUSE  = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
DATA_LAKE  = WAREHOUSE / "data-lake"
DUCKDB     = WAREHOUSE / "duckdb" / "market.duckdb"
LOGS       = WAREHOUSE / "logs"
REPO       = Path(__file__).resolve().parent
SCRIPTS    = REPO / "scripts"
PRESETS    = REPO / "presets"
ENV_FILE   = WAREHOUSE / ".env"

_VENV_PY  = WAREHOUSE / ".venv" / "bin" / "python"
PYTHON    = str(_VENV_PY) if _VENV_PY.exists() else sys.executable

DOCKER_GATEWAY_DIR = REPO / "docker" / "ib-gateway"

# ═══════════════════════════════════════════════════════════════════════════════
#  THEME & STYLE
# ═══════════════════════════════════════════════════════════════════════════════

C = {
    "bg":       "#090c10",
    "sidebar":  "#0c1118",
    "card":     "#111720",
    "card2":    "#161d28",
    "border":   "#1d2738",
    "border2":  "#253347",
    "acc":      "#3d8bfe",
    "acc_dim":  "#142044",
    "green":    "#3fb950",
    "green_dim":"#0e2a16",
    "yellow":   "#e3b341",
    "yellow_dim":"#2e2208",
    "red":      "#f85149",
    "red_dim":  "#2e0f0e",
    "purple":   "#a78bfa",
    "text":     "#dae2ed",
    "muted":    "#7d8999",
    "dim":      "#4a5568",
    "term":     "#07090e",
}

# Navigation items: (label, icon, page_key)
NAV_ITEMS = [
    ("Dashboard",        "⬡",  "dashboard"),
    ("Fetch Historical", "⬇",  "fetch"),
    ("Daily Update",     "⟳",  "daily"),
    ("CBOE Volatility",  "⋯",  "cboe"),
    ("Rebuild DuckDB",   "◈",  "rebuild"),
    ("IBC Gateway",      "⬡",  "gateway"),
    ("Presets Browser",  "☰",  "presets"),
    ("Environment",      "⚙",  "env"),
    ("Logs Viewer",      "≡",  "logs"),
    ("Settings",         "⊙",  "settings"),
]

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


# ═══════════════════════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def load_presets() -> list[dict]:
    """Load all preset JSON files from the presets/ directory."""
    result = []
    if not PRESETS.exists():
        return result
    for p in sorted(PRESETS.glob("*.json")):
        try:
            with p.open() as f:
                data = json.load(f)
            tickers = data.get("tickers", [])
            contracts = data.get("contracts", [])
            flat = tickers or [f"{c['root']}_{c['expiry']}" for c in contracts]
            result.append({
                "path":        str(p),
                "filename":    p.name,
                "name":        data.get("name", p.stem),
                "description": data.get("description", ""),
                "count":       len(flat),
                "tickers":     flat,
                "sector":      data.get("sector", ""),
                "source":      data.get("source", ""),
                "notes":       data.get("notes", ""),
            })
        except Exception:
            pass
    return result


def bronze_stats() -> dict[str, int]:
    """Count bronze Parquet snapshots by asset class."""
    bronze = DATA_LAKE / "bronze"
    stats: dict[str, int] = {}
    if bronze.exists():
        for d in bronze.glob("asset_class=*"):
            ac = d.name.split("=", 1)[1]
            stats[ac] = len(list(d.glob("symbol=*/data.parquet")))
    return stats


def duckdb_size_str() -> str:
    if not DUCKDB.exists():
        return "Not found"
    sz = DUCKDB.stat().st_size
    for unit in ("B", "KB", "MB", "GB"):
        if sz < 1024:
            return f"{sz:.1f} {unit}"
        sz /= 1024
    return f"{sz:.1f} TB"


def last_log_time() -> str:
    if not LOGS.exists():
        return "—"
    files = sorted(LOGS.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return "—"
    return datetime.fromtimestamp(files[0].stat().st_mtime).strftime("%Y-%m-%d %H:%M")


def ib_gateway_reachable(host: str = "127.0.0.1", port: int = 4001) -> bool:
    try:
        s = socket.create_connection((host, port), timeout=0.8)
        s.close()
        return True
    except Exception:
        return False


def load_env_file() -> dict[str, str]:
    result: dict[str, str] = {}
    for path in (ENV_FILE, REPO / ".env"):
        if path.exists():
            with path.open() as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, _, v = line.partition("=")
                        result[k.strip()] = v.strip().strip('"').strip("'")
            return result
    return result


def save_env_file(data: dict[str, str]) -> None:
    ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
    with ENV_FILE.open("w") as f:
        f.write(f"# Market Data Warehouse — .env\n"
                f"# Saved by GUI on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        for k, v in sorted(data.items()):
            f.write(f'{k}="{v}"\n')


def cmd(script_name: str, args: list[str]) -> str:
    """Build a full command string: python scripts/<name> [args]"""
    s = str(SCRIPTS / script_name)
    parts = [PYTHON, s] + [a for a in args if a]
    return " ".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
#  REUSABLE WIDGETS
# ═══════════════════════════════════════════════════════════════════════════════

class TerminalWidget(ctk.CTkFrame):
    """Streaming terminal output widget with colored tags and copy/clear."""

    def __init__(self, parent, height: int = 280, **kw):
        super().__init__(parent, fg_color=C["term"], corner_radius=8,
                         border_width=1, border_color=C["border"], **kw)
        self._running = False

        # Top bar
        bar = ctk.CTkFrame(self, fg_color=C["card2"], corner_radius=0, height=30)
        bar.pack(fill="x")
        bar.pack_propagate(False)
        ctk.CTkLabel(bar, text="  OUTPUT", font=("Courier New", 9, "bold"),
                     text_color=C["muted"]).pack(side="left", padx=8)
        self._status_lbl = ctk.CTkLabel(bar, text="", font=("Courier New", 9),
                                         text_color=C["muted"])
        self._status_lbl.pack(side="left", padx=4)
        for txt, fn in [("Copy", self._copy), ("Clear", self._clear)]:
            ctk.CTkButton(bar, text=txt, width=52, height=20, font=("Sora", 9),
                          fg_color=C["border"], hover_color=C["border2"],
                          text_color=C["muted"], command=fn
                          ).pack(side="right", padx=4, pady=4)

        # Text widget
        self._txt = tk.Text(
            self, bg=C["term"], fg="#c9d1d9", font=("Courier New", 11),
            wrap="word", relief="flat", borderwidth=0,
            padx=12, pady=8, state="disabled",
            insertbackground="white",
            selectbackground=C["acc"], selectforeground="white",
        )
        sb = tk.Scrollbar(self, command=self._txt.yview, bg=C["border"], troughcolor=C["bg"])
        self._txt.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._txt.pack(fill="both", expand=True)

        # Color tags
        self._txt.tag_configure("err",  foreground="#f28b82")
        self._txt.tag_configure("ok",   foreground="#56d364")
        self._txt.tag_configure("info", foreground="#58a6ff")
        self._txt.tag_configure("warn", foreground="#e3b341")
        self._txt.tag_configure("cmd",  foreground="#f7c948", font=("Courier New", 11, "bold"))
        self._txt.tag_configure("dim",  foreground=C["muted"])

    def _write(self, text: str, tag: str = ""):
        """Append text. Must be called from main thread (or via .after)."""
        self._txt.configure(state="normal")
        if tag:
            self._txt.insert("end", text, tag)
        else:
            lo = text.lower()
            if any(w in lo for w in ("error:", "exception", "traceback", "failed:")):
                self._txt.insert("end", text, "err")
            elif any(w in lo for w in ("warning:", "warn:")):
                self._txt.insert("end", text, "warn")
            elif "✅" in text or "success" in lo:
                self._txt.insert("end", text, "ok")
            else:
                self._txt.insert("end", text)
        self._txt.see("end")
        self._txt.configure(state="disabled")

    def write(self, text: str, tag: str = ""):
        """Thread-safe write."""
        self._txt.after(0, lambda: self._write(text, tag))

    def _clear(self):
        self._txt.configure(state="normal")
        self._txt.delete("1.0", "end")
        self._txt.configure(state="disabled")

    def _copy(self):
        content = self._txt.get("1.0", "end")
        self._txt.clipboard_clear()
        self._txt.clipboard_append(content)
        self._status_lbl.configure(text="Copied!")
        self._txt.after(2000, lambda: self._status_lbl.configure(text=""))

    def run(self, command: str, env_extras: Optional[dict] = None,
            on_done: Optional[Callable[[int], None]] = None):
        """Run a shell command, streaming stdout/stderr in real-time."""
        if self._running:
            messagebox.showwarning("Busy", "A command is already running.\nWait for it to finish.")
            return
        self._running = True
        self._status_lbl.configure(text="⟳ Running...")
        self.write(f"$ {command}\n\n", "cmd")

        def _worker():
            try:
                env = {**os.environ, **(env_extras or {})}
                proc = subprocess.Popen(
                    command, shell=True,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, env=env,
                )
                for line in proc.stdout:
                    self.write(line)
                proc.wait()
                rc = proc.returncode
                if rc == 0:
                    self.write(f"\n✅  Finished — exit code 0\n", "ok")
                    self._txt.after(0, lambda: self._status_lbl.configure(text="✅ Done"))
                else:
                    self.write(f"\n❌  Finished — exit code {rc}\n", "err")
                    self._txt.after(0, lambda: self._status_lbl.configure(text=f"❌ Exit {rc}"))
                if on_done:
                    self._txt.after(0, lambda: on_done(rc))
            except Exception as e:
                self.write(f"\n❌  Launch error: {e}\n", "err")
                self._txt.after(0, lambda: self._status_lbl.configure(text="❌ Error"))
            finally:
                self._running = False
        threading.Thread(target=_worker, daemon=True).start()


class Card(ctk.CTkFrame):
    def __init__(self, parent, **kw):
        super().__init__(parent, fg_color=C["card"], corner_radius=10,
                         border_width=1, border_color=C["border"], **kw)


class SectionHeader(ctk.CTkFrame):
    def __init__(self, parent, title: str, subtitle: str = "", **kw):
        super().__init__(parent, fg_color="transparent", **kw)
        ctk.CTkLabel(self, text=title, font=("Sora", 20, "bold"),
                     text_color=C["text"]).pack(anchor="w")
        if subtitle:
            ctk.CTkLabel(self, text=subtitle, font=("Sora", 12),
                         text_color=C["muted"]).pack(anchor="w", pady=(2, 0))


class Divider(ctk.CTkFrame):
    def __init__(self, parent, **kw):
        super().__init__(parent, height=1, fg_color=C["border"], **kw)


class Badge(ctk.CTkLabel):
    _MAP = {
        "ok":      (C["green_dim"], C["green"]),
        "warn":    (C["yellow_dim"], C["yellow"]),
        "error":   (C["red_dim"],   C["red"]),
        "info":    (C["acc_dim"],   C["acc"]),
        "neutral": (C["border"],    C["muted"]),
    }
    def __init__(self, parent, text="", state="neutral", **kw):
        bg, fg = self._MAP.get(state, self._MAP["neutral"])
        super().__init__(parent, text=f"  {text}  ", font=("Sora", 10, "bold"),
                         fg_color=bg, text_color=fg, corner_radius=10, **kw)

    def set(self, text: str, state: str = "neutral"):
        bg, fg = self._MAP.get(state, self._MAP["neutral"])
        self.configure(text=f"  {text}  ", fg_color=bg, text_color=fg)


def labeled_entry(parent, label: str, placeholder: str = "",
                  width: int = 260, default: str = "") -> ctk.CTkEntry:
    row = ctk.CTkFrame(parent, fg_color="transparent")
    row.pack(fill="x", pady=4)
    ctk.CTkLabel(row, text=label, font=("Sora", 12), text_color=C["muted"],
                 width=185, anchor="w").pack(side="left")
    e = ctk.CTkEntry(row, placeholder_text=placeholder, width=width,
                     fg_color=C["card2"], border_color=C["border2"],
                     text_color=C["text"])
    e.pack(side="left", padx=(8, 0))
    if default:
        e.insert(0, default)
    return e


def labeled_combo(parent, label: str, values: list[str],
                  width: int = 260, default: str = "") -> ctk.CTkComboBox:
    row = ctk.CTkFrame(parent, fg_color="transparent")
    row.pack(fill="x", pady=4)
    ctk.CTkLabel(row, text=label, font=("Sora", 12), text_color=C["muted"],
                 width=185, anchor="w").pack(side="left")
    cb = ctk.CTkComboBox(row, values=values, width=width,
                          fg_color=C["card2"], border_color=C["border2"],
                          button_color=C["border2"], button_hover_color=C["acc"],
                          text_color=C["text"])
    cb.pack(side="left", padx=(8, 0))
    if default:
        cb.set(default)
    return cb


def check_var(parent, label: str, row_frame=None) -> ctk.StringVar:
    frame = row_frame or ctk.CTkFrame(parent, fg_color="transparent")
    if not row_frame:
        frame.pack(fill="x", pady=3)
    var = tk.IntVar(value=0)
    ctk.CTkCheckBox(frame, text=label, variable=var, font=("Sora", 12),
                    text_color=C["text"], checkmark_color=C["acc"],
                    fg_color=C["acc"], hover_color=C["acc_dim"]).pack(side="left")
    return var


def run_btn(parent, text: str, cmd_fn: Callable, color: str = None) -> ctk.CTkButton:
    return ctk.CTkButton(
        parent, text=text, font=("Sora", 13, "bold"),
        fg_color=color or C["acc"], hover_color="#2b6bcc",
        height=40, command=cmd_fn
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

class DashboardPage(ctk.CTkScrollableFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent", corner_radius=0)
        self._nav = nav_fn
        self._stat_labels: dict[str, ctk.CTkLabel] = {}
        self._badges: dict[str, Badge] = {}
        self._build()

    def _build(self):
        SectionHeader(self, APP_NAME, f"v{VERSION} — Local-first market data warehouse").pack(anchor="w", pady=(0, 20))

        # ── Stat Cards ──
        stat_row = ctk.CTkFrame(self, fg_color="transparent")
        stat_row.pack(fill="x", pady=(0, 18))
        cfg = [
            ("equity",     "Equity Symbols",    "⬡", C["acc"]),
            ("volatility", "Vol. Indices",       "~", C["green"]),
            ("futures",    "Futures",            "◈", C["yellow"]),
            ("duckdb",     "DuckDB Size",        "⊙", C["purple"]),
        ]
        for key, lbl, icon, color in cfg:
            card = ctk.CTkFrame(stat_row, fg_color=C["card"], corner_radius=12,
                                border_width=1, border_color=C["border"], width=170)
            card.pack(side="left", padx=(0, 12))
            card.pack_propagate(False)
            ctk.CTkLabel(card, text=icon, font=("Courier New", 22),
                         text_color=color).pack(pady=(18, 4))
            val = ctk.CTkLabel(card, text="—", font=("Sora", 26, "bold"),
                               text_color=C["text"])
            val.pack()
            ctk.CTkLabel(card, text=lbl, font=("Sora", 10),
                         text_color=C["muted"]).pack(pady=(2, 18))
            self._stat_labels[key] = val

        # ── Two-column content ──
        cols = ctk.CTkFrame(self, fg_color="transparent")
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=5)
        cols.columnconfigure(1, weight=4)

        left  = ctk.CTkFrame(cols, fg_color="transparent")
        right = ctk.CTkFrame(cols, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right.grid(row=0, column=1, sticky="nsew")

        # Quick Actions
        qa = Card(left)
        qa.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(qa, text="Quick Actions", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(qa).pack(fill="x", padx=16, pady=(0, 10))

        actions = [
            ("⟳  Daily Update",         "daily",   C["acc"]),
            ("⬇  Fetch IB Historical",  "fetch",   "#2a5298"),
            ("⋯  CBOE Volatility Sync", "cboe",    "#1a5c2e"),
            ("◈  Rebuild DuckDB",       "rebuild", "#3d2a00"),
            ("≡  View Logs",            "logs",    C["border2"]),
            ("☰  Browse Presets",       "presets", C["border2"]),
        ]
        for label, page_id, color in actions:
            ctk.CTkButton(
                qa, text=label, font=("Sora", 12), anchor="w",
                fg_color=color, hover_color=C["border2"],
                text_color=C["text"], height=38,
                command=lambda p=page_id: self._nav(p)
            ).pack(fill="x", padx=16, pady=3)
        ctk.CTkFrame(qa, height=12, fg_color="transparent").pack()

        # System Status
        sys_card = Card(right)
        sys_card.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(sys_card, text="System Status", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(sys_card).pack(fill="x", padx=16, pady=(0, 10))

        status_items = [
            ("IB Gateway (4001)", "ib"),
            ("DuckDB File",       "duckdb"),
            ("Warehouse Dir",     "warehouse"),
            ("Scripts Dir",       "scripts"),
            ("Venv Python",       "python"),
            ("Last Log",          "lastlog"),
        ]
        for label, key in status_items:
            row = ctk.CTkFrame(sys_card, fg_color="transparent")
            row.pack(fill="x", padx=16, pady=4)
            ctk.CTkLabel(row, text=label, font=("Sora", 11), text_color=C["muted"],
                         width=140, anchor="w").pack(side="left")
            b = Badge(row, "—", "neutral")
            b.pack(side="left")
            self._badges[key] = b

        ctk.CTkButton(
            sys_card, text="↺  Refresh Status", font=("Sora", 11),
            fg_color=C["border"], hover_color=C["border2"], height=32,
            command=self.refresh
        ).pack(padx=16, pady=(10, 16))

        # Data Lake info
        dl_card = Card(left)
        dl_card.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(dl_card, text="Data Lake Paths", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(dl_card).pack(fill="x", padx=16, pady=(0, 10))
        paths = [
            ("Warehouse",  str(WAREHOUSE)),
            ("Data Lake",  str(DATA_LAKE)),
            ("DuckDB",     str(DUCKDB)),
            ("Logs",       str(LOGS)),
            ("Repo",       str(REPO)),
            ("Presets",    str(PRESETS)),
        ]
        for k, v in paths:
            row = ctk.CTkFrame(dl_card, fg_color="transparent")
            row.pack(fill="x", padx=16, pady=3)
            ctk.CTkLabel(row, text=k, font=("Sora", 11), text_color=C["muted"],
                         width=100, anchor="w").pack(side="left")
            ctk.CTkLabel(row, text=v, font=("Courier New", 10), text_color="#7cbeff",
                         anchor="w").pack(side="left")
        ctk.CTkFrame(dl_card, height=12, fg_color="transparent").pack()

        self.refresh()

    def refresh(self):
        stats = bronze_stats()
        self._stat_labels["equity"].configure(text=str(stats.get("equity", 0)))
        self._stat_labels["volatility"].configure(text=str(stats.get("volatility", 0)))
        self._stat_labels["futures"].configure(text=str(stats.get("futures", 0)))
        self._stat_labels["duckdb"].configure(text=duckdb_size_str())

        ib_ok = ib_gateway_reachable()
        self._badges["ib"].set("Connected" if ib_ok else "Not reachable", "ok" if ib_ok else "warn")
        self._badges["duckdb"].set("Found" if DUCKDB.exists() else "Missing",
                                    "ok" if DUCKDB.exists() else "error")
        self._badges["warehouse"].set("✓ Exists" if WAREHOUSE.exists() else "Missing",
                                       "ok" if WAREHOUSE.exists() else "warn")
        self._badges["scripts"].set("✓ Exists" if SCRIPTS.exists() else "Missing",
                                     "ok" if SCRIPTS.exists() else "warn")
        venv_ok = _VENV_PY.exists()
        self._badges["python"].set(f"Venv" if venv_ok else "System Python",
                                    "ok" if venv_ok else "warn")
        self._badges["lastlog"].set(last_log_time(), "neutral")


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: FETCH HISTORICAL (IB)
# ═══════════════════════════════════════════════════════════════════════════════

class FetchHistoricalPage(ctk.CTkScrollableFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent", corner_radius=0)
        self._presets = load_presets()
        self._build()

    def _build(self):
        SectionHeader(self, "Fetch Historical Data",
                      "Download OHLCV bars from Interactive Brokers into bronze Parquet").pack(anchor="w", pady=(0, 20))

        cols = ctk.CTkFrame(self, fg_color="transparent")
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=1)
        cols.columnconfigure(1, weight=1)

        left  = ctk.CTkFrame(cols, fg_color="transparent")
        right = ctk.CTkFrame(cols, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right.grid(row=0, column=1, sticky="nsew")

        # ── Source card ──
        src = Card(left)
        src.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(src, text="Data Source", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(src).pack(fill="x", padx=16, pady=(0, 12))

        f = ctk.CTkFrame(src, fg_color="transparent")
        f.pack(fill="x", padx=16, pady=(0, 8))

        # Mode toggle
        ctk.CTkLabel(f, text="Mode", font=("Sora", 12), text_color=C["muted"],
                     width=185, anchor="w").pack(side="left")
        self._mode = tk.StringVar(value="preset")
        for val, lbl in [("preset", "Preset File"), ("tickers", "Manual Tickers")]:
            ctk.CTkRadioButton(f, text=lbl, variable=self._mode, value=val,
                               font=("Sora", 12), text_color=C["text"],
                               fg_color=C["acc"], command=self._on_mode).pack(side="left", padx=6)

        # Preset selector
        self._preset_row = ctk.CTkFrame(src, fg_color="transparent")
        self._preset_row.pack(fill="x", padx=16, pady=4)
        ctk.CTkLabel(self._preset_row, text="Preset File", font=("Sora", 12),
                     text_color=C["muted"], width=185, anchor="w").pack(side="left")
        preset_names = [f"{p['name']} ({p['count']} symbols)" for p in self._presets]
        self._preset_cb = ctk.CTkComboBox(
            self._preset_row, values=preset_names or ["— no presets found —"],
            width=320, fg_color=C["card2"], border_color=C["border2"],
            button_color=C["border2"], button_hover_color=C["acc"], text_color=C["text"]
        )
        self._preset_cb.pack(side="left", padx=(8, 0))
        ctk.CTkButton(self._preset_row, text="Browse", width=70, height=28,
                      font=("Sora", 11), fg_color=C["border"], hover_color=C["border2"],
                      command=self._browse_preset).pack(side="left", padx=6)

        # Manual tickers
        self._ticker_row = ctk.CTkFrame(src, fg_color="transparent")
        self._ticker_row.pack(fill="x", padx=16, pady=4)
        ctk.CTkLabel(self._ticker_row, text="Tickers (space-separated)", font=("Sora", 12),
                     text_color=C["muted"], width=185, anchor="w").pack(side="left")
        self._ticker_entry = ctk.CTkEntry(
            self._ticker_row, placeholder_text="AAPL MSFT NVDA GOOGL ...",
            width=320, fg_color=C["card2"], border_color=C["border2"], text_color=C["text"]
        )
        self._ticker_entry.pack(side="left", padx=(8, 0))
        ctk.CTkFrame(src, height=10, fg_color="transparent").pack()
        self._on_mode()

        # ── Parameters card ──
        params = Card(left)
        params.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(params, text="Parameters", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(params).pack(fill="x", padx=16, pady=(0, 12))

        pf = ctk.CTkFrame(params, fg_color="transparent")
        pf.pack(fill="x", padx=16)

        self._asset_class = labeled_combo(pf, "Asset Class",
                                           ["equity", "futures", "volatility"],
                                           default="equity")
        self._host = labeled_entry(pf, "IB Gateway Host",
                                    placeholder="127.0.0.1", default="127.0.0.1")
        self._port = labeled_entry(pf, "IB Gateway Port",
                                    placeholder="4001", default="4001")
        self._batch_size = labeled_entry(pf, "Batch Size", placeholder="50 (default)")
        self._max_concurrent = labeled_entry(pf, "Max Concurrent", placeholder="6 (default)")

        # Options flags
        ctk.CTkLabel(pf, text="Flags", font=("Sora", 12), text_color=C["muted"]).pack(anchor="w", pady=(8, 4))
        flags_row = ctk.CTkFrame(pf, fg_color="transparent")
        flags_row.pack(fill="x", pady=4)
        self._backfill = tk.IntVar(value=0)
        self._reset    = tk.IntVar(value=0)
        for label, var in [("--backfill", self._backfill), ("--reset", self._reset)]:
            ctk.CTkCheckBox(flags_row, text=label, variable=var, font=("Sora", 12),
                            text_color=C["text"], fg_color=C["acc"],
                            hover_color=C["acc_dim"]).pack(side="left", padx=(0, 16))
        ctk.CTkFrame(params, height=12, fg_color="transparent").pack()

        # ── Run / Preview ──
        run_row = ctk.CTkFrame(left, fg_color="transparent")
        run_row.pack(fill="x", pady=8)
        run_btn(run_row, "⬇  Run Fetch Historical", self._run).pack(side="left", padx=(0, 8))
        ctk.CTkButton(run_row, text="Preview Command", font=("Sora", 12),
                      fg_color=C["border"], hover_color=C["border2"], height=40,
                      command=self._preview).pack(side="left")

        # ── Command preview + info ──
        info = Card(right)
        info.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(info, text="About", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(info).pack(fill="x", padx=16, pady=(0, 10))
        about_text = (
            "Fetches full OHLCV history from IB Gateway via ib_insync.\n\n"
            "• Parallelises with async semaphore (default 6 concurrent)\n"
            "• Per-ticker cursor file for resume on interruption\n"
            "• Saves atomic bronze Parquet snapshots\n"
            "• Supports equities, futures, volatility indices\n\n"
            "Output path:\n"
            "  data-lake/bronze/asset_class=<ac>/symbol=<T>/data.parquet\n\n"
            "Requires IB Gateway or TWS running.\n"
            "Default port: 4001 (live), 4002 (paper), 7497 (TWS)."
        )
        ctk.CTkLabel(info, text=about_text, font=("Sora", 11), text_color=C["muted"],
                     justify="left", wraplength=340).pack(anchor="w", padx=16, pady=(0, 16))

        # Terminal output
        self._term = TerminalWidget(self)
        self._term.pack(fill="x", pady=(8, 0))

    def _on_mode(self):
        mode = self._mode.get()
        if mode == "preset":
            self._preset_row.pack(fill="x", padx=16, pady=4, after=self._preset_row.master.winfo_children()[0])
            self._ticker_row.pack_forget()
        else:
            self._ticker_row.pack(fill="x", padx=16, pady=4)
            self._preset_row.pack_forget()

    def _browse_preset(self):
        p = filedialog.askopenfilename(
            title="Select Preset File",
            initialdir=str(PRESETS) if PRESETS.exists() else str(REPO),
            filetypes=[("JSON", "*.json"), ("All", "*")]
        )
        if p:
            self._preset_cb.set(Path(p).name)
            self._preset_cb._custom_path = p

    def _get_preset_path(self) -> Optional[str]:
        if hasattr(self._preset_cb, "_custom_path"):
            return self._preset_cb._custom_path
        sel = self._preset_cb.get()
        for p in self._presets:
            if p["name"] in sel or sel.startswith(p["name"]):
                return p["path"]
        return None

    def _build_cmd(self) -> str:
        args = []
        if self._mode.get() == "preset":
            pp = self._get_preset_path()
            if pp:
                args += ["--preset", pp]
        else:
            t = self._ticker_entry.get().strip()
            if t:
                args += ["--tickers"] + t.split()

        ac = self._asset_class.get()
        if ac:
            args += ["--asset-class", ac]

        host = self._host.get().strip()
        port = self._port.get().strip()
        if host and host != "127.0.0.1":
            args += ["--host", host]
        if port and port != "4001":
            args += ["--port", port]

        bs = self._batch_size.get().strip()
        if bs.isdigit():
            args += ["--batch-size", bs]

        mc = self._max_concurrent.get().strip()
        if mc.isdigit():
            args += ["--max-concurrent", mc]

        if self._backfill.get():
            args.append("--backfill")
        if self._reset.get():
            args.append("--reset")

        return cmd("fetch_ib_historical.py", args)

    def _preview(self):
        c = self._build_cmd()
        self._term.write(f"Preview:\n{c}\n\n", "info")

    def _run(self):
        self._term.run(self._build_cmd())


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: DAILY UPDATE
# ═══════════════════════════════════════════════════════════════════════════════

class DailyUpdatePage(ctk.CTkScrollableFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent", corner_radius=0)
        self._presets = load_presets()
        self._build()

    def _build(self):
        SectionHeader(self, "Daily Update",
                      "Incremental daily OHLCV sync — detects and fills only missing bars").pack(anchor="w", pady=(0, 20))

        cols = ctk.CTkFrame(self, fg_color="transparent")
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=1)
        cols.columnconfigure(1, weight=1)
        left  = ctk.CTkFrame(cols, fg_color="transparent")
        right = ctk.CTkFrame(cols, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right.grid(row=0, column=1, sticky="nsew")

        # Config
        cfg = Card(left)
        cfg.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(cfg, text="Configuration", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(cfg).pack(fill="x", padx=16, pady=(0, 12))

        cf = ctk.CTkFrame(cfg, fg_color="transparent")
        cf.pack(fill="x", padx=16)

        # Preset row
        preset_row = ctk.CTkFrame(cf, fg_color="transparent")
        preset_row.pack(fill="x", pady=4)
        ctk.CTkLabel(preset_row, text="Preset (optional)", font=("Sora", 12),
                     text_color=C["muted"], width=185, anchor="w").pack(side="left")
        names = ["— auto-discover from bronze —"] + [f"{p['name']} ({p['count']} symbols)" for p in self._presets]
        self._preset_cb = ctk.CTkComboBox(
            preset_row, values=names, width=300,
            fg_color=C["card2"], border_color=C["border2"],
            button_color=C["border2"], button_hover_color=C["acc"], text_color=C["text"]
        )
        self._preset_cb.pack(side="left", padx=(8, 0))

        self._asset_class = labeled_combo(cf, "Asset Class",
                                           ["equity", "futures", "volatility"], default="equity")
        self._host = labeled_entry(cf, "IB Gateway Host", placeholder="127.0.0.1", default="127.0.0.1")
        self._port = labeled_entry(cf, "IB Gateway Port", placeholder="4001",    default="4001")
        self._max_concurrent = labeled_entry(cf, "Max Concurrent", placeholder="6 (default)")
        self._target_date = labeled_entry(cf, "Target Date (optional)", placeholder="YYYY-MM-DD")

        ctk.CTkLabel(cf, text="Flags", font=("Sora", 12), text_color=C["muted"]).pack(anchor="w", pady=(10, 4))
        flags_row = ctk.CTkFrame(cf, fg_color="transparent")
        flags_row.pack(fill="x", pady=4)
        self._dry_run = tk.IntVar(value=0)
        self._force   = tk.IntVar(value=0)
        for label, var in [("--dry-run", self._dry_run), ("--force", self._force)]:
            ctk.CTkCheckBox(flags_row, text=label, variable=var, font=("Sora", 12),
                            text_color=C["text"], fg_color=C["acc"],
                            hover_color=C["acc_dim"]).pack(side="left", padx=(0, 16))
        ctk.CTkFrame(cfg, height=12, fg_color="transparent").pack()

        # Run buttons
        run_row = ctk.CTkFrame(left, fg_color="transparent")
        run_row.pack(fill="x", pady=8)
        run_btn(run_row, "⟳  Run Daily Update", self._run).pack(side="left", padx=(0, 8))
        ctk.CTkButton(run_row, text="Preview", font=("Sora", 12),
                      fg_color=C["border"], hover_color=C["border2"], height=40,
                      command=self._preview).pack(side="left", padx=(0, 8))
        ctk.CTkButton(run_row, text="Dry Run", font=("Sora", 12),
                      fg_color=C["yellow_dim"], hover_color=C["border2"], height=40,
                      text_color=C["yellow"], command=self._dry_run_cmd).pack(side="left")

        # Right — info
        info = Card(right)
        info.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(info, text="About", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(info).pack(fill="x", padx=16, pady=(0, 10))
        about = (
            "Lightweight daily incremental sync.\n\n"
            "• Discovers tickers from existing bronze Parquet\n"
            "• Detects and fills only missing trading days\n"
            "• NYSE trading calendar awareness\n"
            "• Atomic per-ticker snapshot rewrites\n"
            "• Fallback recovery chain for IB gaps\n"
            "• Validates OHLCV after each fetch\n\n"
            "--dry-run: shows gap report without fetching\n"
            "--force:   run even on non-trading days\n"
            "--target-date: manual catch-up to specific date\n\n"
            "Scheduled via launchd at 13:05 PT (4:05 PM ET).\n"
            "Watchdog fires at 18:30 PT."
        )
        ctk.CTkLabel(info, text=about, font=("Sora", 11), text_color=C["muted"],
                     justify="left", wraplength=340).pack(anchor="w", padx=16, pady=(0, 16))

        # Also offer the "run with retries" job runner
        job = Card(right)
        job.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(job, text="Daily Update Job Runner", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(job).pack(fill="x", padx=16, pady=(0, 10))
        ctk.CTkLabel(job, text="run_daily_update_job.py — retrying runner with\n"
                               "alert emails on failure (reads MDW_* env vars).",
                     font=("Sora", 11), text_color=C["muted"],
                     justify="left").pack(anchor="w", padx=16, pady=(0, 10))
        ctk.CTkButton(
            job, text="▶  Run Job Runner", font=("Sora", 12),
            fg_color=C["border"], hover_color=C["border2"], height=36,
            command=lambda: self._term.run(f"{PYTHON} {SCRIPTS}/run_daily_update_job.py")
        ).pack(padx=16, pady=(0, 14))

        self._term = TerminalWidget(self)
        self._term.pack(fill="x", pady=(8, 0))

    def _get_preset_path(self) -> Optional[str]:
        sel = self._preset_cb.get()
        if "auto-discover" in sel:
            return None
        for p in self._presets:
            if p["name"] in sel:
                return p["path"]
        return None

    def _build_cmd(self, force_dry: bool = False) -> str:
        args = []
        pp = self._get_preset_path()
        if pp:
            args += ["--preset", pp]

        ac = self._asset_class.get()
        if ac:
            args += ["--asset-class", ac]

        host = self._host.get().strip()
        port = self._port.get().strip()
        if host and host != "127.0.0.1":
            args += ["--host", host]
        if port and port != "4001":
            args += ["--port", port]

        mc = self._max_concurrent.get().strip()
        if mc.isdigit():
            args += ["--max-concurrent", mc]

        td = self._target_date.get().strip()
        if td:
            args += ["--target-date", td]

        if self._dry_run.get() or force_dry:
            args.append("--dry-run")
        if self._force.get():
            args.append("--force")

        return cmd("daily_update.py", args)

    def _preview(self):
        self._term.write(f"Preview:\n{self._build_cmd()}\n\n", "info")

    def _run(self):
        self._term.run(self._build_cmd())

    def _dry_run_cmd(self):
        self._term.run(self._build_cmd(force_dry=True))


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: CBOE VOLATILITY
# ═══════════════════════════════════════════════════════════════════════════════

class CBOEPage(ctk.CTkScrollableFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent", corner_radius=0)
        self._build()

    def _build(self):
        SectionHeader(self, "CBOE Volatility Sync",
                      "Fetch CBOE volatility indices directly from CBOE's public API").pack(anchor="w", pady=(0, 20))

        cols = ctk.CTkFrame(self, fg_color="transparent")
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=1)
        cols.columnconfigure(1, weight=1)
        left  = ctk.CTkFrame(cols, fg_color="transparent")
        right = ctk.CTkFrame(cols, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right.grid(row=0, column=1, sticky="nsew")

        cfg = Card(left)
        cfg.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(cfg, text="Configuration", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(cfg).pack(fill="x", padx=16, pady=(0, 12))
        cf = ctk.CTkFrame(cfg, fg_color="transparent")
        cf.pack(fill="x", padx=16)

        # Mode
        ctk.CTkLabel(cf, text="Mode", font=("Sora", 12), text_color=C["muted"]).pack(anchor="w", pady=(0, 4))
        self._mode = tk.StringVar(value="default")
        mode_row = ctk.CTkFrame(cf, fg_color="transparent")
        mode_row.pack(fill="x", pady=4)
        for val, lbl in [("default", "Default (volatility preset)"), ("symbols", "Custom Symbols")]:
            ctk.CTkRadioButton(mode_row, text=lbl, variable=self._mode, value=val,
                               font=("Sora", 12), text_color=C["text"],
                               fg_color=C["acc"], command=self._on_mode).pack(side="left", padx=(0, 16))

        self._sym_row = ctk.CTkFrame(cf, fg_color="transparent")
        self._sym_row.pack(fill="x", pady=4)
        ctk.CTkLabel(self._sym_row, text="Symbols", font=("Sora", 12),
                     text_color=C["muted"], width=185, anchor="w").pack(side="left")
        self._sym_entry = ctk.CTkEntry(
            self._sym_row, placeholder_text="VIX VVIX VIX3M RVX OVX ...",
            width=300, fg_color=C["card2"], border_color=C["border2"], text_color=C["text"]
        )
        self._sym_entry.pack(side="left", padx=(8, 0))

        self._warehouse = labeled_entry(cf, "Warehouse Path (optional)",
                                         placeholder=str(WAREHOUSE), default=str(WAREHOUSE))

        ctk.CTkFrame(cfg, height=12, fg_color="transparent").pack()

        run_row = ctk.CTkFrame(left, fg_color="transparent")
        run_row.pack(fill="x", pady=8)
        run_btn(run_row, "⋯  Run CBOE Sync", self._run, color=C["green_dim"]).pack(side="left", padx=(0, 8))
        ctk.CTkButton(run_row, text="Preview", font=("Sora", 12),
                      fg_color=C["border"], hover_color=C["border2"], height=40,
                      command=self._preview).pack(side="left")

        # Default symbols
        default_vol = Card(right)
        default_vol.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(default_vol, text="Default Indices (volatility.json)", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(default_vol).pack(fill="x", padx=16, pady=(0, 10))
        indices = "VIX  VVIX  COR1M  COR3M  OVX  VXSLV  VXEEM\nVXGDX  VIX3M  VXN  RVX  VXHYG  VXSMH"
        ctk.CTkLabel(default_vol, text=indices, font=("Courier New", 12),
                     text_color="#7cbeff", justify="left").pack(anchor="w", padx=16, pady=(0, 16))

        info = Card(right)
        info.pack(fill="x")
        ctk.CTkLabel(info, text="About", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(info).pack(fill="x", padx=16, pady=(0, 10))
        ctk.CTkLabel(info, text=(
            "Primary sync source for all CBOE volatility indices.\n\n"
            "• Fetches from cdn.cboe.com public API (no auth)\n"
            "• Also handles historical backfill for VXHYG, VXSMH\n"
            "  (not available via IB)\n"
            "• Writes bronze Parquet (asset_class=volatility)\n"
            "• No IB Gateway required\n\n"
            "Runs as part of the daily update pipeline."
        ), font=("Sora", 11), text_color=C["muted"],
           justify="left", wraplength=340).pack(anchor="w", padx=16, pady=(0, 16))

        self._term = TerminalWidget(self)
        self._term.pack(fill="x", pady=(8, 0))
        self._on_mode()

    def _on_mode(self):
        if self._mode.get() == "default":
            self._sym_row.pack_forget()
        else:
            self._sym_row.pack(fill="x", pady=4)

    def _build_cmd(self) -> str:
        args = []
        if self._mode.get() == "symbols":
            syms = self._sym_entry.get().strip()
            if syms:
                args += ["--symbols"] + syms.split()
        wh = self._warehouse.get().strip()
        if wh and wh != str(WAREHOUSE):
            args += ["--warehouse", wh]
        return cmd("fetch_cboe_volatility.py", args)

    def _preview(self):
        self._term.write(f"Preview:\n{self._build_cmd()}\n\n", "info")

    def _run(self):
        self._term.run(self._build_cmd())


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: REBUILD DUCKDB
# ═══════════════════════════════════════════════════════════════════════════════

class RebuildDuckDBPage(ctk.CTkScrollableFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent", corner_radius=0)
        self._build()

    def _build(self):
        SectionHeader(self, "Rebuild DuckDB",
                      "Regenerate the analytical DuckDB database from canonical Parquet").pack(anchor="w", pady=(0, 20))

        cols = ctk.CTkFrame(self, fg_color="transparent")
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=1)
        cols.columnconfigure(1, weight=1)
        left  = ctk.CTkFrame(cols, fg_color="transparent")
        right = ctk.CTkFrame(cols, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right.grid(row=0, column=1, sticky="nsew")

        cfg = Card(left)
        cfg.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(cfg, text="Configuration", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(cfg).pack(fill="x", padx=16, pady=(0, 12))
        cf = ctk.CTkFrame(cfg, fg_color="transparent")
        cf.pack(fill="x", padx=16)

        self._asset_class = labeled_combo(cf, "Asset Class",
                                           ["equity", "volatility", "futures"], default="equity")
        self._bronze_dir  = labeled_entry(cf, "Bronze Dir (optional)",
                                           placeholder="auto-derived from asset class")
        self._db_path     = labeled_entry(cf, "DuckDB Path (optional)",
                                           placeholder=str(DUCKDB), default=str(DUCKDB))

        # Browse buttons
        browse_row = ctk.CTkFrame(cf, fg_color="transparent")
        browse_row.pack(fill="x", pady=6)
        ctk.CTkButton(browse_row, text="Browse Bronze Dir", width=160, height=28,
                      font=("Sora", 11), fg_color=C["border"], hover_color=C["border2"],
                      command=lambda: self._browse_dir(self._bronze_dir)
                      ).pack(side="left", padx=(0, 8))
        ctk.CTkButton(browse_row, text="Browse DB Path", width=150, height=28,
                      font=("Sora", 11), fg_color=C["border"], hover_color=C["border2"],
                      command=lambda: self._browse_file(self._db_path)
                      ).pack(side="left")
        ctk.CTkFrame(cfg, height=12, fg_color="transparent").pack()

        run_row = ctk.CTkFrame(left, fg_color="transparent")
        run_row.pack(fill="x", pady=8)
        run_btn(run_row, "◈  Rebuild DuckDB", self._run, color="#5a3e9e").pack(side="left", padx=(0, 8))
        ctk.CTkButton(run_row, text="Preview", font=("Sora", 12),
                      fg_color=C["border"], hover_color=C["border2"], height=40,
                      command=self._preview).pack(side="left")

        # Status
        status = Card(right)
        status.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(status, text="Current DuckDB Status", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(status).pack(fill="x", padx=16, pady=(0, 10))
        self._db_size_lbl = ctk.CTkLabel(status, text=f"Size: {duckdb_size_str()}",
                                          font=("Sora", 13), text_color=C["text"])
        self._db_size_lbl.pack(anchor="w", padx=16, pady=4)
        self._db_path_lbl = ctk.CTkLabel(status, text=str(DUCKDB), font=("Courier New", 10),
                                          text_color=C["muted"], wraplength=320)
        self._db_path_lbl.pack(anchor="w", padx=16, pady=(0, 10))
        ctk.CTkButton(status, text="↺  Refresh Size", font=("Sora", 11),
                      fg_color=C["border"], hover_color=C["border2"], height=28,
                      command=lambda: self._db_size_lbl.configure(text=f"Size: {duckdb_size_str()}")
                      ).pack(padx=16, pady=(0, 14))

        info = Card(right)
        info.pack(fill="x")
        ctk.CTkLabel(info, text="About", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(info).pack(fill="x", padx=16, pady=(0, 10))
        ctk.CTkLabel(info, text=(
            "Rebuilds md.symbols and md.equities_daily tables\n"
            "from bronze Parquet snapshots.\n\n"
            "• Drops and recreates the DuckDB tables\n"
            "• Imports all bronze symbols in one pass\n"
            "• Parquet is the system of record — DuckDB is\n"
            "  a derived analytical layer\n\n"
            "Run after large backfills or after restoring from\n"
            "a backup Parquet archive."
        ), font=("Sora", 11), text_color=C["muted"],
           justify="left").pack(anchor="w", padx=16, pady=(0, 16))

        self._term = TerminalWidget(self)
        self._term.pack(fill="x", pady=(8, 0))

    def _browse_dir(self, entry_widget):
        d = filedialog.askdirectory(title="Select Bronze Directory")
        if d:
            entry_widget.delete(0, "end")
            entry_widget.insert(0, d)

    def _browse_file(self, entry_widget):
        f = filedialog.asksaveasfilename(
            title="DuckDB Path", filetypes=[("DuckDB", "*.duckdb"), ("All", "*")]
        )
        if f:
            entry_widget.delete(0, "end")
            entry_widget.insert(0, f)

    def _build_cmd(self) -> str:
        args = []
        ac = self._asset_class.get()
        if ac:
            args += ["--asset-class", ac]
        bd = self._bronze_dir.get().strip()
        if bd:
            args += ["--bronze-dir", bd]
        dp = self._db_path.get().strip()
        if dp and dp != str(DUCKDB):
            args += ["--db-path", dp]
        return cmd("rebuild_duckdb_from_parquet.py", args)

    def _preview(self):
        self._term.write(f"Preview:\n{self._build_cmd()}\n\n", "info")

    def _run(self):
        self._term.run(self._build_cmd())


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: IBC GATEWAY
# ═══════════════════════════════════════════════════════════════════════════════

class GatewayPage(ctk.CTkScrollableFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent", corner_radius=0)
        self._build()

    def _build(self):
        SectionHeader(self, "IBC Gateway Management",
                      "Start, stop, and monitor IB Gateway via IBC or Docker").pack(anchor="w", pady=(0, 20))

        cols = ctk.CTkFrame(self, fg_color="transparent")
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=1)
        cols.columnconfigure(1, weight=1)
        left  = ctk.CTkFrame(cols, fg_color="transparent")
        right = ctk.CTkFrame(cols, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right.grid(row=0, column=1, sticky="nsew")

        # ── Connection test ──
        conn = Card(left)
        conn.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(conn, text="Connection Test", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(conn).pack(fill="x", padx=16, pady=(0, 12))
        cf = ctk.CTkFrame(conn, fg_color="transparent")
        cf.pack(fill="x", padx=16)
        self._ib_host = labeled_entry(cf, "Host", placeholder="127.0.0.1", default="127.0.0.1")
        self._ib_port = labeled_entry(cf, "Port", placeholder="4001", default="4001")
        test_row = ctk.CTkFrame(cf, fg_color="transparent")
        test_row.pack(fill="x", pady=8)
        self._conn_badge = Badge(test_row, "Not tested", "neutral")
        self._conn_badge.pack(side="left", padx=(0, 10))
        ctk.CTkButton(test_row, text="Test Connection", font=("Sora", 12),
                      fg_color=C["border"], hover_color=C["border2"], height=34,
                      command=self._test_conn).pack(side="left")
        ctk.CTkFrame(conn, height=10, fg_color="transparent").pack()

        # ── Docker Gateway ──
        dock = Card(left)
        dock.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(dock, text="Option 1 — Docker IB Gateway", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(dock).pack(fill="x", padx=16, pady=(0, 12))
        dck_dir_row = ctk.CTkFrame(dock, fg_color="transparent")
        dck_dir_row.pack(fill="x", padx=16, pady=4)
        ctk.CTkLabel(dck_dir_row, text="Compose Dir", font=("Sora", 12),
                     text_color=C["muted"], width=185, anchor="w").pack(side="left")
        self._docker_dir = ctk.CTkEntry(dck_dir_row, width=260,
                                         fg_color=C["card2"], border_color=C["border2"],
                                         text_color=C["text"])
        self._docker_dir.insert(0, str(DOCKER_GATEWAY_DIR))
        self._docker_dir.pack(side="left", padx=(8, 0))
        ctk.CTkButton(dck_dir_row, text="Browse", width=68, height=28,
                      font=("Sora", 11), fg_color=C["border"], hover_color=C["border2"],
                      command=self._browse_docker_dir).pack(side="left", padx=6)

        btn_row = ctk.CTkFrame(dock, fg_color="transparent")
        btn_row.pack(fill="x", padx=16, pady=(6, 0))
        for label, docker_cmd, color in [
            ("▶ Up",    "up -d",    C["green_dim"]),
            ("⏹ Down",  "down",     C["red_dim"]),
            ("⟳ Status","ps",       C["border"]),
            ("📋 Logs", "logs -f",  C["border"]),
        ]:
            ctk.CTkButton(
                btn_row, text=label, width=90, height=34, font=("Sora", 12),
                fg_color=color, hover_color=C["border2"], text_color=C["text"],
                command=lambda dc=docker_cmd: self._docker_cmd(dc)
            ).pack(side="left", padx=(0, 6))
        ctk.CTkFrame(dock, height=12, fg_color="transparent").pack()

        # ── IBC (macOS) ──
        ibc = Card(right)
        ibc.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(ibc, text="Option 3 — IBC (macOS Native)", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(ibc).pack(fill="x", padx=16, pady=(0, 12))

        self._ibc_ver = labeled_entry(ibc.master if False else ibc,
                                       "TWS Major Version", placeholder="10.44", default="10.44")
        # Actually use a separate frame
        ibc_cf = ctk.CTkFrame(ibc, fg_color="transparent")
        ibc_cf.pack(fill="x", padx=16)
        self._tws_ver = labeled_entry(ibc_cf, "TWS Major Version",
                                       placeholder="10.44", default="10.44")

        ibc_btn_row = ctk.CTkFrame(ibc, fg_color="transparent")
        ibc_btn_row.pack(fill="x", padx=16, pady=8)
        for label, script_name, color in [
            ("Install IBC Service", "install_ibc_secure_service.py", C["border"]),
            ("Start Gateway",       "start_ibc_gateway_keychain.py", C["green_dim"]),
        ]:
            ctk.CTkButton(
                ibc_btn_row, text=label, width=155, height=34, font=("Sora", 11),
                fg_color=color, hover_color=C["border2"], text_color=C["text"],
                command=lambda s=script_name: self._ibc_cmd(s)
            ).pack(side="left", padx=(0, 8))

        # macOS launchctl commands
        if IS_MAC:
            svc = Card(right)
            svc.pack(fill="x", pady=(0, 10))
            ctk.CTkLabel(svc, text="macOS Service (launchd)", font=("Sora", 14, "bold"),
                         text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
            Divider(svc).pack(fill="x", padx=16, pady=(0, 12))
            svc_btns = ctk.CTkFrame(svc, fg_color="transparent")
            svc_btns.pack(fill="x", padx=16, pady=(0, 14))
            for label, sc_cmd in [
                ("Load", "launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist"),
                ("Unload", "launchctl unload ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist"),
                ("Status", "launchctl list | grep market-warehouse"),
            ]:
                ctk.CTkButton(
                    svc_btns, text=label, width=90, height=32, font=("Sora", 11),
                    fg_color=C["border"], hover_color=C["border2"],
                    command=lambda c=sc_cmd: self._term.run(c)
                ).pack(side="left", padx=(0, 6))

        self._term = TerminalWidget(self)
        self._term.pack(fill="x", pady=(8, 0))

    def _test_conn(self):
        host = self._ib_host.get().strip() or "127.0.0.1"
        port = int(self._ib_port.get().strip() or "4001")
        ok = ib_gateway_reachable(host, port)
        self._conn_badge.set(f"Connected to {host}:{port}" if ok else f"No response from {host}:{port}",
                              "ok" if ok else "error")

    def _browse_docker_dir(self):
        d = filedialog.askdirectory(title="Select docker/ib-gateway Directory")
        if d:
            self._docker_dir.delete(0, "end")
            self._docker_dir.insert(0, d)

    def _docker_cmd(self, docker_subcmd: str):
        d = self._docker_dir.get().strip()
        self._term.run(f"cd '{d}' && docker compose {docker_subcmd}")

    def _ibc_cmd(self, script_name: str):
        ver = self._tws_ver.get().strip() or "10.44"
        extra = f"--tws-major-version {ver}" if "start" in script_name else ""
        self._term.run(f"{PYTHON} {SCRIPTS / script_name} {extra}".strip())


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: PRESETS BROWSER
# ═══════════════════════════════════════════════════════════════════════════════

class PresetsPage(ctk.CTkFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent")
        self._presets = load_presets()
        self._filtered = self._presets[:]
        self._selected: Optional[dict] = None
        self._build()

    def _build(self):
        SectionHeader(self, "Presets Browser",
                      "Browse all symbol universes available for data ingestion").pack(anchor="w", pady=(0, 16))

        cols = ctk.CTkFrame(self, fg_color="transparent")
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=2)
        cols.columnconfigure(1, weight=3)
        left  = ctk.CTkFrame(cols, fg_color="transparent")
        right = ctk.CTkFrame(cols, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right.grid(row=0, column=1, sticky="nsew")

        # Search
        search_row = ctk.CTkFrame(left, fg_color="transparent")
        search_row.pack(fill="x", pady=(0, 8))
        ctk.CTkLabel(search_row, text="🔍", font=("Segoe UI Emoji", 14),
                     text_color=C["muted"]).pack(side="left", padx=(0, 6))
        self._search = ctk.CTkEntry(search_row, placeholder_text="Search presets...",
                                     fg_color=C["card2"], border_color=C["border2"],
                                     text_color=C["text"])
        self._search.pack(fill="x", expand=True, side="left")
        self._search.bind("<KeyRelease>", self._filter)
        ctk.CTkLabel(left, text=f"{len(self._presets)} presets loaded",
                     font=("Sora", 10), text_color=C["muted"]).pack(anchor="w", pady=(0, 6))

        # List
        list_card = Card(left)
        list_card.pack(fill="both", expand=True)
        self._listbox = tk.Listbox(
            list_card, bg=C["card"], fg=C["text"], font=("Sora", 11),
            selectbackground=C["acc_dim"], selectforeground=C["acc"],
            relief="flat", borderwidth=0, highlightthickness=0,
            activestyle="none"
        )
        sb = tk.Scrollbar(list_card, command=self._listbox.yview,
                          bg=C["border"], troughcolor=C["bg"])
        self._listbox.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._listbox.pack(fill="both", expand=True, padx=4, pady=4)
        self._listbox.bind("<<ListboxSelect>>", self._on_select)
        self._populate_list()

        # Detail panel
        detail = Card(right)
        detail.pack(fill="both", expand=True)
        ctk.CTkLabel(detail, text="Preset Details", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(detail).pack(fill="x", padx=16, pady=(0, 10))

        self._detail_name = ctk.CTkLabel(detail, text="Select a preset →",
                                          font=("Sora", 16, "bold"), text_color=C["text"])
        self._detail_name.pack(anchor="w", padx=16, pady=(0, 4))
        self._detail_desc = ctk.CTkLabel(detail, text="", font=("Sora", 11),
                                          text_color=C["muted"], wraplength=380, justify="left")
        self._detail_desc.pack(anchor="w", padx=16, pady=(0, 8))

        meta_row = ctk.CTkFrame(detail, fg_color="transparent")
        meta_row.pack(fill="x", padx=16, pady=(0, 8))
        self._detail_count = Badge(meta_row, "0 symbols", "info")
        self._detail_count.pack(side="left", padx=(0, 8))
        self._detail_sector = Badge(meta_row, "", "neutral")
        self._detail_sector.pack(side="left")

        Divider(detail).pack(fill="x", padx=16, pady=8)

        # Action buttons
        action_row = ctk.CTkFrame(detail, fg_color="transparent")
        action_row.pack(fill="x", padx=16, pady=(0, 10))
        ctk.CTkButton(action_row, text="Copy Tickers", font=("Sora", 11), width=120, height=30,
                      fg_color=C["border"], hover_color=C["border2"],
                      command=self._copy_tickers).pack(side="left", padx=(0, 8))
        ctk.CTkButton(action_row, text="Copy Preset Path", font=("Sora", 11), width=130, height=30,
                      fg_color=C["border"], hover_color=C["border2"],
                      command=self._copy_path).pack(side="left")

        # Ticker list
        ctk.CTkLabel(detail, text="Symbols", font=("Sora", 12, "bold"),
                     text_color=C["muted"]).pack(anchor="w", padx=16, pady=(4, 4))
        self._ticker_box = tk.Text(
            detail, bg=C["card2"], fg="#7cbeff", font=("Courier New", 11),
            relief="flat", borderwidth=0, padx=12, pady=8, state="disabled",
            height=12, wrap="word"
        )
        self._ticker_box.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    def _populate_list(self):
        self._listbox.delete(0, "end")
        for p in self._filtered:
            self._listbox.insert("end", f"  {p['name']}  ({p['count']})")

    def _filter(self, *_):
        q = self._search.get().lower()
        self._filtered = [p for p in self._presets
                          if q in p["name"].lower() or q in p["description"].lower()
                          or any(q in t.lower() for t in p["tickers"][:20])]
        self._populate_list()

    def _on_select(self, *_):
        sel = self._listbox.curselection()
        if not sel:
            return
        self._selected = self._filtered[sel[0]]
        p = self._selected
        self._detail_name.configure(text=p["name"])
        self._detail_desc.configure(text=p["description"] or p.get("notes", ""))
        self._detail_count.set(f"{p['count']} symbols", "info")
        sec = p.get("sector") or p.get("source", "")
        self._detail_sector.set(sec[:28] if sec else "", "neutral")

        self._ticker_box.configure(state="normal")
        self._ticker_box.delete("1.0", "end")
        self._ticker_box.insert("1.0", "  ".join(p["tickers"]))
        self._ticker_box.configure(state="disabled")

    def _copy_tickers(self):
        if self._selected:
            self._ticker_box.clipboard_clear()
            self._ticker_box.clipboard_append(" ".join(self._selected["tickers"]))
            messagebox.showinfo("Copied", f"Copied {self._selected['count']} tickers to clipboard.")

    def _copy_path(self):
        if self._selected:
            self._ticker_box.clipboard_clear()
            self._ticker_box.clipboard_append(self._selected["path"])
            messagebox.showinfo("Copied", f"Copied preset path:\n{self._selected['path']}")


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: ENVIRONMENT CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

ENV_KEYS = [
    # (key, description, placeholder)
    ("MDW_IB_HOST",                     "IB Gateway host",               "127.0.0.1"),
    ("MDW_IB_PORT",                     "IB Gateway port",               "4001"),
    ("MDW_RADON_API_URL",               "Radon API URL (optional)",      "https://app.radon.run/api/ib"),
    ("MDW_API_KEY",                     "Radon API Key (64-char hex)",   ""),
    ("MDW_WAREHOUSE_DIR",               "Warehouse root directory",      "~/market-warehouse"),
    ("MDW_DAILY_UPDATE_MAX_ATTEMPTS",   "Max retry attempts",            "3"),
    ("MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS", "Retry delay (seconds)",    "300"),
    ("MDW_DAILY_UPDATE_LOG_DIR",        "Log directory",                 "~/market-warehouse/logs"),
    ("MDW_DAILY_UPDATE_SCRIPT",         "Override daily_update.py path", ""),
    ("MDW_NODE_BIN",                    "Node.js binary path",           "/opt/homebrew/bin/node"),
    ("MDW_ALERT_EMAIL_FROM",            "Alert sender email",            "market-warehouse@example.com"),
    ("MDW_ALERT_EMAIL_TO",              "Alert recipient email",         "you@example.com"),
    ("MDW_ALERT_EMAIL_CC",              "CC address (optional)",         ""),
    ("MDW_ALERT_SMTP_URL",              "Full SMTP URL",                 "smtp://user:pass@mail.example.com:587"),
    ("MDW_ALERT_SMTP_HOST",             "SMTP host",                     "mail.example.com"),
    ("MDW_ALERT_SMTP_PORT",             "SMTP port",                     "587"),
    ("MDW_ALERT_SMTP_USER",             "SMTP user",                     ""),
    ("MDW_ALERT_SMTP_PASS",             "SMTP password",                 ""),
    ("CEREBRAS_API_KEY_FREE",           "Cerebras free API key",         "csk-..."),
    ("CEREBRAS_API_KEY",                "Cerebras API key",              "csk-..."),
    ("MDW_CEREBRAS_MODEL",              "Cerebras model",                "gpt-oss-120b"),
]


class EnvironmentPage(ctk.CTkScrollableFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent", corner_radius=0)
        self._widgets: dict[str, ctk.CTkEntry] = {}
        self._build()

    def _build(self):
        SectionHeader(self, "Environment Configuration",
                      f"Edit .env settings — saved to {ENV_FILE}").pack(anchor="w", pady=(0, 16))

        # Action bar
        act_row = ctk.CTkFrame(self, fg_color="transparent")
        act_row.pack(fill="x", pady=(0, 16))
        ctk.CTkButton(act_row, text="↻  Reload from File", font=("Sora", 12),
                      fg_color=C["border"], hover_color=C["border2"], height=36,
                      command=self._load).pack(side="left", padx=(0, 8))
        ctk.CTkButton(act_row, text="💾  Save to File", font=("Sora", 12),
                      fg_color=C["acc"], hover_color="#2b6bcc", height=36,
                      command=self._save).pack(side="left", padx=(0, 8))
        ctk.CTkButton(act_row, text="Copy All as Export", font=("Sora", 12),
                      fg_color=C["border"], hover_color=C["border2"], height=36,
                      command=self._copy_exports).pack(side="left")
        self._save_status = ctk.CTkLabel(act_row, text="", font=("Sora", 11),
                                          text_color=C["green"])
        self._save_status.pack(side="left", padx=10)

        # Group labels and their key prefixes
        groups = [
            ("IB Gateway Connection",   ["MDW_IB_HOST", "MDW_IB_PORT", "MDW_RADON_API_URL", "MDW_API_KEY"]),
            ("Warehouse & Scheduler",   ["MDW_WAREHOUSE_DIR", "MDW_DAILY_UPDATE_MAX_ATTEMPTS",
                                         "MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS", "MDW_DAILY_UPDATE_LOG_DIR",
                                         "MDW_DAILY_UPDATE_SCRIPT", "MDW_NODE_BIN"]),
            ("Failure Alert Emails",    ["MDW_ALERT_EMAIL_FROM", "MDW_ALERT_EMAIL_TO",
                                         "MDW_ALERT_EMAIL_CC", "MDW_ALERT_SMTP_URL",
                                         "MDW_ALERT_SMTP_HOST", "MDW_ALERT_SMTP_PORT",
                                         "MDW_ALERT_SMTP_USER", "MDW_ALERT_SMTP_PASS"]),
            ("Cerebras AI (optional)",  ["CEREBRAS_API_KEY_FREE", "CEREBRAS_API_KEY",
                                         "MDW_CEREBRAS_MODEL"]),
        ]

        key_meta = {k: (d, p) for k, d, p in ENV_KEYS}
        env_data = load_env_file()

        for group_name, keys in groups:
            card = Card(self)
            card.pack(fill="x", pady=(0, 12))
            ctk.CTkLabel(card, text=group_name, font=("Sora", 14, "bold"),
                         text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
            Divider(card).pack(fill="x", padx=16, pady=(0, 10))
            f = ctk.CTkFrame(card, fg_color="transparent")
            f.pack(fill="x", padx=16)

            for key in keys:
                desc, placeholder = key_meta.get(key, ("", ""))
                row = ctk.CTkFrame(f, fg_color="transparent")
                row.pack(fill="x", pady=5)
                ctk.CTkLabel(row, text=key, font=("Courier New", 11),
                             text_color=C["acc"], width=280, anchor="w").pack(side="left")
                e = ctk.CTkEntry(row, placeholder_text=f"{desc} — {placeholder}" if placeholder else desc,
                                  width=320, fg_color=C["card2"], border_color=C["border2"],
                                  text_color=C["text"],
                                  show="*" if "PASS" in key or "KEY" in key else "")
                if key in env_data:
                    e.insert(0, env_data[key])
                e.pack(side="left", padx=(8, 0))
                self._widgets[key] = e

            ctk.CTkFrame(card, height=10, fg_color="transparent").pack()

    def _load(self):
        env_data = load_env_file()
        for key, widget in self._widgets.items():
            widget.delete(0, "end")
            if key in env_data:
                widget.insert(0, env_data[key])
        self._save_status.configure(text="✓ Reloaded")
        self.after(2500, lambda: self._save_status.configure(text=""))

    def _save(self):
        data = {k: w.get().strip() for k, w in self._widgets.items() if w.get().strip()}
        try:
            save_env_file(data)
            self._save_status.configure(text="✅ Saved!")
            self.after(2500, lambda: self._save_status.configure(text=""))
        except Exception as e:
            messagebox.showerror("Save Error", str(e))

    def _copy_exports(self):
        lines = [f'export {k}="{w.get().strip()}"'
                 for k, w in self._widgets.items() if w.get().strip()]
        self.clipboard_clear()
        self.clipboard_append("\n".join(lines))
        self._save_status.configure(text="✓ Copied exports!")
        self.after(2500, lambda: self._save_status.configure(text=""))


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: LOGS VIEWER
# ═══════════════════════════════════════════════════════════════════════════════

class LogsPage(ctk.CTkFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent")
        self._log_dir = LOGS
        self._build()

    def _build(self):
        SectionHeader(self, "Logs Viewer",
                      f"Browse and inspect log files from {LOGS}").pack(anchor="w", pady=(0, 16))

        cols = ctk.CTkFrame(self, fg_color="transparent")
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=1)
        cols.columnconfigure(1, weight=3)
        left  = ctk.CTkFrame(cols, fg_color="transparent")
        right = ctk.CTkFrame(cols, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right.grid(row=0, column=1, sticky="nsew")

        # Log directory selector
        dir_row = ctk.CTkFrame(left, fg_color="transparent")
        dir_row.pack(fill="x", pady=(0, 8))
        self._dir_entry = ctk.CTkEntry(dir_row, fg_color=C["card2"],
                                        border_color=C["border2"], text_color=C["text"])
        self._dir_entry.insert(0, str(self._log_dir))
        self._dir_entry.pack(fill="x", expand=True, side="left", padx=(0, 6))
        ctk.CTkButton(dir_row, text="Browse", width=70, height=28,
                      font=("Sora", 11), fg_color=C["border"], hover_color=C["border2"],
                      command=self._browse_dir).pack(side="left")

        ctk.CTkButton(left, text="↺  Refresh File List", font=("Sora", 11), height=30,
                      fg_color=C["border"], hover_color=C["border2"],
                      command=self._refresh_files).pack(fill="x", pady=(0, 8))

        # File list
        list_card = Card(left)
        list_card.pack(fill="both", expand=True)
        ctk.CTkLabel(list_card, text="Log Files", font=("Sora", 12, "bold"),
                     text_color=C["muted"]).pack(anchor="w", padx=12, pady=(10, 4))
        self._file_list = tk.Listbox(
            list_card, bg=C["card"], fg=C["text"], font=("Courier New", 10),
            selectbackground=C["acc_dim"], selectforeground=C["acc"],
            relief="flat", borderwidth=0, highlightthickness=0, activestyle="none"
        )
        sb = tk.Scrollbar(list_card, command=self._file_list.yview,
                          bg=C["border"], troughcolor=C["bg"])
        self._file_list.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._file_list.pack(fill="both", expand=True, padx=4, pady=(0, 4))
        self._file_list.bind("<<ListboxSelect>>", self._on_file_select)

        # Right panel — log content
        hdr_row = ctk.CTkFrame(right, fg_color="transparent")
        hdr_row.pack(fill="x", pady=(0, 8))
        self._file_label = ctk.CTkLabel(hdr_row, text="Select a log file →",
                                         font=("Sora", 12, "bold"), text_color=C["muted"])
        self._file_label.pack(side="left")
        ctk.CTkButton(hdr_row, text="↺ Reload", width=80, height=28,
                      font=("Sora", 11), fg_color=C["border"], hover_color=C["border2"],
                      command=self._reload_current).pack(side="right")
        ctk.CTkButton(hdr_row, text="Open in Editor", width=120, height=28,
                      font=("Sora", 11), fg_color=C["border"], hover_color=C["border2"],
                      command=self._open_in_editor).pack(side="right", padx=(0, 6))

        self._log_text = tk.Text(
            right, bg=C["term"], fg="#c9d1d9", font=("Courier New", 11),
            wrap="none", relief="flat", borderwidth=0, padx=12, pady=8,
            state="disabled",
        )
        sb_v = tk.Scrollbar(right, command=self._log_text.yview,
                            bg=C["border"], troughcolor=C["bg"])
        sb_h = tk.Scrollbar(right, orient="horizontal", command=self._log_text.xview,
                            bg=C["border"], troughcolor=C["bg"])
        self._log_text.configure(yscrollcommand=sb_v.set, xscrollcommand=sb_h.set)
        sb_h.pack(side="bottom", fill="x")
        sb_v.pack(side="right", fill="y")
        self._log_text.pack(fill="both", expand=True)

        self._log_text.tag_configure("err",  foreground="#f28b82")
        self._log_text.tag_configure("warn", foreground="#e3b341")
        self._log_text.tag_configure("ok",   foreground="#56d364")
        self._log_text.tag_configure("dim",  foreground=C["muted"])

        self._current_file: Optional[Path] = None
        self._refresh_files()

    def _browse_dir(self):
        d = filedialog.askdirectory(title="Select Log Directory")
        if d:
            self._log_dir = Path(d)
            self._dir_entry.delete(0, "end")
            self._dir_entry.insert(0, d)
            self._refresh_files()

    def _refresh_files(self):
        self._file_list.delete(0, "end")
        self._files: list[Path] = []
        d = Path(self._dir_entry.get().strip())
        if d.exists():
            files = sorted(d.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
            for f in files:
                sz = f.stat().st_size
                self._files.append(f)
                mt = datetime.fromtimestamp(f.stat().st_mtime).strftime("%m-%d %H:%M")
                self._file_list.insert("end", f"  {f.name}  [{mt}]")

    def _on_file_select(self, *_):
        sel = self._file_list.curselection()
        if not sel or sel[0] >= len(self._files):
            return
        self._current_file = self._files[sel[0]]
        self._load_file(self._current_file)

    def _load_file(self, path: Path):
        self._file_label.configure(text=path.name)
        self._log_text.configure(state="normal")
        self._log_text.delete("1.0", "end")
        try:
            content = path.read_text(errors="replace")
            for line in content.splitlines(keepends=True):
                lo = line.lower()
                if any(w in lo for w in ("error", "exception", "failed", "critical")):
                    self._log_text.insert("end", line, "err")
                elif "warning" in lo or "warn" in lo:
                    self._log_text.insert("end", line, "warn")
                elif any(w in lo for w in ("success", "✅", "done", "complete")):
                    self._log_text.insert("end", line, "ok")
                elif line.strip() == "" or line.startswith("#"):
                    self._log_text.insert("end", line, "dim")
                else:
                    self._log_text.insert("end", line)
            self._log_text.see("end")
        except Exception as e:
            self._log_text.insert("end", f"Error reading file: {e}")
        self._log_text.configure(state="disabled")

    def _reload_current(self):
        if self._current_file:
            self._load_file(self._current_file)

    def _open_in_editor(self):
        if self._current_file and self._current_file.exists():
            if IS_MAC:
                subprocess.Popen(["open", "-t", str(self._current_file)])
            else:
                subprocess.Popen(["xdg-open", str(self._current_file)])


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════

class SettingsPage(ctk.CTkScrollableFrame):

    def __init__(self, parent, nav_fn: Callable):
        super().__init__(parent, fg_color="transparent", corner_radius=0)
        self._build()

    def _build(self):
        SectionHeader(self, "Settings",
                      "Configure paths and defaults for the MDW GUI").pack(anchor="w", pady=(0, 20))

        # System info
        info = Card(self)
        info.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(info, text="System Information", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(info).pack(fill="x", padx=16, pady=(0, 10))
        sysinfo = [
            ("Python Binary",   PYTHON),
            ("Python Version",  sys.version.split()[0]),
            ("Repo Root",       str(REPO)),
            ("Scripts Dir",     str(SCRIPTS)),
            ("Presets Dir",     str(PRESETS)),
            ("Warehouse Dir",   str(WAREHOUSE)),
            ("OS",              f"{platform.system()} {platform.release()}"),
        ]
        for k, v in sysinfo:
            row = ctk.CTkFrame(info, fg_color="transparent")
            row.pack(fill="x", padx=16, pady=4)
            ctk.CTkLabel(row, text=k, font=("Sora", 11), text_color=C["muted"],
                         width=160, anchor="w").pack(side="left")
            ctk.CTkLabel(row, text=v, font=("Courier New", 10), text_color="#7cbeff",
                         anchor="w").pack(side="left")
        ctk.CTkFrame(info, height=10, fg_color="transparent").pack()

        # Setup script
        setup = Card(self)
        setup.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(setup, text="Setup & Install", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(setup).pack(fill="x", padx=16, pady=(0, 10))
        ctk.CTkLabel(setup, text=(
            "Run the full warehouse setup script to install all dependencies,\n"
            "create virtual environment, and optionally start ClickHouse."
        ), font=("Sora", 11), text_color=C["muted"]).pack(anchor="w", padx=16, pady=(0, 8))

        setup_row = ctk.CTkFrame(setup, fg_color="transparent")
        setup_row.pack(fill="x", padx=16, pady=(0, 6))
        self._ch = tk.IntVar(value=0)
        self._sample = tk.IntVar(value=0)
        self._smoke  = tk.IntVar(value=0)
        for label, var in [("--start-clickhouse", self._ch),
                            ("--with-sample-data", self._sample),
                            ("--smoke-test",       self._smoke)]:
            ctk.CTkCheckBox(setup_row, text=label, variable=var, font=("Sora", 11),
                            text_color=C["text"], fg_color=C["acc"],
                            hover_color=C["acc_dim"]).pack(side="left", padx=(0, 12))

        self._setup_term = TerminalWidget(setup, height=200)
        self._setup_term.pack(fill="x", padx=12, pady=(8, 12))

        ctk.CTkButton(
            setup, text="▶  Run setup_market_warehouse.sh", font=("Sora", 12),
            fg_color=C["border"], hover_color=C["border2"], height=36,
            command=self._run_setup
        ).pack(padx=16, pady=(0, 14))

        # Test suite
        test = Card(self)
        test.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(test, text="Test Suite", font=("Sora", 14, "bold"),
                     text_color=C["text"]).pack(anchor="w", padx=16, pady=(14, 8))
        Divider(test).pack(fill="x", padx=16, pady=(0, 10))
        ctk.CTkLabel(test, text="Run pytest (100% coverage enforced by project).",
                     font=("Sora", 11), text_color=C["muted"]).pack(anchor="w", padx=16, pady=(0, 8))

        self._test_term = TerminalWidget(test, height=200)
        self._test_term.pack(fill="x", padx=12, pady=(0, 8))

        test_btns = ctk.CTkFrame(test, fg_color="transparent")
        test_btns.pack(fill="x", padx=16, pady=(0, 14))
        ctk.CTkButton(test_btns, text="pytest -v", font=("Sora", 12), height=34, width=130,
                      fg_color=C["border"], hover_color=C["border2"],
                      command=lambda: self._test_term.run(
                          f"cd '{REPO}' && {PYTHON} -m pytest tests/ -v"
                      )).pack(side="left", padx=(0, 8))
        ctk.CTkButton(test_btns, text="pytest --cov", font=("Sora", 12), height=34, width=140,
                      fg_color=C["border"], hover_color=C["border2"],
                      command=lambda: self._test_term.run(
                          f"cd '{REPO}' && {PYTHON} -m pytest tests/ -v --cov=clients --cov=scripts"
                      )).pack(side="left")

    def _run_setup(self):
        args = []
        if self._ch.get():    args.append("--start-clickhouse")
        if self._sample.get(): args.append("--with-sample-data")
        if self._smoke.get():  args.append("--smoke-test")
        extra = " ".join(args)
        self._setup_term.run(f"chmod +x '{REPO / 'scripts' / 'setup_market_warehouse.sh'}' && "
                              f"bash '{REPO / 'scripts' / 'setup_market_warehouse.sh'}' {extra}".strip())


# ═══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════

class Sidebar(ctk.CTkFrame):

    def __init__(self, parent, on_select: Callable):
        super().__init__(parent, fg_color=C["sidebar"], width=220, corner_radius=0)
        self.pack_propagate(False)
        self._on_select = on_select
        self._btns: dict[str, ctk.CTkButton] = {}
        self._active: Optional[str] = None
        self._build()

    def _build(self):
        # Logo / title
        logo = ctk.CTkFrame(self, fg_color="transparent", height=72)
        logo.pack(fill="x")
        logo.pack_propagate(False)
        ctk.CTkLabel(logo, text="▸ MDW", font=("Courier New", 18, "bold"),
                     text_color=C["acc"]).pack(side="left", padx=16, pady=18)
        ctk.CTkLabel(logo, text=f"v{VERSION}", font=("Sora", 10),
                     text_color=C["dim"]).pack(side="left", pady=22)

        ctk.CTkFrame(self, height=1, fg_color=C["border"]).pack(fill="x", padx=12)

        # Nav buttons
        ctk.CTkLabel(self, text="  NAVIGATION", font=("Sora", 9, "bold"),
                     text_color=C["dim"]).pack(anchor="w", padx=16, pady=(12, 6))

        for label, icon, page_id in NAV_ITEMS:
            btn = ctk.CTkButton(
                self, text=f"  {icon}   {label}", anchor="w",
                font=("Sora", 13), height=40,
                fg_color="transparent", hover_color=C["border"],
                text_color=C["muted"], corner_radius=8,
                command=lambda p=page_id: self._select(p)
            )
            btn.pack(fill="x", padx=8, pady=1)
            self._btns[page_id] = btn

        # Bottom section
        ctk.CTkFrame(self, height=1, fg_color=C["border"]).pack(fill="x", padx=12, side="bottom", pady=8)
        ctk.CTkLabel(self, text=f"  {APP_NAME}",
                     font=("Sora", 9), text_color=C["dim"]).pack(side="bottom", anchor="w", padx=16, pady=(0, 4))

        # IB status dot
        self._ib_dot = ctk.CTkLabel(self, text="  ⬡ IB Gateway: checking...",
                                     font=("Sora", 9), text_color=C["dim"])
        self._ib_dot.pack(side="bottom", anchor="w", padx=12, pady=2)
        self._refresh_ib()

    def _select(self, page_id: str):
        if self._active:
            self._btns[self._active].configure(fg_color="transparent", text_color=C["muted"])
        self._active = page_id
        self._btns[page_id].configure(fg_color=C["acc_dim"], text_color=C["acc"])
        self._on_select(page_id)

    def select(self, page_id: str):
        self._select(page_id)

    def _refresh_ib(self):
        ok = ib_gateway_reachable()
        color = C["green"] if ok else C["yellow"]
        status = "Connected" if ok else "Offline"
        self._ib_dot.configure(text=f"  ● IB Gateway: {status}", text_color=color)
        self.after(15000, self._refresh_ib)


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

class App(ctk.CTk):

    def __init__(self):
        super().__init__()
        self.title(f"{APP_NAME} — Control Panel")
        self.geometry("1280x820")
        self.minsize(1100, 700)
        self.configure(fg_color=C["bg"])

        # Try to set custom icon
        try:
            self.iconbitmap("")  # Silence on macOS
        except Exception:
            pass

        self._pages: dict[str, ctk.CTkFrame] = {}
        self._active_page: Optional[str] = None
        self._build()

    def _build(self):
        # Sidebar
        self._sidebar = Sidebar(self, self._show_page)
        self._sidebar.pack(side="left", fill="y")

        # Main content area
        content = ctk.CTkFrame(self, fg_color="transparent", corner_radius=0)
        content.pack(side="left", fill="both", expand=True)

        # Header bar
        hdr = ctk.CTkFrame(content, fg_color=C["sidebar"], height=50, corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        self._page_title = ctk.CTkLabel(hdr, text="Dashboard",
                                         font=("Sora", 15, "bold"), text_color=C["text"])
        self._page_title.pack(side="left", padx=20, pady=12)

        # IB host indicator in header
        self._hdr_ib = ctk.CTkLabel(hdr, text=f"IB: 127.0.0.1:4001",
                                     font=("Sora", 10), text_color=C["muted"])
        self._hdr_ib.pack(side="right", padx=20)
        ctk.CTkLabel(hdr, text=f"Python: {Path(PYTHON).name}",
                     font=("Sora", 10), text_color=C["dim"]).pack(side="right", padx=(0, 8))

        # Page container
        self._container = ctk.CTkFrame(content, fg_color="transparent")
        self._container.pack(fill="both", expand=True, padx=24, pady=20)

        # Create all pages
        def nav(p):
            self._show_page(p)
            self._sidebar.select(p)

        self._pages["dashboard"] = DashboardPage(self._container, nav)
        self._pages["fetch"]     = FetchHistoricalPage(self._container, nav)
        self._pages["daily"]     = DailyUpdatePage(self._container, nav)
        self._pages["cboe"]      = CBOEPage(self._container, nav)
        self._pages["rebuild"]   = RebuildDuckDBPage(self._container, nav)
        self._pages["gateway"]   = GatewayPage(self._container, nav)
        self._pages["presets"]   = PresetsPage(self._container, nav)
        self._pages["env"]       = EnvironmentPage(self._container, nav)
        self._pages["logs"]      = LogsPage(self._container, nav)
        self._pages["settings"]  = SettingsPage(self._container, nav)

        # Hide all pages initially
        for page in self._pages.values():
            page.pack_forget()

        # Show dashboard
        self._sidebar.select("dashboard")

    def _show_page(self, page_id: str):
        if self._active_page and self._active_page in self._pages:
            self._pages[self._active_page].pack_forget()
        self._active_page = page_id
        page = self._pages[page_id]
        page.pack(fill="both", expand=True)
        # Update header title
        label = next((l for l, _, p in NAV_ITEMS if p == page_id), page_id.title())
        self._page_title.configure(text=label)


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
