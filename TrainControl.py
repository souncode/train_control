import sys
import re
import json
import os
import csv
import importlib.util
import base64
import hashlib
import queue
import random
import shutil
import zipfile
import tempfile
import threading
import subprocess
import time
import socket
import hmac
from pathlib import Path
from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

from flask import Flask, jsonify, request, render_template, abort, send_file, after_this_request, session, redirect, url_for

from notify import TelegramNotifier

# ===================== Cáº¤U HĂŒNH =====================
ROOT_DIR = Path(r"D:\Object Detection\admin")   # sá»­a láº¡i Ä‘Æ°á»ng dáº«n tháº­t cá»§a báº¡n
TRAIN_FILE = "Train_model_AI.py"
PORT = 820
HOST = "0.0.0.0"
CONTINUE_IF_ERROR = True
WAITRESS_THREADS = int(os.getenv("TRAIN_CONTROL_WAITRESS_THREADS", "16"))

TRAIN_MONITOR_HOST = "127.0.0.1"
TRAIN_MONITOR_PORT = 8008
TRAIN_MONITOR_TIMEOUT = 2

TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

PROJECT_LOG_MAX_LINES = 300
API_LOG_TAIL_LINES = 150
TRAIN_MONITOR_LOG_TAIL = 150
AUTH_SECRET_KEY = os.getenv("TRAIN_CONTROL_SECRET_KEY", "change_me_traincontrol_secret")
AUTH_SESSION_TTL_SECONDS = int(os.getenv("TRAIN_CONTROL_SESSION_TTL_SECONDS", "43200"))
LV2_PASSWORD = os.getenv("TRAIN_CONTROL_LV2_PASSWORD", "080200")
LV2_SESSION_TTL_SECONDS = int(os.getenv("TRAIN_CONTROL_LV2_SESSION_TTL_SECONDS", "1800"))
USER_FILE_NAME = "user.js"
TRAIN_HISTORY_FILE_NAME = "train_history.jsonl"
AUDIT_LOG_FILE_NAME = "audit_log.jsonl"

# Má» TRAIN TRONG TERMINAL RIĂNG
OPEN_TRAIN_IN_NEW_TERMINAL = True
# ====================================================

BASE_DIR = Path(__file__).resolve().parent

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "UI"),
    static_folder=str(BASE_DIR / "UI"),
    static_url_path="/UI"
)
app.secret_key = AUTH_SECRET_KEY
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = False

state_lock = threading.Lock()
state_cond = threading.Condition(state_lock)
train_queue = queue.Queue()
worker_thread = None
monitor_thread = None
app_runtime_initialized = False

STATE = {
    "projects": {},
    "queue": [],
    "current": None,
    "history": [],
    "worker_running": False,
    "last_scan": None,
    "version": 0,
}

MONITOR_CACHE = {
    "status_ok": False,
    "status": {},
    "history_ok": False,
    "history_state": {},
    "logs": [],
    "matched_project_name": None,
    "status_error": None,
    "status_url": None,
    "history_error": None,
    "history_url": None,
}

CURRENT_TRAIN_CONTROL = {
    "project": None,
    "project_path": "",
    "pid": None,
    "stop_requested": False,
}

NOTIFY_STATE = {
    "enabled": False
}

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".gif", ".tif", ".tiff"}
PUBLIC_PATHS = {"/login"}
DEFAULT_USER_CREDENTIALS = {
    "username": "admin",
    "password": {
        "algorithm": "pbkdf2_sha256",
        "iterations": 200000,
        "salt": "NrTlFkGs4SYsNQ6kxWuZyg==",
        "hash": "r5H5ClA1hq9LzlKTK5xqXmPcZ+5eKZNthomUmbmWZ9k=",
    }
}


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def user_file_path():
    return BASE_DIR / USER_FILE_NAME


def train_history_file_path():
    return BASE_DIR / TRAIN_HISTORY_FILE_NAME


def audit_log_file_path():
    return BASE_DIR / AUDIT_LOG_FILE_NAME


def current_auth_user() -> str:
    return str(session.get("auth_user", "") or "").strip() or "anonymous"


def append_audit_log(entry: dict):
    try:
        p = audit_log_file_path()
        line = json.dumps(entry or {}, ensure_ascii=False)
        with p.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def write_audit_log(action: str, status: str, project: str = "", target: str = "", details: str = ""):
    try:
        remote_addr = str(request.headers.get("X-Forwarded-For") or request.remote_addr or "").strip()
    except Exception:
        remote_addr = ""

    append_audit_log({
        "time": now_str(),
        "user": current_auth_user(),
        "ip": remote_addr,
        "action": str(action or "").strip(),
        "status": str(status or "").strip(),
        "project": str(project or "").strip(),
        "target": str(target or "").strip(),
        "details": str(details or "").strip(),
    })


def save_default_user_file_if_missing():
    p = user_file_path()
    if p.exists():
        return
    try:
        p.write_text(json.dumps(DEFAULT_USER_CREDENTIALS, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def append_train_history_file(entry: dict):
    try:
        p = train_history_file_path()
        line = json.dumps(entry or {}, ensure_ascii=False)
        with p.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_train_history_file(max_items: int = 500):
    p = train_history_file_path()
    if not p.exists() or not p.is_file():
        return []

    rows = []
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                raw = str(line or "").strip()
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                if isinstance(data, dict):
                    rows.append({
                        "project": str(data.get("project", "") or "").strip(),
                        "status": str(data.get("status", "") or "").strip(),
                        "time": str(data.get("time", "") or "").strip(),
                        "returncode": data.get("returncode"),
                    })
    except Exception:
        return []

    return rows[-max_items:]


def record_train_history_locked(project_name: str, status: str, returncode=None, when: str | None = None):
    entry = {
        "project": str(project_name or "").strip(),
        "status": str(status or "").strip(),
        "time": str(when or now_str()),
        "returncode": returncode,
    }
    STATE["history"].append(entry)
    STATE["history"] = STATE["history"][-500:]
    append_train_history_file(entry)
    return entry


def _parse_user_file_text(text: str):
    raw = str(text or "").strip()
    if not raw:
        return {}

    # Support both pure JSON and `module.exports = {...}` style.
    if raw.startswith("module.exports"):
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if m:
            raw = m.group(0)

    return json.loads(raw)


def load_user_credentials():
    save_default_user_file_if_missing()

    try:
        raw = user_file_path().read_text(encoding="utf-8")
        data = _parse_user_file_text(raw)
        if isinstance(data, dict):
            username = str(data.get("username", "")).strip()
            pwd = data.get("password", {})
            if username and isinstance(pwd, dict):
                algo = str(pwd.get("algorithm", "")).strip().lower()
                iterations = int(pwd.get("iterations", 0))
                salt = str(pwd.get("salt", "")).strip()
                hash_b64 = str(pwd.get("hash", "")).strip()
                if algo == "pbkdf2_sha256" and iterations > 0 and salt and hash_b64:
                    return {
                        "username": username,
                        "password": {
                            "algorithm": algo,
                            "iterations": iterations,
                            "salt": salt,
                            "hash": hash_b64,
                        }
                    }
    except Exception:
        pass

    return dict(DEFAULT_USER_CREDENTIALS)


def verify_password(password: str, creds: dict):
    try:
        pwd = (creds or {}).get("password", {})
        if str(pwd.get("algorithm", "")).lower() != "pbkdf2_sha256":
            return False
        iterations = int(pwd.get("iterations", 0))
        salt = base64.b64decode(str(pwd.get("salt", "")).encode("utf-8"))
        expected = base64.b64decode(str(pwd.get("hash", "")).encode("utf-8"))
        got = hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), salt, iterations)
        return hmac.compare_digest(got, expected)
    except Exception:
        return False


def get_next_url(default="/"):
    next_url = (request.values.get("next", "") or "").strip()
    if not next_url.startswith("/"):
        return default
    if next_url.startswith("//"):
        return default
    return next_url or default


def is_authenticated():
    if not bool(session.get("auth")):
        return False
    try:
        exp = int(session.get("auth_exp", 0))
    except Exception:
        return False
    return exp > int(time.time())


def mark_authenticated(username: str):
    session["auth"] = True
    session["auth_user"] = str(username or "").strip() or "admin"
    session["auth_exp"] = int(time.time()) + AUTH_SESSION_TTL_SECONDS


def is_lv2_authenticated():
    try:
        exp = int(session.get("auth_lv2_exp", 0))
    except Exception:
        return False
    return exp > int(time.time())


def mark_lv2_authenticated():
    session["auth_lv2"] = True
    session["auth_lv2_exp"] = int(time.time()) + LV2_SESSION_TTL_SECONDS


def clear_auth():
    session.pop("auth", None)
    session.pop("auth_user", None)
    session.pop("auth_exp", None)
    session.pop("auth_lv2", None)
    session.pop("auth_lv2_exp", None)


def require_lv2_json():
    if is_lv2_authenticated():
        return None
    return jsonify({"ok": False, "message": "LV2 password required"}), 403


def is_valid_project_name(name: str) -> bool:
    text = str(name or "").strip()
    if not text:
        return False
    if text in {".", ".."}:
        return False
    if re.search(r'[\\/:*?"<>|]', text):
        return False
    return True


def get_available_duplicate_name(source_name: str) -> str:
    base = f"{str(source_name or '').strip()} - Copy".strip()
    candidate = base
    idx = 2
    while (ROOT_DIR / candidate).exists():
        candidate = f"{base} {idx}"
        idx += 1
    return candidate


def clear_train_history_locked():
    STATE["history"] = []
    try:
        train_history_file_path().write_text("", encoding="utf-8")
    except Exception:
        pass


def clear_project_dataset_dirs(project_path: Path):
    removed = []
    for folder_name in ("runs", "test", "train", "valid"):
        target = (project_path / folder_name).resolve()
        if not is_path_inside(target, project_path):
            continue
        if target.exists() and target.is_dir():
            shutil.rmtree(target)
            removed.append(folder_name)
    return removed


def dataset_config_file_path() -> Path:
    return BASE_DIR / "dataset_config.json"


def read_shared_dataset_config():
    p = dataset_config_file_path()
    if not p.exists() or not p.is_file():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_shared_dataset_config(data: dict):
    p = dataset_config_file_path()
    p.write_text(json.dumps(data or {}, ensure_ascii=False, indent=2), encoding="utf-8")


def load_dataset_config(project_path: Path):
    defaults = {
        "train_percent": 80,
        "valid_percent": 20,
        "test_percent": 0,
        "shuffle": True,
        "seed": 42,
        "split_by_class": False,
        "train_all_data": False,
    }

    data = read_shared_dataset_config()

    if not isinstance(data, dict):
        return dict(defaults)

    try:
        cfg = dict(defaults)
        for key in ("train_percent", "valid_percent", "test_percent"):
            try:
                cfg[key] = int(data.get(key, cfg[key]))
            except Exception:
                pass
        cfg["shuffle"] = bool(data.get("shuffle", cfg["shuffle"]))
        try:
            cfg["seed"] = int(data.get("seed", cfg["seed"]))
        except Exception:
            pass
        cfg["split_by_class"] = bool(data.get("split_by_class", cfg["split_by_class"]))
        cfg["train_all_data"] = bool(data.get("train_all_data", cfg["train_all_data"]))
        return cfg
    except Exception:
        return dict(defaults)


def validate_dataset_config(cfg: dict):
    try:
        train_pct = int(cfg.get("train_percent", 0))
        valid_pct = int(cfg.get("valid_percent", 0))
        test_pct = int(cfg.get("test_percent", 0))
        seed = int(cfg.get("seed", 42))
    except Exception:
        return False, "Tỷ lệ dataset không hợp lệ", None

    shuffle_enabled = bool(cfg.get("shuffle", True))
    split_by_class = bool(cfg.get("split_by_class", False))
    train_all_data = bool(cfg.get("train_all_data", False))

    values = [train_pct, valid_pct, test_pct]
    if any(x < 0 or x > 100 for x in values):
        return False, "Tỷ lệ dataset phải nằm trong khoảng 0-100", None
    if (train_pct + valid_pct + test_pct) != 100:
        return False, "Tổng Train/Validation/Test phải bằng 100", None

    return True, None, {
        "train_percent": train_pct,
        "valid_percent": valid_pct,
        "test_percent": test_pct,
        "shuffle": shuffle_enabled,
        "seed": seed,
        "split_by_class": split_by_class,
        "train_all_data": train_all_data,
    }


def save_dataset_config(project_path: Path, cfg: dict):
    ok, err, clean_cfg = validate_dataset_config(cfg)
    if not ok:
        return False, err, None
    try:
        write_shared_dataset_config(clean_cfg)
        return True, None, clean_cfg
    except Exception as e:
        return False, f"Không lưu được dataset config: {e}", None


def bump_state_version_locked():
    STATE["version"] = int(STATE.get("version", 0)) + 1
    state_cond.notify_all()


def monitor_snapshot_signature(status_ok, status_data, history_ok, history_state, logs, status_error, history_error):
    try:
        return json.dumps({
            "status_ok": bool(status_ok),
            "status": status_data or {},
            "history_ok": bool(history_ok),
            "history_state": history_state or {},
            "logs": logs or [],
            "status_error": status_error,
            "history_error": history_error,
        }, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        return repr((status_ok, status_data, history_ok, history_state, logs, status_error, history_error))


def build_state_payload_locked():
    projects = [project_state_public(x) for x in STATE["projects"].values()]
    projects.sort(key=lambda x: (x["name"] or "").lower())

    queue_with_order = [
        {"order": idx + 1, "name": name}
        for idx, name in enumerate(STATE["queue"])
    ]

    current_name = MONITOR_CACHE.get("matched_project_name") or STATE.get("current")
    current_progress = 0.0

    if current_name and current_name in STATE["projects"]:
        try:
            current_progress = float(STATE["projects"][current_name].get("progress", 0.0) or 0.0)
        except Exception:
            current_progress = 0.0
    elif MONITOR_CACHE.get("status_ok"):
        try:
            current_progress = float((MONITOR_CACHE.get("status") or {}).get("progress", 0.0) or 0.0)
        except Exception:
            current_progress = 0.0

    return {
        "version": int(STATE.get("version", 0)),
        "projects": projects,
        "queue": queue_with_order,
        "current": current_name,
        "current_train_project": current_name,
        "current_train_progress": current_progress,
        "monitor_project_name": (MONITOR_CACHE.get("status") or {}).get("project_name"),
        "monitor_project_dir": (MONITOR_CACHE.get("status") or {}).get("project_dir"),
        "history": STATE["history"][-50:],
        "worker_running": STATE["worker_running"],
        "last_scan": STATE["last_scan"],
    }


def build_monitor_history_payload_locked():
    state_data = MONITOR_CACHE.get("history_state") or MONITOR_CACHE.get("status") or {}
    ok = bool(MONITOR_CACHE.get("status_ok") or MONITOR_CACHE.get("history_ok"))
    return {
        "ok": ok,
        "version": int(STATE.get("version", 0)),
        "state": state_data,
        "logs": list(MONITOR_CACHE.get("logs") or []),
        "error": MONITOR_CACHE.get("history_error") or MONITOR_CACHE.get("status_error"),
        "url": MONITOR_CACHE.get("history_url") or MONITOR_CACHE.get("status_url"),
    }


def build_monitor_status_payload_locked():
    return {
        "ok": bool(MONITOR_CACHE.get("status_ok")),
        "version": int(STATE.get("version", 0)),
        "data": dict(MONITOR_CACHE.get("status") or {}),
        "error": MONITOR_CACHE.get("status_error"),
        "url": MONITOR_CACHE.get("status_url"),
    }


def build_notify_state_payload_locked():
    configured = build_notifier() is not None
    enabled = bool(NOTIFY_STATE.get("enabled", False))
    return {
        "ok": True,
        "enabled": enabled,
        "configured": configured,
    }


def build_project_log_payload_locked(project: str, tail: int = API_LOG_TAIL_LINES):
    project_name = str(project or "").strip()
    if not project_name:
        return {
            "project": "",
            "log": [],
        }

    try:
        tail = max(20, min(int(tail), 500))
    except Exception:
        tail = API_LOG_TAIL_LINES

    p = STATE["projects"].get(project_name, {})
    logs = p.get("last_log", [])
    return {
        "project": project_name,
        "log": logs[-tail:],
    }


def build_snapshot_payload_locked(log_project: str = "", log_tail: int = API_LOG_TAIL_LINES):
    state_payload = build_state_payload_locked()
    log_project_name = str(log_project or "").strip() or str(state_payload.get("current") or "")
    return {
        "ok": True,
        "version": int(STATE.get("version", 0)),
        "state": state_payload,
        "monitor": build_monitor_history_payload_locked(),
        "notify": build_notify_state_payload_locked(),
        "log": build_project_log_payload_locked(log_project_name, log_tail),
    }


def safe_download_part(text: str) -> str:
    text = str(text).strip()
    text = re.sub(r'[\\/:*?"<>|]+', "_", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("._") or "file"


def build_notifier():
    token = str(TELEGRAM_BOT_TOKEN).strip()
    chat_id = str(TELEGRAM_CHAT_ID).strip()

    if not token or not chat_id or token == "YOUR_BOT_TOKEN" or chat_id == "YOUR_CHAT_ID":
        return None

    try:
        return TelegramNotifier(token=token, chat_id=chat_id)
    except Exception:
        return None


def is_notify_enabled():
    with state_lock:
        return bool(NOTIFY_STATE.get("enabled", False))


def send_telegram_notification_async(text: str):
    notifier = build_notifier()
    if not notifier:
        return False

    def _worker():
        try:
            notifier.send_message(text)
        except Exception:
            pass

    threading.Thread(target=_worker, daemon=True).start()
    return True


def notify_train_finished(project_name: str, status: str, returncode=None):
    if not is_notify_enabled():
        return

    icon = "âœ…" if status == "success" else "âŒ"
    text = (
        f"{icon} TRAIN FINISHED\n\n"
        f"Project: {project_name}\n"
        f"Status: {status.upper()}\n"
        f"Return code: {returncode}\n"
        f"Time: {now_str()}"
    )
    send_telegram_notification_async(text)


def ensure_project_state(project_name):
    if project_name not in STATE["projects"]:
        STATE["projects"][project_name] = {
            "name": project_name,
            "path": "",
            "status": "idle",   # idle | queued | running | success | failed
            "progress": 0.0,
            "last_start": None,
            "last_end": None,
            "last_returncode": None,
            "last_log": [],
            "pid": None,
        }
    return STATE["projects"][project_name]


def project_state_public(p: dict):
    return {
        "name": p.get("name"),
        "path": p.get("path"),
        "status": p.get("status"),
        "progress": p.get("progress"),
        "last_start": p.get("last_start"),
        "last_end": p.get("last_end"),
        "last_returncode": p.get("last_returncode"),
        "pid": p.get("pid"),
    }


def append_log(project_name, line, max_lines=PROJECT_LOG_MAX_LINES):
    with state_lock:
        p = ensure_project_state(project_name)
        p["last_log"].append(line.rstrip("\n"))
        if len(p["last_log"]) > max_lines:
            p["last_log"] = p["last_log"][-max_lines:]
        bump_state_version_locked()


def mark_current_train_control(project_name: str | None, project_path: Path | None = None, pid: int | None = None, stop_requested: bool = False):
    with state_lock:
        CURRENT_TRAIN_CONTROL["project"] = str(project_name or "").strip() or None
        CURRENT_TRAIN_CONTROL["project_path"] = str(project_path.resolve()) if project_path else ""
        CURRENT_TRAIN_CONTROL["pid"] = int(pid) if pid else None
        CURRENT_TRAIN_CONTROL["stop_requested"] = bool(stop_requested)


def set_current_train_pid(pid: int | None):
    with state_lock:
        CURRENT_TRAIN_CONTROL["pid"] = int(pid) if pid else None


def request_current_train_stop_flag(project_name: str):
    with state_lock:
        if str(CURRENT_TRAIN_CONTROL.get("project") or "") != str(project_name or ""):
            return False
        CURRENT_TRAIN_CONTROL["stop_requested"] = True
        return True


def is_current_train_stop_requested(project_name: str) -> bool:
    with state_lock:
        return (
            str(CURRENT_TRAIN_CONTROL.get("project") or "") == str(project_name or "") and
            bool(CURRENT_TRAIN_CONTROL.get("stop_requested"))
        )


def find_project_train_pids(project_path: Path):
    pids = set()
    if not sys.platform.startswith("win"):
        return pids

    project_path = project_path.resolve()
    train_script = str((project_path / TRAIN_FILE).resolve()).replace("'", "''")
    launcher_path = str((project_path / "_train_launcher.cmd").resolve()).replace("'", "''")
    command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -and "
        f"(($_.CommandLine -like '*{train_script}*') -or ($_.CommandLine -like '*{launcher_path}*')) }} | "
        "Select-Object -ExpandProperty ProcessId"
    )
    try:
        out = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command", command],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        for line in str(out or "").splitlines():
            s = str(line or "").strip()
            if s.isdigit():
                pids.add(int(s))
    except Exception:
        pass
    return pids


def kill_pid_tree(pid: int):
    try:
        result = subprocess.run(
            ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return result.returncode == 0, (result.stdout or result.stderr or "").strip()
    except Exception as e:
        return False, str(e)


def stop_current_train():
    with state_lock:
        project_name = str(STATE.get("current") or "").strip()
        project_info = STATE["projects"].get(project_name) if project_name else None
        tracked_pid = CURRENT_TRAIN_CONTROL.get("pid")
        if not project_name or not project_info:
            return False, "No running project", {"project": None, "killed_pids": []}
        project_path = Path(project_info.get("path") or "")
        CURRENT_TRAIN_CONTROL["stop_requested"] = True

    append_log(project_name, f"[{now_str()}] STOP REQUESTED")

    killed_pids = []
    errors = []
    pid_candidates = set()
    if tracked_pid:
        pid_candidates.add(int(tracked_pid))
    pid_candidates.update(find_project_train_pids(project_path))

    for pid in sorted(pid_candidates):
        ok, info = kill_pid_tree(pid)
        if ok:
            killed_pids.append(pid)
        elif info:
            errors.append(f"{pid}: {info}")

    details = {
        "project": project_name,
        "killed_pids": killed_pids,
        "errors": errors,
    }
    if killed_pids:
        return True, f"Stop requested for {project_name} (killed {len(killed_pids)} process(es))", details
    return True, f"Stop requested for {project_name}", details


def get_train_template_path() -> Path:
    return BASE_DIR / TRAIN_FILE


def ensure_train_script_in_project(project_path: Path):
    source_path = get_train_template_path()
    if not source_path.exists() or not source_path.is_file():
        return False, f"Không tìm thấy file nguồn {TRAIN_FILE} tại {source_path}"

    target_path = project_path / TRAIN_FILE
    try:
        shutil.copy2(source_path, target_path)
    except Exception as e:
        return False, f"Không copy được {TRAIN_FILE} vào project: {e}"

    return True, str(target_path)


def find_project_data_yaml(project_path: Path):
    for file_name in ("data.yaml", "data.yml"):
        candidate = project_path / file_name
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def create_train_launcher_cmd(project_path: Path, python_exe: str, script_file: str) -> Path:
    launcher_path = project_path / "_train_launcher.cmd"
    content = "\n".join([
        "@echo off",
        "title TrainControl Runner",
        f'cd /d "{project_path}"',
        f'"{python_exe}" "{script_file}"',
        "set RC=%ERRORLEVEL%",
        "if not \"%RC%\"==\"0\" (",
        "  echo.",
        "  echo Train failed with RC=%RC%",
        "  pause",
        ")",
        "exit /b %RC%",
        ""
    ])
    launcher_path.write_text(content, encoding="utf-8")
    return launcher_path


def cleanup_project_train_runtime_files(project_path: Path):
    removed = []
    errors = []

    for file_name in (TRAIN_FILE, "_train_launcher.cmd"):
        target = (project_path / file_name).resolve()
        if not is_path_inside(target, project_path):
            continue
        if not target.exists() or not target.is_file():
            continue
        try:
            target.unlink()
            removed.append(target.name)
        except Exception as e:
            errors.append(f"{target.name}: {e}")

    return removed, errors


def get_projects(root_dir: Path):
    projects = []
    if not root_dir.exists():
        return projects

    for item in root_dir.iterdir():
        if item.is_dir():
            projects.append(item)

    projects.sort(key=lambda x: x.name.lower())
    return projects


def scan_projects():
    projects = get_projects(ROOT_DIR)
    with state_lock:
        known_names = set()
        for p in projects:
            known_names.add(p.name)
            info = ensure_project_state(p.name)
            info["path"] = str(p)

        for name, info in STATE["projects"].items():
            if name not in known_names and info.get("status") in ("idle", "success", "failed"):
                info["path"] = ""

        STATE["last_scan"] = now_str()
        bump_state_version_locked()

    return projects


def queue_project(project_name):
    with state_lock:
        p = ensure_project_state(project_name)
        if not p["path"]:
            return False, f"Project not found: {project_name}"
        if p["status"] == "running":
            return False, f"{project_name} is training"
        if project_name in STATE["queue"]:
            return False, f"{project_name} is already queued"

        STATE["queue"].append(project_name)
        p["status"] = "queued"
        p["progress"] = 0.0
        bump_state_version_locked()

    train_queue.put(project_name)
    start_worker_if_needed()
    return True, f"Added {project_name} to queue"


def queue_projects(project_names):
    added = []
    skipped = []

    for name in project_names:
        ok, _ = queue_project(name)
        if ok:
            added.append(name)
        else:
            skipped.append(name)

    return added, skipped


def queue_all():
    added = []
    skipped = []

    with state_lock:
        names = sorted(
            [name for name, info in STATE["projects"].items() if info["path"]],
            key=str.lower
        )

    for name in names:
        ok, _ = queue_project(name)
        if ok:
            added.append(name)
        else:
            skipped.append(name)

    return added, skipped


def retry_failed_projects():
    with state_lock:
        failed_projects = sorted(
            [
                name for name, info in STATE["projects"].items()
                if info.get("status") == "failed" and info.get("path")
            ],
            key=str.lower
        )

    added, skipped = queue_projects(failed_projects)
    return added, skipped


def stop_pending_queue():
    cleared = []

    with state_lock:
        queued_names = list(STATE["queue"])
        STATE["queue"].clear()

        for name in queued_names:
            p = ensure_project_state(name)
            if p["status"] == "queued":
                p["status"] = "idle"
                p["progress"] = 0.0

        cleared = queued_names
        bump_state_version_locked()

    while True:
        try:
            train_queue.get_nowait()
            try:
                train_queue.task_done()
            except Exception:
                pass
        except queue.Empty:
            break
        except Exception:
            break

    return cleared


def fetch_train_monitor_json(path: str):
    url = f"http://{TRAIN_MONITOR_HOST}:{TRAIN_MONITOR_PORT}{path}"
    try:
        with urlopen(url, timeout=TRAIN_MONITOR_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
            return True, json.loads(raw)
    except (URLError, HTTPError, TimeoutError, json.JSONDecodeError, OSError) as e:
        return False, {"error": str(e), "url": url}


def count_images_in_dir(folder: Path) -> int:
    if not folder.exists() or not folder.is_dir():
        return 0
    try:
        return sum(
            1 for x in folder.rglob("*")
            if x.is_file() and x.suffix.lower() in IMAGE_EXTS
        )
    except Exception:
        return 0


def resolve_project_path(project_name: str):
    with state_lock:
        info = STATE["projects"].get(project_name)
        if not info or not info.get("path"):
            return None
        try:
            return Path(info["path"]).resolve()
        except Exception:
            return None


def find_project_image_dir(project_path: Path):
    for folder_name in ("image", "images"):
        p = project_path / folder_name
        if p.exists() and p.is_dir():
            return p
    return None


def list_project_images(project_path: Path, limit: int = 0):
    image_dir = find_project_image_dir(project_path)
    if not image_dir:
        return None, []

    rows = []
    try:
        for f in image_dir.rglob("*"):
            if f.is_file() and f.suffix.lower() in IMAGE_EXTS:
                rows.append(f.relative_to(image_dir).as_posix())
    except Exception:
        return image_dir, []

    rows.sort(key=str.lower)
    if limit > 0:
        rows = rows[:limit]
    return image_dir, rows


def resolve_project_image_file(project_name: str, rel_path: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return None, "project_not_found"

    image_dir = find_project_image_dir(project_path)
    if not image_dir:
        return None, "image_dir_not_found"

    rel = str(rel_path or "").strip().replace("\\", "/")
    if not rel:
        return None, "missing_rel_path"

    candidate = (image_dir / rel).resolve()
    if not is_path_inside(candidate, image_dir):
        return None, "invalid_path"
    if not candidate.exists() or not candidate.is_file():
        return None, "file_not_found"
    if candidate.suffix.lower() not in IMAGE_EXTS:
        return None, "not_image_file"

    return candidate, None


def get_project_image_info(project_name: str, rel_path: str):
    img_file, err = resolve_project_image_file(project_name, rel_path)
    if err or not img_file:
        return None, err or "file_not_found"

    try:
        st = img_file.stat()
        root_link = str(img_file)
        try:
            root_link = (img_file.resolve()).relative_to(ROOT_DIR.resolve()).as_posix()
            root_link = f"{ROOT_DIR.as_posix()}/{root_link}"
        except Exception:
            root_link = str(img_file)

        info = {
            "name": img_file.name,
            "rel": rel_path,
            "path": str(img_file),
            "root_link": root_link,
            "modified_at": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "size_kb": round(st.st_size / 1024, 2),
        }
        return info, None
    except Exception:
        return None, "cannot_read_file_info"


def resolve_label_file_for_image(project_name: str, rel_path: str, create_missing: bool = False):
    img_file, err = resolve_project_image_file(project_name, rel_path)
    if err or not img_file:
        return None, err

    project_path = resolve_project_path(project_name)
    image_dir = find_project_image_dir(project_path)
    if not project_path or not image_dir:
        return None, "project_not_found"

    rel_img = img_file.relative_to(image_dir)
    rel_txt = rel_img.with_suffix(".txt")

    candidates = [
        image_dir / rel_txt,
        image_dir / "labels" / rel_txt,
        project_path / "labels" / rel_txt,
    ]

    for c in candidates:
        if c.exists() and c.is_file():
            return c, None

    if not create_missing:
        return candidates[-1], None

    target = candidates[-1]
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        return None, "cannot_create_label_dir"
    return target, None


def extract_class_ids_from_label_file(label_file: Path):
    if not label_file or not label_file.exists() or not label_file.is_file():
        return []
    ids = []
    try:
        raw = label_file.read_text(encoding="utf-8")
    except Exception:
        return []

    for line in raw.replace("\r\n", "\n").split("\n"):
        s = line.strip()
        if not s:
            continue
        parts = s.split()
        if not parts:
            continue
        try:
            cid = int(float(parts[0]))
        except Exception:
            continue
        ids.append(cid)

    # unique + sorted
    return sorted(set(ids))


def load_project_classes(project_path: Path):
    candidates = [
        project_path / "class.txt",
        project_path / "class,txt",
        project_path / "classes.txt",
        project_path / "class.csv",
        project_path / "image" / "class.txt",
        project_path / "image" / "class,txt",
        project_path / "image" / "classes.txt",
        project_path / "images" / "class.txt",
        project_path / "images" / "class,txt",
        project_path / "images" / "classes.txt",
    ]
    class_file = None
    for p in candidates:
        if p.exists() and p.is_file():
            class_file = p
            break

    if not class_file:
        # Fallback: read names from data.yaml if available.
        data_yaml = project_path / "data.yaml"
        if data_yaml.exists() and data_yaml.is_file():
            try:
                import yaml  # lazy import to avoid hard dependency at app startup
                data = yaml.safe_load(data_yaml.read_text(encoding="utf-8")) or {}
                names = data.get("names", {})
                if isinstance(names, list):
                    classes = [str(x).strip() for x in names if str(x).strip()]
                    return classes, str(data_yaml)
                if isinstance(names, dict):
                    rows = []
                    for k in sorted(names.keys(), key=lambda x: int(x) if str(x).isdigit() else str(x)):
                        v = str(names.get(k, "")).strip()
                        if v:
                            rows.append(v)
                    return rows, str(data_yaml)
            except Exception:
                pass
        return [], None

    try:
        raw = class_file.read_text(encoding="utf-8")
    except Exception:
        return [], str(class_file)

    rows = []
    for line in raw.replace("\r\n", "\n").split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = [x.strip() for x in line.split(",") if x.strip()]
        if len(parts) == 1:
            rows.append(parts[0])
        else:
            rows.extend(parts)

    # de-duplicate while preserving order
    seen = set()
    classes = []
    for x in rows:
        if x in seen:
            continue
        seen.add(x)
        classes.append(x)

    return classes, str(class_file)


def collect_dataset_source_rows(project_name: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Không tìm thấy project", None, None

    image_dir = find_project_image_dir(project_path)
    if not image_dir:
        return False, "Không tìm thấy thư mục image/images", None, None

    _, rel_images = list_project_images(project_path, limit=0)
    image_rows = []
    for rel in rel_images:
        label_file, _ = resolve_label_file_for_image(project_name, rel, create_missing=False)
        if not label_file or not label_file.exists() or not label_file.is_file():
            continue
        class_ids = extract_class_ids_from_label_file(label_file)
        image_rows.append({
            "rel": rel,
            "image_file": (image_dir / rel).resolve(),
            "label_file": label_file.resolve(),
            "class_ids": class_ids,
            "primary_class": class_ids[0] if class_ids else -1,
        })

    if not image_rows:
        return False, "Không có cặp image/label hợp lệ để tạo dataset", None, None

    image_rows.sort(key=lambda x: str(x["rel"]).lower())
    return True, None, project_path, image_rows


def split_dataset_rows_by_count(image_rows: list, clean_cfg: dict):
    rows = list(image_rows)
    if clean_cfg.get("shuffle", True):
        random.Random(int(clean_cfg.get("seed", 42))).shuffle(rows)

    total = len(rows)
    train_count = int(round(total * clean_cfg["train_percent"] / 100.0))
    valid_count = int(round(total * clean_cfg["valid_percent"] / 100.0))
    train_count = max(0, min(train_count, total))
    valid_count = max(0, min(valid_count, total - train_count))

    return {
        "train": rows[:train_count],
        "valid": rows[train_count:train_count + valid_count],
        "test": rows[train_count + valid_count:],
    }


def split_dataset_rows_by_class(image_rows: list, clean_cfg: dict):
    groups = {}
    for row in image_rows:
        key = int(row.get("primary_class", -1))
        groups.setdefault(key, []).append(row)

    class_keys = sorted(groups.keys())
    if clean_cfg.get("shuffle", True):
        random.Random(int(clean_cfg.get("seed", 42))).shuffle(class_keys)

    total_groups = len(class_keys)
    train_group_count = int(round(total_groups * clean_cfg["train_percent"] / 100.0))
    valid_group_count = int(round(total_groups * clean_cfg["valid_percent"] / 100.0))
    train_group_count = max(0, min(train_group_count, total_groups))
    valid_group_count = max(0, min(valid_group_count, total_groups - train_group_count))

    split_map = {
        "train": class_keys[:train_group_count],
        "valid": class_keys[train_group_count:train_group_count + valid_group_count],
        "test": class_keys[train_group_count + valid_group_count:],
    }

    buckets = {"train": [], "valid": [], "test": []}
    for split_name, keys in split_map.items():
        for key in keys:
            buckets[split_name].extend(groups.get(key, []))

    return buckets


def create_dataset_for_project(project_name: str, cfg: dict, split_mode: str = "count"):
    ok, err, clean_cfg = validate_dataset_config(cfg)
    if not ok:
        return False, err, None

    ok, err, project_path, image_rows = collect_dataset_source_rows(project_name)
    if not ok:
        return False, err, None

    split_mode = str(split_mode or "count").strip().lower()
    if split_mode == "class":
        buckets = split_dataset_rows_by_class(image_rows, clean_cfg)
    else:
        buckets = split_dataset_rows_by_count(image_rows, clean_cfg)

    clear_project_dataset_dirs(project_path)

    created_counts = {}
    for split_name, rows in buckets.items():
        if not rows:
            created_counts[split_name] = 0
            continue

        img_dst_root = project_path / split_name / "images"
        lbl_dst_root = project_path / split_name / "labels"
        img_dst_root.mkdir(parents=True, exist_ok=True)
        lbl_dst_root.mkdir(parents=True, exist_ok=True)

        count = 0
        for row in rows:
            rel_path = Path(row["rel"])
            img_dst = img_dst_root / rel_path
            lbl_dst = (lbl_dst_root / rel_path).with_suffix(".txt")
            img_dst.parent.mkdir(parents=True, exist_ok=True)
            lbl_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(row["image_file"], img_dst)
            shutil.copy2(row["label_file"], lbl_dst)
            count += 1
        created_counts[split_name] = count

    classes, _ = load_project_classes(project_path)
    data_yaml = {
        "path": str(project_path),
        "train": "train/images",
        "val": "valid/images",
        "test": "test/images",
        "nc": len(classes),
        "names": classes,
    }
    try:
        import yaml
        (project_path / "data.yaml").write_text(
            yaml.safe_dump(data_yaml, allow_unicode=True, sort_keys=False),
            encoding="utf-8"
        )
    except Exception as e:
        return False, f"Không ghi được data.yaml: {e}", None

    ok, err, saved_cfg = save_dataset_config(project_path, clean_cfg)
    if not ok:
        return False, err, None

    return True, "Tạo dataset thành công", {
        "config": saved_cfg,
        "counts": created_counts,
        "total": len(image_rows),
        "split_mode": split_mode,
    }


def merge_train_valid_to_train(project_name: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Không tìm thấy project", None

    source_rows = []
    for split_name in ("train", "valid"):
        img_root = project_path / split_name / "images"
        lbl_root = project_path / split_name / "labels"
        if not img_root.exists() or not img_root.is_dir():
            continue
        for img_file in img_root.rglob("*"):
            if not img_file.is_file() or img_file.suffix.lower() not in IMAGE_EXTS:
                continue
            rel_path = img_file.relative_to(img_root)
            lbl_file = (lbl_root / rel_path).with_suffix(".txt")
            if not lbl_file.exists() or not lbl_file.is_file():
                continue
            source_rows.append({
                "rel": rel_path,
                "image_file": img_file.resolve(),
                "label_file": lbl_file.resolve(),
            })

    if not source_rows:
        return False, "Không có dữ liệu train/valid để gộp", None

    train_root = project_path / "train"
    if train_root.exists() and train_root.is_dir():
        shutil.rmtree(train_root)

    img_dst_root = train_root / "images"
    lbl_dst_root = train_root / "labels"
    img_dst_root.mkdir(parents=True, exist_ok=True)
    lbl_dst_root.mkdir(parents=True, exist_ok=True)

    copied = 0
    for row in source_rows:
        rel_path = Path(row["rel"])
        img_dst = img_dst_root / rel_path
        lbl_dst = (lbl_dst_root / rel_path).with_suffix(".txt")
        img_dst.parent.mkdir(parents=True, exist_ok=True)
        lbl_dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(row["image_file"], img_dst)
        shutil.copy2(row["label_file"], lbl_dst)
        copied += 1

    return True, "Đã gộp train + valid vào train", {
        "counts": {
            "train": copied,
            "valid": count_images_in_dir(project_path / "valid" / "images"),
            "test": count_images_in_dir(project_path / "test" / "images"),
        }
    }


def is_path_inside(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def find_output_dir(project_path: Path):
    for folder_name in ("Output", "output"):
        p = project_path / folder_name
        if p.exists() and p.is_dir():
            return p
    return None


def resolve_uploaded_project_dir(extract_root: Path):
    try:
        items = list(extract_root.iterdir())
    except Exception:
        return None, "cannot_scan_extracted_content"

    if not items:
        return None, "project_not_found"

    top_level_dirs = [item for item in items if item.is_dir()]
    top_level_files = [item for item in items if item.is_file()]

    if len(top_level_dirs) == 1 and not top_level_files:
        return top_level_dirs[0], None

    return extract_root, None


def save_uploaded_project_zip(upload_file):
    if not upload_file:
        return False, "missing_file", None

    filename = str(upload_file.filename or "").strip()
    if not filename.lower().endswith(".zip"):
        return False, "only_zip_supported", None

    with tempfile.TemporaryDirectory(prefix="traincontrol_upload_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        zip_path = tmp_dir_path / "upload.zip"
        extract_root = tmp_dir_path / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)

        upload_file.save(str(zip_path))

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                infos = zf.infolist()
                if not infos:
                    return False, "zip_empty", None

                for info in infos:
                    name = str(info.filename or "").replace("\\", "/")
                    parts = Path(name).parts
                    if name.startswith("/") or ".." in parts:
                        return False, "invalid_zip_path", None

                zf.extractall(path=str(extract_root))
        except zipfile.BadZipFile:
            return False, "bad_zip_file", None
        except Exception:
            return False, "extract_failed", None

        candidate, err = resolve_uploaded_project_dir(extract_root)
        if err or not candidate:
            return False, err or "project_not_found", None

        suggested_name = candidate.name
        if candidate == extract_root:
            suggested_name = Path(filename).stem

        target_name = safe_download_part(suggested_name)
        target_dir = ROOT_DIR / target_name

        if target_dir.exists():
            return False, "project_already_exists", target_name

        try:
            shutil.copytree(candidate, target_dir)
        except Exception:
            return False, "copy_failed", target_name

    return True, "uploaded", target_name


def get_output_model_train_weights_info(project_path: Path):
    output_dir = find_output_dir(project_path)
    if not output_dir:
        return {
            "exists": False,
            "folder": None,
            "model_train_folders": [],
            "items": []
        }

    model_train_dirs = [
        d for d in sorted(output_dir.iterdir(), key=lambda p: p.name.lower())
        if d.is_dir() and d.name.lower().startswith("model_train")
    ]

    rows = []
    for run_dir in model_train_dirs:
        for f in run_dir.rglob("*"):
            if f.is_file():
                if f.suffix.lower() in IMAGE_EXTS:
                    continue
                st = f.stat()
                rows.append({
                    "run_folder": run_dir.name,
                    "file_name": f.name,
                    "relative_path": f.relative_to(output_dir).as_posix(),
                    "modified_at": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    "size_kb": round(st.st_size / 1024, 2),
                    "_sort_ts": st.st_mtime
                })

    rows.sort(key=lambda x: x["_sort_ts"], reverse=True)
    for row in rows:
        row.pop("_sort_ts", None)

    return {
        "exists": True,
        "folder": output_dir.name,
        "model_train_folders": [d.name for d in model_train_dirs],
        "items": rows
    }


def read_results_csv_preview(csv_file: Path, max_rows: int = 200):
    if not csv_file.exists() or not csv_file.is_file():
        return {
            "exists": False,
            "columns": [],
            "rows": [],
            "row_count": 0,
        }

    rows = []
    columns = []
    total = 0
    try:
        with csv_file.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            columns = list(reader.fieldnames or [])
            for row in reader:
                total += 1
                if len(rows) < max_rows:
                    rows.append({k: str(v or "") for k, v in (row or {}).items()})
    except UnicodeDecodeError:
        try:
            with csv_file.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                columns = list(reader.fieldnames or [])
                for row in reader:
                    total += 1
                    if len(rows) < max_rows:
                        rows.append({k: str(v or "") for k, v in (row or {}).items()})
        except Exception:
            return {
                "exists": True,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "error": "cannot_read_results_csv",
            }
    except Exception:
        return {
            "exists": True,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "error": "cannot_read_results_csv",
        }

    return {
        "exists": True,
        "columns": columns,
        "rows": rows,
        "row_count": total,
    }


def _to_float_or_none(value):
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _fmt_pct_text(value, scale: float = 100.0):
    num = _to_float_or_none(value)
    if num is None:
        return None
    return f"{num * scale:.1f}%"


def summarize_results_csv(csv_file: Path):
    if not csv_file.exists() or not csv_file.is_file():
        return {
            "exists": False,
        }

    try:
        with csv_file.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except UnicodeDecodeError:
        try:
            with csv_file.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception:
            return {"exists": True, "error": "cannot_read_results_csv"}
    except Exception:
        return {"exists": True, "error": "cannot_read_results_csv"}

    if not rows:
        return {"exists": True, "error": "results_csv_empty"}

    best_row = None
    best_score = -1.0
    latest_row = rows[-1]

    for row in rows:
        score = _to_float_or_none(row.get("metrics/mAP50-95(B)"))
        if score is None:
            continue
        if score > best_score:
            best_score = score
            best_row = row

    target_row = best_row or latest_row
    latest_precision = _to_float_or_none(latest_row.get("metrics/precision(B)"))
    latest_recall = _to_float_or_none(latest_row.get("metrics/recall(B)"))
    latest_map50 = _to_float_or_none(latest_row.get("metrics/mAP50(B)"))
    latest_map5095 = _to_float_or_none(latest_row.get("metrics/mAP50-95(B)"))
    precision = _to_float_or_none(target_row.get("metrics/precision(B)"))
    recall = _to_float_or_none(target_row.get("metrics/recall(B)"))
    map50 = _to_float_or_none(target_row.get("metrics/mAP50(B)"))
    map5095 = _to_float_or_none(target_row.get("metrics/mAP50-95(B)"))
    best_val_box_loss = _to_float_or_none(target_row.get("val/box_loss"))
    best_val_cls_loss = _to_float_or_none(target_row.get("val/cls_loss"))
    best_val_dfl_loss = _to_float_or_none(target_row.get("val/dfl_loss"))
    best_train_box_loss = _to_float_or_none(target_row.get("train/box_loss"))
    best_train_cls_loss = _to_float_or_none(target_row.get("train/cls_loss"))
    best_train_dfl_loss = _to_float_or_none(target_row.get("train/dfl_loss"))
    train_box_loss = _to_float_or_none(latest_row.get("train/box_loss"))
    train_cls_loss = _to_float_or_none(latest_row.get("train/cls_loss"))
    train_dfl_loss = _to_float_or_none(latest_row.get("train/dfl_loss"))
    val_box_loss = _to_float_or_none(latest_row.get("val/box_loss"))
    val_cls_loss = _to_float_or_none(latest_row.get("val/cls_loss"))
    val_dfl_loss = _to_float_or_none(latest_row.get("val/dfl_loss"))

    quality_label = "Unknown"
    quality_score = map5095 if map5095 is not None else -1.0
    if quality_score >= 0.75:
        quality_label = "Excellent"
    elif quality_score >= 0.6:
        quality_label = "Good"
    elif quality_score >= 0.4:
        quality_label = "Fair"
    elif quality_score >= 0:
        quality_label = "Weak"

    analysis_notes = []
    precision_gap = None
    localization_gap = None
    latest_train_total_loss = None
    latest_val_total_loss = None
    best_train_total_loss = None
    best_val_total_loss = None
    loss_gap = None
    best_epoch = int(float(target_row.get("epoch", len(rows)) or len(rows)))
    latest_epoch = int(float(latest_row.get("epoch", len(rows)) or len(rows)))

    if None not in (train_box_loss, train_cls_loss, train_dfl_loss):
        latest_train_total_loss = train_box_loss + train_cls_loss + train_dfl_loss
    if None not in (val_box_loss, val_cls_loss, val_dfl_loss):
        latest_val_total_loss = val_box_loss + val_cls_loss + val_dfl_loss
    if None not in (best_train_box_loss, best_train_cls_loss, best_train_dfl_loss):
        best_train_total_loss = best_train_box_loss + best_train_cls_loss + best_train_dfl_loss
    if None not in (best_val_box_loss, best_val_cls_loss, best_val_dfl_loss):
        best_val_total_loss = best_val_box_loss + best_val_cls_loss + best_val_dfl_loss
    if latest_train_total_loss is not None and latest_val_total_loss is not None:
        loss_gap = latest_val_total_loss - latest_train_total_loss

    if precision is not None and recall is not None:
        precision_gap = precision - recall
        if precision_gap >= 0.12:
            analysis_notes.append({
                "severity": "warn",
                "title": "Low recall",
                "message": f"Based on best validation metrics: precision={_fmt_pct_text(precision)}, recall={_fmt_pct_text(recall)}, gap={_fmt_pct_text(precision_gap)}. The model is finding correct boxes when it predicts, but it is still missing many true objects.",
            })
        elif precision_gap <= -0.12:
            analysis_notes.append({
                "severity": "warn",
                "title": "Low precision",
                "message": f"Based on best validation metrics: recall={_fmt_pct_text(recall)}, precision={_fmt_pct_text(precision)}, gap={_fmt_pct_text(-precision_gap)}. The model is detecting many objects, but too many predictions are wrong.",
            })

    if map50 is not None and map5095 is not None:
        localization_gap = map50 - map5095
        if localization_gap >= 0.18:
            analysis_notes.append({
                "severity": "warn",
                "title": "Localization gap",
                "message": f"Based on validation mAP: mAP50={_fmt_pct_text(map50)} but mAP50-95={_fmt_pct_text(map5095)}, gap={_fmt_pct_text(localization_gap)}. Class prediction is acceptable, but box localization drops under stricter IoU thresholds.",
            })

    if map5095 is not None and latest_map5095 is not None and best_epoch < latest_epoch:
        best_is_early = best_epoch <= max(3, int(len(rows) * 0.7))
        metric_drop = map5095 - latest_map5095
        val_loss_worse = (
            best_val_box_loss is not None and val_box_loss is not None and val_box_loss > (best_val_box_loss + 0.02)
        ) or (
            best_val_cls_loss is not None and val_cls_loss is not None and val_cls_loss > (best_val_cls_loss + 0.02)
        ) or (
            best_val_dfl_loss is not None and val_dfl_loss is not None and val_dfl_loss > (best_val_dfl_loss + 0.02)
        )
        if best_is_early and (metric_drop >= 0.03 or val_loss_worse):
            analysis_notes.append({
                "severity": "warn",
                "title": "Possible overfitting",
                "message": f"Based on epoch trend: best mAP50-95 was {_fmt_pct_text(map5095)} at epoch {best_epoch}, latest mAP50-95 is {_fmt_pct_text(latest_map5095)} at epoch {latest_epoch}. Validation quality dropped by {_fmt_pct_text(metric_drop)} after the best epoch.",
            })

    if latest_train_total_loss is not None and latest_val_total_loss is not None and latest_epoch >= max(15, int(len(rows) * 0.5)):
        if loss_gap is not None and loss_gap >= 0.8:
            analysis_notes.append({
                "severity": "warn",
                "title": "Train/val loss gap",
                "message": f"Based on latest losses: train total loss={latest_train_total_loss:.3f}, val total loss={latest_val_total_loss:.3f}, gap={loss_gap:.3f}. Validation loss is staying much higher than train loss, which is a stronger overfit signal.",
            })
        elif (
            best_val_total_loss is not None and latest_val_total_loss > best_val_total_loss + 0.4 and
            best_train_total_loss is not None and latest_train_total_loss <= best_train_total_loss + 0.2
        ):
            analysis_notes.append({
                "severity": "warn",
                "title": "Validation loss regression",
                "message": f"Based on loss trend: best validation total loss was {best_val_total_loss:.3f}, latest is {latest_val_total_loss:.3f}, while train total loss stayed low at {latest_train_total_loss:.3f}. The model is fitting train data better than validation data.",
            })

    if quality_label == "Weak" and len(rows) >= 20:
        analysis_notes.append({
            "severity": "warn",
            "title": "Likely underfitting",
            "message": f"Based on overall run quality: best mAP50-95 only reached {_fmt_pct_text(map5095)} after {len(rows)} epochs. Training finished many epochs without reaching a strong validation score.",
        })

    if quality_label in {"Excellent", "Good"} and not analysis_notes:
        analysis_notes.append({
            "severity": "good",
            "title": "Healthy training curve",
            "message": f"Based on validation metrics: best mAP50-95={_fmt_pct_text(map5095)}, precision={_fmt_pct_text(precision)}, recall={_fmt_pct_text(recall)}. The main metrics are reasonably balanced.",
        })

    return {
        "exists": True,
        "epochs": len(rows),
        "best_epoch": best_epoch,
        "latest_epoch": latest_epoch,
        "precision": precision,
        "recall": recall,
        "map50": map50,
        "map5095": map5095,
        "latest_precision": latest_precision,
        "latest_recall": latest_recall,
        "latest_map50": latest_map50,
        "latest_map5095": latest_map5095,
        "precision_gap": precision_gap,
        "localization_gap": localization_gap,
        "latest_train_total_loss": latest_train_total_loss,
        "latest_val_total_loss": latest_val_total_loss,
        "best_train_total_loss": best_train_total_loss,
        "best_val_total_loss": best_val_total_loss,
        "loss_gap": loss_gap,
        "train_box_loss": train_box_loss,
        "train_cls_loss": train_cls_loss,
        "train_dfl_loss": train_dfl_loss,
        "val_box_loss": val_box_loss,
        "val_cls_loss": val_cls_loss,
        "val_dfl_loss": val_dfl_loss,
        "quality_label": quality_label,
        "analysis_notes": analysis_notes,
    }


def read_csv_rows(csv_file: Path, max_rows: int = 200):
    if not csv_file.exists() or not csv_file.is_file():
        return []
    try:
        with csv_file.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))[:max_rows]
    except UnicodeDecodeError:
        try:
            with csv_file.open("r", encoding="utf-8-sig", newline="") as f:
                return list(csv.DictReader(f))[:max_rows]
        except Exception:
            return []
    except Exception:
        return []


def summarize_confusion_analysis(run_dir: Path):
    counts_csv = run_dir / "misclassified_counts.csv"
    pairs_csv = run_dir / "misclassified_pairs.csv"
    samples_csv = run_dir / "misclassified_samples.csv"

    count_rows = read_csv_rows(counts_csv, max_rows=2000)
    pair_rows = read_csv_rows(pairs_csv, max_rows=5000)
    sample_rows = read_csv_rows(samples_csv, max_rows=5000)

    if not count_rows and not pair_rows and not sample_rows:
        return {
            "exists": False,
            "top_error_classes": [],
            "top_confusions": [],
        }

    all_error_classes = []
    for row in count_rows:
        try:
            gt_total = int(float(row.get("gt_total", 0) or 0))
            total_errors = int(float(row.get("total_errors", 0) or 0))
            error_rate = float(row.get("error_rate", 0) or 0)
        except Exception:
            continue
        all_error_classes.append({
            "gt_class_name": str(row.get("gt_class_name", "") or "").strip(),
            "gt_total": gt_total,
            "total_errors": total_errors,
            "error_rate": error_rate,
            "correct": int(float(row.get("correct", 0) or 0)),
        })
    all_error_classes.sort(key=lambda x: (x["error_rate"], x["total_errors"]), reverse=True)
    top_error_classes = all_error_classes[:10]

    top_confusions = []
    for row in pair_rows:
        try:
            count = int(float(row.get("count", 0) or 0))
            rate_over_gt = float(row.get("rate_over_gt", 0) or 0)
        except Exception:
            continue
        top_confusions.append({
            "gt_class_name": str(row.get("gt_class_name", "") or "").strip(),
            "pred_class_name": str(row.get("pred_class_name", "") or "").strip(),
            "count": count,
            "rate_over_gt": rate_over_gt,
        })
    top_confusions.sort(key=lambda x: (x["rate_over_gt"], x["count"]), reverse=True)
    top_confusions = top_confusions[:15]

    insights = []
    class_overview = []
    low_sample_classes = []
    total_gt = sum(max(0, int(x.get("gt_total", 0) or 0)) for x in all_error_classes)
    sample_threshold = max(10, int(total_gt * 0.01)) if total_gt > 0 else 10
    low_sample_classes = [
        {
            "gt_class_name": row.get("gt_class_name", ""),
            "gt_total": int(row.get("gt_total", 0) or 0),
            "error_rate": float(row.get("error_rate", 0) or 0),
        }
        for row in sorted(all_error_classes, key=lambda x: (x["gt_total"], -x["error_rate"]))
        if int(row.get("gt_total", 0) or 0) > 0 and int(row.get("gt_total", 0) or 0) <= sample_threshold
    ][:8]
    if top_error_classes:
        class_overview = [
            f"{row.get('gt_class_name') or '-'} ({_fmt_pct_text(row.get('error_rate')) or '0.0%'})"
            for row in top_error_classes[:5]
        ]
        top_err = top_error_classes[0]
        insights.append({
            "severity": "warn" if float(top_err.get("error_rate", 0) or 0) >= 0.2 else "info",
            "title": "Most unstable class",
            "message": f"Based on confusion totals: class {top_err.get('gt_class_name') or '-'} is wrong {_fmt_pct_text(top_err.get('error_rate')) or '0.0%'} of the time, with {top_err.get('total_errors', 0)} errors out of {top_err.get('gt_total', 0)} ground-truth samples.",
        })
        insights.append({
            "severity": "info",
            "title": "Classes detected incorrectly",
            "message": "Top classes with the highest error rate: " + ", ".join(class_overview) + ".",
        })
    if low_sample_classes:
        low_sample_text = ", ".join(
            f"{row.get('gt_class_name') or '-'} ({row.get('gt_total', 0)} samples)"
            for row in low_sample_classes[:5]
        )
        insights.append({
            "severity": "warn",
            "title": "Classes with few samples",
            "message": f"Based on confusion totals, these classes have very few validation samples and can make the metrics unstable: {low_sample_text}.",
        })
    if top_confusions:
        top_pair = top_confusions[0]
        insights.append({
            "severity": "warn" if float(top_pair.get("rate_over_gt", 0) or 0) >= 0.15 else "info",
            "title": "Dominant confusion pair",
            "message": f"Based on confusion pairs: {top_pair.get('gt_class_name') or '-'} is most often predicted as {top_pair.get('pred_class_name') or '-'} at {_fmt_pct_text(top_pair.get('rate_over_gt')) or '0.0%'}, count={top_pair.get('count', 0)}.",
        })

    sample_items = []
    for row in sample_rows:
        rel_path = str(row.get("sample_rel_path", "") or "").strip().replace("\\", "/")
        if not rel_path:
            continue
        sample_items.append({
            "image_name": str(row.get("image_name", "") or "").strip(),
            "sample_rel_path": rel_path,
            "gt_class_name": str(row.get("gt_class_name", "") or "").strip(),
            "pred_class_name": str(row.get("pred_class_name", "") or "").strip(),
            "issue_type": str(row.get("issue_type", "") or "").strip(),
        })

    return {
        "exists": True,
        "top_error_classes": top_error_classes,
        "top_confusions": top_confusions,
        "class_overview": class_overview,
        "low_sample_classes": low_sample_classes,
        "sample_threshold": sample_threshold,
        "sample_items": sample_items,
        "insights": insights,
    }


def get_output_model_train_runs_info(project_path: Path):
    output_dir = find_output_dir(project_path)
    if not output_dir:
        return []

    run_dirs = [
        d for d in sorted(output_dir.iterdir(), key=lambda p: p.name.lower())
        if d.is_dir() and d.name.lower().startswith("model_train")
    ]

    runs = []
    for run_dir in run_dirs:
        image_items = []
        for f in sorted(run_dir.rglob("*"), key=lambda p: p.as_posix().lower()):
            if not f.is_file():
                continue
            if f.suffix.lower() not in IMAGE_EXTS:
                continue
            try:
                rel_path = f.relative_to(output_dir).as_posix()
            except Exception:
                continue
            if "/misclassified_samples/" in f"/{rel_path.lower()}/":
                continue
            image_items.append({
                "name": f.name,
                "relative_path": rel_path,
            })

        results_csv = run_dir / "results.csv"
        results_info = read_results_csv_preview(results_csv)
        results_summary = summarize_results_csv(results_csv)
        confusion_summary = summarize_confusion_analysis(run_dir)
        sample_items = []
        for row in list(confusion_summary.get("sample_items") or []):
            rel_path = str(row.get("sample_rel_path", "") or "").strip().replace("\\", "/")
            if not rel_path:
                continue
            sample_items.append({
                **row,
                "relative_path": f"{run_dir.name}/{rel_path}",
            })
        confusion_summary["sample_items"] = sample_items
        runs.append({
            "run_folder": run_dir.name,
            "image_files": image_items,
            "image_count": len(image_items),
            "confusion_analysis": confusion_summary,
            "results_csv": {
                "relative_path": results_csv.relative_to(output_dir).as_posix() if results_csv.exists() else None,
                "summary": results_summary,
                **results_info,
            }
        })

    return runs


def resolve_weight_download_file(project_name: str, relative_path: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return None, "project_not_found"

    output_dir = find_output_dir(project_path)
    if not output_dir:
        return None, "output_not_found"

    candidate = (output_dir / relative_path).resolve()

    if not is_path_inside(candidate, output_dir):
        return None, "invalid_path"

    if not candidate.exists() or not candidate.is_file():
        return None, "file_not_found"

    try:
        rel_parts = candidate.relative_to(output_dir).parts
    except Exception:
        return None, "invalid_path"

    if len(rel_parts) < 2:
        return None, "invalid_output_file_path"

    run_folder = rel_parts[0]
    if not run_folder.lower().startswith("model_train"):
        return None, "invalid_run_folder"

    return candidate, None


def resolve_output_file(project_name: str, relative_path: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return None, "project_not_found"

    output_dir = find_output_dir(project_path)
    if not output_dir:
        return None, "output_not_found"

    candidate = (output_dir / relative_path).resolve()
    if not is_path_inside(candidate, output_dir):
        return None, "invalid_path"
    if not candidate.exists() or not candidate.is_file():
        return None, "file_not_found"
    return candidate, None


def resolve_run_dir(project_name: str, run_folder: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return None, "project_not_found"

    output_dir = find_output_dir(project_path)
    if not output_dir:
        return None, "output_not_found"

    candidate = (output_dir / str(run_folder or "")).resolve()
    if not is_path_inside(candidate, output_dir):
        return None, "invalid_path"
    if not candidate.exists() or not candidate.is_dir():
        return None, "run_not_found"
    return candidate, None


def load_train_module():
    module_path = get_train_template_path()
    if not module_path.exists() or not module_path.is_file():
        raise FileNotFoundError(f"Missing train module: {module_path}")

    spec = importlib.util.spec_from_file_location("train_model_ai_runtime", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load Train_model_AI.py module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_run_args(run_dir: Path):
    args_file = run_dir / "args.yaml"
    defaults = {"imgsz": 640, "device": 0}
    if not args_file.exists() or not args_file.is_file():
        return defaults
    try:
        import yaml
        data = yaml.safe_load(args_file.read_text(encoding="utf-8")) or {}
        if isinstance(data, dict):
            imgsz = data.get("imgsz", defaults["imgsz"])
            device = data.get("device", defaults["device"])
            return {
                "imgsz": int(imgsz) if imgsz is not None else defaults["imgsz"],
                "device": device if device is not None else defaults["device"],
            }
    except Exception:
        pass
    return defaults


def revalidate_run(project_name: str, run_folder: str):
    run_dir, err = resolve_run_dir(project_name, run_folder)
    if err or not run_dir:
        return False, "Run folder not found", None

    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Project not found", None

    data_yaml = find_project_data_yaml(project_path)
    if data_yaml is None:
        return False, "Missing data.yaml in project", None

    best_path = run_dir / "weights" / "best.pt"
    last_path = run_dir / "weights" / "last.pt"
    weights_path = best_path if best_path.exists() else last_path
    if not weights_path.exists():
        return False, "Missing best.pt/last.pt in run folder", None

    args_info = _read_run_args(run_dir)
    try:
        module = load_train_module()
        append_log(project_name, f"[{now_str()}] REVALIDATE START: {run_folder}")
        info = module.validate_and_export(
            weights=str(weights_path),
            data_yaml=str(data_yaml),
            img_size=int(args_info.get("imgsz", 640) or 640),
            device=args_info.get("device", 0),
            iou=0.65,
            conf=0.50,
            output_dir=str(run_dir),
        )
        append_log(project_name, f"[{now_str()}] REVALIDATE DONE: {run_folder}")
        return True, f"Re-validated {run_folder}", {
            "run_folder": run_folder,
            "artifacts_dir": str(run_dir),
            "info": info,
        }
    except Exception as e:
        append_log(project_name, f"[{now_str()}] REVALIDATE FAILED: {run_folder}: {e}")
        return False, f"Re-validate failed: {e}", None


def get_success_project_outputs():
    rows = []
    with state_lock:
        project_items = list(STATE["projects"].items())

    for project_name, info in project_items:
        if str(info.get("status", "")).lower() != "success":
            continue

        project_path_raw = str(info.get("path", "") or "").strip()
        if not project_path_raw:
            continue

        project_path = Path(project_path_raw)
        output_dir = find_output_dir(project_path)
        if not output_dir or not output_dir.exists() or not output_dir.is_dir():
            continue

        rows.append({
            "project_name": project_name,
            "output_dir": output_dir,
        })

    rows.sort(key=lambda x: str(x["project_name"]).lower())
    return rows


def sync_running_project_progress_from_data_locked(ok, data):
    monitor_progress = 0.0
    monitor_project_name = None
    monitor_project_dir = None
    monitor_is_training = False
    changed = False

    if ok and isinstance(data, dict):
        try:
            monitor_progress = float(data.get("progress", 0.0) or 0.0)
        except Exception:
            monitor_progress = 0.0

        monitor_project_name = data.get("project_name")
        monitor_project_dir = data.get("project_dir")
        monitor_is_training = bool(data.get("is_training", False))

    matched_project_name = None

    for info in STATE["projects"].values():
        if info.get("status") != "success" and float(info.get("progress", 0.0) or 0.0) != 0.0:
            info["progress"] = 0.0
            changed = True

    if monitor_project_dir:
        try:
            monitor_project_dir_norm = str(Path(monitor_project_dir).resolve()).lower()
            for name, info in STATE["projects"].items():
                try:
                    p_norm = str(Path(info["path"]).resolve()).lower()
                    if p_norm == monitor_project_dir_norm:
                        matched_project_name = name
                        break
                except Exception:
                    pass
        except Exception:
            pass

    if not matched_project_name and monitor_project_name:
        for name, info in STATE["projects"].items():
            if name.strip().lower() == str(monitor_project_name).strip().lower():
                matched_project_name = name
                break

    if not matched_project_name:
        matched_project_name = STATE.get("current")

    if matched_project_name and matched_project_name in STATE["projects"] and monitor_is_training:
        p = STATE["projects"][matched_project_name]

        if p.get("status") in ("idle", "queued", "running") and p.get("status") != "running":
            p["status"] = "running"
            changed = True

        if float(p.get("progress", 0.0) or 0.0) != monitor_progress:
            p["progress"] = monitor_progress
            changed = True

        if not p.get("last_start"):
            p["last_start"] = now_str()
            changed = True

    MONITOR_CACHE["matched_project_name"] = matched_project_name
    return matched_project_name, changed


def monitor_loop():
    last_signature = None

    while True:
        status_ok, status_data = fetch_train_monitor_json("/status")
        history_ok, history_data = fetch_train_monitor_json("/history")

        status_payload = status_data if status_ok and isinstance(status_data, dict) else {}
        history_state = history_data.get("state", {}) if history_ok and isinstance(history_data, dict) else {}
        history_logs = history_data.get("logs", []) if history_ok and isinstance(history_data, dict) else []
        history_logs = history_logs[-TRAIN_MONITOR_LOG_TAIL:]

        current_signature = monitor_snapshot_signature(
            status_ok,
            status_payload,
            history_ok,
            history_state,
            history_logs,
            None if status_ok else status_data.get("error"),
            None if history_ok else history_data.get("error"),
        )

        with state_lock:
            matched_name, state_changed = sync_running_project_progress_from_data_locked(status_ok, status_payload)

            cache_changed = current_signature != last_signature
            if cache_changed:
                MONITOR_CACHE["status_ok"] = bool(status_ok)
                MONITOR_CACHE["status"] = status_payload
                MONITOR_CACHE["history_ok"] = bool(history_ok)
                MONITOR_CACHE["history_state"] = history_state
                MONITOR_CACHE["logs"] = history_logs
                MONITOR_CACHE["matched_project_name"] = matched_name
                MONITOR_CACHE["status_error"] = None if status_ok else status_data.get("error")
                MONITOR_CACHE["status_url"] = None if status_ok else status_data.get("url")
                MONITOR_CACHE["history_error"] = None if history_ok else history_data.get("error")
                MONITOR_CACHE["history_url"] = None if history_ok else history_data.get("url")
                last_signature = current_signature

            if cache_changed or state_changed:
                bump_state_version_locked()

        time.sleep(1.0)


def start_monitor_if_needed():
    global monitor_thread

    if monitor_thread and monitor_thread.is_alive():
        return

    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()




def wait_train_completion_from_monitor(project_name: str, project_path: Path, process=None, assume_started: bool = False):
    project_name_norm = str(project_name or "").strip().lower()
    project_path_norm = str(project_path.resolve()).strip().lower()
    seen_training = False
    consecutive_monitor_errors = 0
    saw_failed_signal = False
    start_ts = time.time()
    grace_seconds = 20.0

    while True:
        ok, data = fetch_train_monitor_json("/status")
        stop_requested = is_current_train_stop_requested(project_name)

        if ok and isinstance(data, dict):
            consecutive_monitor_errors = 0
            monitor_name = str(data.get("project_name") or "").strip().lower()
            monitor_dir = str(data.get("project_dir") or "").strip().lower()
            monitor_is_training = bool(data.get("is_training", False))
            monitor_msg = str(data.get("msg") or "").strip().lower()

            is_match = False
            if monitor_dir and monitor_dir == project_path_norm:
                is_match = True
            elif monitor_name and monitor_name == project_name_norm:
                is_match = True

            if is_match and monitor_is_training:
                seen_training = True

            if is_match and (monitor_msg.startswith("failed") or "error" in monitor_msg):
                saw_failed_signal = True

            if seen_training and is_match and not monitor_is_training:
                if stop_requested:
                    return -15
                if saw_failed_signal:
                    return 1
                return 0
        else:
            consecutive_monitor_errors += 1
            if seen_training and consecutive_monitor_errors >= 3:
                # If monitor port is closed after training was observed, treat as finished.
                port_closed = False
                try:
                    with socket.create_connection((TRAIN_MONITOR_HOST, int(TRAIN_MONITOR_PORT)), timeout=1.0):
                        port_closed = False
                except OSError:
                    port_closed = True

                if port_closed and stop_requested:
                    return -15
                if port_closed and (time.time() - start_ts) >= grace_seconds:
                    return 1 if saw_failed_signal else 0

        if process is not None:
            polled = process.poll()
            if polled is not None and stop_requested:
                return -15
            if polled is not None and not seen_training and (time.time() - start_ts) >= grace_seconds:
                return int(polled)

        time.sleep(1.0)
def run_project(project_name):
    launcher_cmd = None

    with state_lock:
        project_info = STATE["projects"].get(project_name)
        if not project_info:
            return
        project_path = Path(project_info["path"])
    mark_current_train_control(project_name, project_path, pid=None, stop_requested=False)

    data_yaml_path = find_project_data_yaml(project_path)
    if data_yaml_path is None:
        error_msg = f"Không tìm thấy data.yaml trong project: {project_path}"
        with state_lock:
            p = ensure_project_state(project_name)
            p["status"] = "failed"
            p["progress"] = 0.0
            p["last_start"] = now_str()
            p["last_end"] = now_str()
            p["last_returncode"] = -998
            p["pid"] = None
            p["last_log"] = [error_msg]
            record_train_history_locked(project_name, "failed", -998)
            bump_state_version_locked()
        return

    copied_ok, copied_result = ensure_train_script_in_project(project_path)
    if not copied_ok:
        with state_lock:
            p = ensure_project_state(project_name)
            p["status"] = "failed"
            p["progress"] = 0.0
            p["last_end"] = now_str()
            p["last_returncode"] = -999
            p["last_log"] = [copied_result]
            record_train_history_locked(project_name, "failed", -999)
        return

    train_script = project_path / TRAIN_FILE

    with state_lock:
        p = ensure_project_state(project_name)
        p["status"] = "running"
        p["progress"] = 0.0
        p["last_start"] = now_str()
        p["last_end"] = None
        p["last_returncode"] = None
        p["last_log"] = []
        STATE["current"] = project_name
        bump_state_version_locked()

    train_cmd = [sys.executable, str(train_script)]
    cmd = list(train_cmd)

    append_log(project_name, f"[{now_str()}] START: {project_name}")
    append_log(project_name, f"[{now_str()}] TRAIN FILE COPIED: {copied_result}")
    append_log(project_name, f"[{now_str()}] CMD: {' '.join(cmd)}")
    append_log(project_name, f"[{now_str()}] CWD: {project_path}")
    append_log(project_name, f"[{now_str()}] TRAIN MONITOR: http://{TRAIN_MONITOR_HOST}:{TRAIN_MONITOR_PORT}")

    try:
        creationflags = 0
        external_terminal_mode = False
        popen_kwargs = {
            "cwd": str(project_path),
            "text": True,
            "bufsize": 1,
        }

        if OPEN_TRAIN_IN_NEW_TERMINAL and sys.platform.startswith("win"):
            python_exe = str(sys.executable)
            script_file = str(train_script)
            launcher_cmd = create_train_launcher_cmd(project_path, python_exe, script_file)
            cmd = [
                "cmd.exe",
                "/c",
                "start",
                "",
                str(launcher_cmd)
            ]
            external_terminal_mode = True
            append_log(project_name, f"[{now_str()}] TERMINAL CMD: cmd.exe /c start \"\" \"{launcher_cmd}\"")
            creationflags = subprocess.CREATE_NEW_CONSOLE
            popen_kwargs["stdout"] = None
            popen_kwargs["stderr"] = None
            append_log(project_name, f"[{now_str()}] Opened training in a new terminal window")
        else:
            popen_kwargs["stdout"] = subprocess.PIPE
            popen_kwargs["stderr"] = subprocess.STDOUT

        process = subprocess.Popen(
            cmd,
            creationflags=creationflags,
            **popen_kwargs
        )

        with state_lock:
            ensure_project_state(project_name)["pid"] = process.pid
        set_current_train_pid(process.pid)

        if process.stdout:
            for line in process.stdout:
                append_log(project_name, line.rstrip("\n"))
        else:
            append_log(project_name, f"[{now_str()}] Training is running in external terminal...")

        if external_terminal_mode:
            append_log(project_name, f"[{now_str()}] Waiting completion signal from monitor API...")
            rc = wait_train_completion_from_monitor(
                project_name,
                project_path,
                process=process,
                assume_started=False
            )
            append_log(project_name, f"[{now_str()}] Monitor completion detected, rc={rc}")
        else:
            process.wait()
            rc = process.returncode

        final_log_line = None
        stop_requested = is_current_train_stop_requested(project_name)
        with state_lock:
            p = ensure_project_state(project_name)
            p["last_end"] = now_str()
            p["last_returncode"] = rc
            p["pid"] = None

            if stop_requested or rc == -15:
                p["status"] = "stopped"
                p["progress"] = 0.0
                result_status = "stopped"
                final_log_line = f"[{now_str()}] TRAIN STOPPED"
            elif rc == 0:
                p["status"] = "success"
                p["progress"] = 100.0
                result_status = "success"
                final_log_line = f"[{now_str()}] TRAIN DONE"
            else:
                p["status"] = "failed"
                p["progress"] = 0.0
                result_status = "failed"
                final_log_line = f"[{now_str()}] TRAIN FAILED, returncode={rc}"

            record_train_history_locked(project_name, result_status, rc)
            bump_state_version_locked()

        if final_log_line:
            append_log(project_name, final_log_line)

        notify_train_finished(project_name, result_status, rc)

    except Exception as e:
        stop_requested = is_current_train_stop_requested(project_name)
        with state_lock:
            p = ensure_project_state(project_name)
            p["status"] = "stopped" if stop_requested else "failed"
            p["progress"] = 0.0
            p["last_end"] = now_str()
            p["last_returncode"] = -15 if stop_requested else -1
            p["pid"] = None
            if stop_requested:
                p["last_log"].append(f"[{now_str()}] TRAIN STOPPED BY USER")
                record_train_history_locked(project_name, "stopped", -15)
            else:
                p["last_log"].append(f"[{now_str()}] EXCEPTION: {repr(e)}")
                record_train_history_locked(project_name, "failed", -1)
            bump_state_version_locked()

        notify_train_finished(project_name, "stopped" if stop_requested else "failed", -15 if stop_requested else -1)

    finally:
        removed_files, cleanup_errors = cleanup_project_train_runtime_files(project_path)
        for name in removed_files:
            append_log(project_name, f"[{now_str()}] CLEANUP REMOVED: {name}")
        for err in cleanup_errors:
            append_log(project_name, f"[{now_str()}] CLEANUP FAILED: {err}")

        with state_lock:
            STATE["current"] = None
            bump_state_version_locked()
        mark_current_train_control(None, None, pid=None, stop_requested=False)


def worker_loop():
    with state_lock:
        STATE["worker_running"] = True

    while True:
        try:
            project_name = train_queue.get(timeout=1)
        except queue.Empty:
            with state_lock:
                if not STATE["queue"]:
                    STATE["worker_running"] = False
                    bump_state_version_locked()
                    return
            continue

        with state_lock:
            if project_name in STATE["queue"]:
                STATE["queue"].remove(project_name)

        run_project(project_name)

        if not CONTINUE_IF_ERROR:
            with state_lock:
                p = STATE["projects"].get(project_name, {})
                if p.get("status") == "failed":
                    STATE["queue"].clear()
                    while not train_queue.empty():
                        try:
                            train_queue.get_nowait()
                            try:
                                train_queue.task_done()
                            except Exception:
                                pass
                        except queue.Empty:
                            break
                    STATE["worker_running"] = False
                    bump_state_version_locked()
                    return

        try:
            train_queue.task_done()
        except Exception:
            pass


def start_worker_if_needed():
    global worker_thread
    with state_lock:
        running = STATE["worker_running"]

    if running:
        return

    worker_thread = threading.Thread(target=worker_loop, daemon=True)
    worker_thread.start()


def initialize_app_runtime():
    global app_runtime_initialized

    if app_runtime_initialized:
        return

    save_default_user_file_if_missing()
    with state_lock:
        STATE["history"] = load_train_history_file()
    start_monitor_if_needed()
    scan_projects()
    app_runtime_initialized = True


@app.before_request
def require_auth():
    path = request.path or "/"

    if path in PUBLIC_PATHS:
        return None

    if path.startswith("/UI/"):
        return None

    if is_authenticated():
        return None

    if path.startswith("/api/"):
        return jsonify({"ok": False, "message": "Unauthorized"}), 401

    return redirect(url_for("login", next=get_next_url(default=path)))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username", "") or "").strip()
        password = request.form.get("password", "") or ""
        next_url = get_next_url(default="/")
        creds = load_user_credentials()
        expected_username = str(creds.get("username", "")).strip()

        if hmac.compare_digest(username, expected_username) and verify_password(password, creds):
            mark_authenticated(expected_username)
            return redirect(next_url)

        return render_template(
            "login.html",
            error="Invalid username or password",
            next_url=next_url
        ), 401

    if is_authenticated():
        return redirect(get_next_url(default="/"))

    return render_template(
        "login.html",
        error="",
        next_url=get_next_url(default="/")
    )


@app.route("/logout", methods=["POST"])
def logout():
    clear_auth()
    return jsonify({"ok": True, "message": "Logged out"})


@app.route("/")
def index():
    creds = load_user_credentials()
    default_user = str(creds.get("username", "")).strip() or "admin"
    return render_template(
        "index.html",
        root_dir=str(ROOT_DIR),
        train_monitor_url=f"http://{TRAIN_MONITOR_HOST}:{TRAIN_MONITOR_PORT}",
        auth_user=session.get("auth_user", default_user)
    )


@app.route("/project_editor")
def project_editor_page():
    project = request.args.get("project", "").strip()
    if not project:
        return redirect(url_for("index"))

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        abort(404)

    return render_template("project_editor.html", project_name=project)


@app.route("/api/scan", methods=["POST"])
def api_scan():
    projects = scan_projects()
    return jsonify({
        "ok": True,
        "message": f"Found {len(projects)} project(s)",
    })


@app.route("/api/upload_project", methods=["POST"])
def api_upload_project():
    upload_file = request.files.get("file")
    ok, status, project_name = save_uploaded_project_zip(upload_file)

    msg_map = {
        "missing_file": "Missing upload file",
        "only_zip_supported": "Only .zip files are supported",
        "zip_empty": "Zip file is empty",
        "invalid_zip_path": "Invalid zip path",
        "bad_zip_file": "Corrupted zip file",
        "extract_failed": "Failed to extract zip",
        "cannot_scan_extracted_content": "Cannot scan extracted content",
        "project_not_found": "Project content not found in zip",
        "multiple_projects_detected": "Zip contains multiple projects",
        "project_already_exists": f"Project already exists: {project_name or ''}".strip(),
        "copy_failed": "Failed to copy project into ROOT_DIR",
        "uploaded": f"Project uploaded: {project_name}",
    }

    if not ok:
        return jsonify({
            "ok": False,
            "status": status,
            "message": msg_map.get(status, "Upload failed")
        }), 400

    projects = scan_projects()
    return jsonify({
        "ok": True,
        "status": "uploaded",
        "project": project_name,
        "project_count": len(projects),
        "message": msg_map["uploaded"]
    })


@app.route("/api/state")
def api_state():
    with state_lock:
        return jsonify(build_state_payload_locked())


@app.route("/api/snapshot")
def api_snapshot():
    log_project = request.args.get("log_project", "")
    log_tail = request.args.get("log_tail", str(API_LOG_TAIL_LINES))
    with state_lock:
        return jsonify(build_snapshot_payload_locked(log_project, log_tail))


@app.route("/api/state/changes")
def api_state_changes():
    try:
        since = int(request.args.get("since", "0"))
    except Exception:
        since = 0

    try:
        timeout = max(5, min(int(request.args.get("timeout", "25")), 60))
    except Exception:
        timeout = 25

    deadline = time.time() + timeout

    with state_cond:
        while int(STATE.get("version", 0)) <= since:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            state_cond.wait(timeout=remaining)

        version = int(STATE.get("version", 0))

    changed = version > since
    with_snapshot = str(request.args.get("with_snapshot", "0")).strip().lower() in {"1", "true", "yes"}
    log_project = request.args.get("log_project", "")
    log_tail = request.args.get("log_tail", str(API_LOG_TAIL_LINES))

    payload = {
        "ok": True,
        "changed": changed,
        "version": version,
    }
    if changed and with_snapshot:
        with state_lock:
            payload["snapshot"] = build_snapshot_payload_locked(log_project, log_tail)
    return jsonify(payload)


@app.route("/api/log")
def api_log():
    project = request.args.get("project", "")
    tail = request.args.get("tail", str(API_LOG_TAIL_LINES))
    with state_lock:
        return jsonify(build_project_log_payload_locked(project, tail))


@app.route("/api/queue", methods=["POST"])
def api_queue():
    data = request.get_json(force=True)
    project = data.get("project", "").strip()

    if not project:
        return jsonify({"ok": False, "message": "Missing project name"}), 400

    ok, msg = queue_project(project)
    return jsonify({"ok": ok, "message": msg})


@app.route("/api/queue_selected", methods=["POST"])
def api_queue_selected():
    data = request.get_json(force=True)
    projects = data.get("projects", [])

    if not isinstance(projects, list) or not projects:
        return jsonify({"ok": False, "message": "No project selected"}), 400

    clean_projects = []
    for x in projects:
        if isinstance(x, str) and x.strip():
            clean_projects.append(x.strip())

    if not clean_projects:
        return jsonify({"ok": False, "message": "Invalid project list"}), 400

    added, skipped = queue_projects(clean_projects)
    return jsonify({
        "ok": True,
        "added": added,
        "skipped": skipped,
        "message": f"Added {len(added)} project(s), skipped {len(skipped)}"
    })


@app.route("/api/queue_all", methods=["POST"])
def api_queue_all():
    added, skipped = queue_all()
    return jsonify({
        "ok": True,
        "added": added,
        "skipped": skipped,
        "message": f"Added={len(added)}, skipped={len(skipped)}",
    })


@app.route("/api/retry_failed", methods=["POST"])
def api_retry_failed():
    added, skipped = retry_failed_projects()
    return jsonify({
        "ok": True,
        "added": added,
        "skipped": skipped,
        "message": f"Retry failed: added={len(added)}, skipped={len(skipped)}"
    })


@app.route("/api/stop_queue", methods=["POST"])
def api_stop_queue():
    cleared = stop_pending_queue()
    return jsonify({
        "ok": True,
        "cleared": cleared,
        "message": f"Stopped pending queue: {len(cleared)} project(s)"
    })


@app.route("/api/stop_current_train", methods=["POST"])
def api_stop_current_train():
    ok, message, details = stop_current_train()
    write_audit_log(
        "stop_current_train",
        "success" if ok else "failed",
        project=str((details or {}).get("project") or ""),
        details=message,
    )
    status_code = 200 if ok else 400
    return jsonify({
        "ok": ok,
        "message": message,
        **(details or {}),
    }), status_code


@app.route("/api/lv2/verify", methods=["POST"])
def api_lv2_verify():
    data = request.get_json(force=True) or {}
    password = str(data.get("password", "") or "")
    if password != LV2_PASSWORD:
        write_audit_log("lv2_verify", "failed", details="Invalid LV2 password")
        return jsonify({"ok": False, "message": "Sai mật khẩu LV2"}), 403
    mark_lv2_authenticated()
    write_audit_log("lv2_verify", "success", details="LV2 verified")
    return jsonify({"ok": True, "message": "LV2 verified"})


@app.route("/api/history/clear", methods=["POST"])
def api_history_clear():
    lv2 = require_lv2_json()
    if lv2 is not None:
        write_audit_log("history_clear", "denied", details="LV2 password required")
        return lv2

    with state_lock:
        clear_train_history_locked()
        bump_state_version_locked()

    write_audit_log("history_clear", "success", details="Training history cleared")
    return jsonify({"ok": True, "message": "Đã xóa lịch sử train"})


@app.route("/api/project/rename", methods=["POST"])
def api_project_rename():
    lv2 = require_lv2_json()
    if lv2 is not None:
        write_audit_log("project_rename", "denied", details="LV2 password required")
        return lv2

    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    new_name = str(data.get("new_name", "") or "").strip()
    if not project or not new_name:
        write_audit_log("project_rename", "failed", project=project, target=new_name, details="Missing project or new_name")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400
    if not is_valid_project_name(new_name):
        write_audit_log("project_rename", "failed", project=project, target=new_name, details="Invalid project name")
        return jsonify({"ok": False, "message": "Tên project không hợp lệ"}), 400
    if project == new_name:
        write_audit_log("project_rename", "noop", project=project, target=new_name, details="New name equals old name")
        return jsonify({"ok": True, "message": "Tên project không thay đổi"}), 200

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        write_audit_log("project_rename", "failed", project=project, target=new_name, details="Project not found")
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404

    target_path = ROOT_DIR / new_name
    if target_path.exists():
        write_audit_log("project_rename", "failed", project=project, target=new_name, details="Target project already exists")
        return jsonify({"ok": False, "message": "Project đích đã tồn tại"}), 400

    with state_lock:
        info = STATE["projects"].get(project)
        if info and (info.get("status") == "running" or project in STATE["queue"] or STATE.get("current") == project):
            write_audit_log("project_rename", "blocked", project=project, target=new_name, details="Project is running or queued")
            return jsonify({"ok": False, "message": "Project đang chạy hoặc đang trong queue"}), 400

    try:
        shutil.move(str(project_path), str(target_path))
    except Exception as e:
        write_audit_log("project_rename", "failed", project=project, target=new_name, details=f"Move failed: {e}")
        return jsonify({"ok": False, "message": f"Không đổi tên được project: {e}"}), 500

    with state_lock:
        info = STATE["projects"].pop(project, None)
        if info:
            info["name"] = new_name
            info["path"] = str(target_path)
            STATE["projects"][new_name] = info
        bump_state_version_locked()

    scan_projects()
    write_audit_log("project_rename", "success", project=project, target=new_name, details="Project renamed")
    return jsonify({"ok": True, "message": f"Đã đổi tên project thành {new_name}", "project": new_name})


@app.route("/api/project/duplicate", methods=["POST"])
def api_project_duplicate():
    lv2 = require_lv2_json()
    if lv2 is not None:
        write_audit_log("project_duplicate", "denied", details="LV2 password required")
        return lv2

    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    new_name = str(data.get("new_name", "") or "").strip()
    if not project:
        write_audit_log("project_duplicate", "failed", details="Missing project name")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        write_audit_log("project_duplicate", "failed", project=project, target=new_name, details="Project not found")
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404

    if not new_name:
        new_name = get_available_duplicate_name(project)
    if not is_valid_project_name(new_name):
        write_audit_log("project_duplicate", "failed", project=project, target=new_name, details="Invalid target project name")
        return jsonify({"ok": False, "message": "Tên project không hợp lệ"}), 400

    target_path = ROOT_DIR / new_name
    if target_path.exists():
        write_audit_log("project_duplicate", "failed", project=project, target=new_name, details="Target project already exists")
        return jsonify({"ok": False, "message": "Project đích đã tồn tại"}), 400

    try:
        shutil.copytree(project_path, target_path)
    except Exception as e:
        write_audit_log("project_duplicate", "failed", project=project, target=new_name, details=f"Copy failed: {e}")
        return jsonify({"ok": False, "message": f"Không nhân bản được project: {e}"}), 500

    scan_projects()
    write_audit_log("project_duplicate", "success", project=project, target=new_name, details="Project duplicated")
    return jsonify({"ok": True, "message": f"Đã nhân bản project thành {new_name}", "project": new_name})


@app.route("/api/project/delete", methods=["POST"])
def api_project_delete():
    lv2 = require_lv2_json()
    if lv2 is not None:
        write_audit_log("project_delete", "denied", details="LV2 password required")
        return lv2

    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    if not project:
        write_audit_log("project_delete", "failed", details="Missing project name")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        write_audit_log("project_delete", "failed", project=project, details="Project not found")
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404
    if not is_path_inside(project_path, ROOT_DIR):
        write_audit_log("project_delete", "failed", project=project, details="Invalid project path")
        return jsonify({"ok": False, "message": "Đường dẫn project không hợp lệ"}), 400

    with state_lock:
        info = STATE["projects"].get(project)
        if info and (info.get("status") == "running" or project in STATE["queue"] or STATE.get("current") == project):
            write_audit_log("project_delete", "blocked", project=project, details="Project is running or queued")
            return jsonify({"ok": False, "message": "Project đang chạy hoặc đang trong queue"}), 400

    try:
        shutil.rmtree(project_path)
    except Exception as e:
        write_audit_log("project_delete", "failed", project=project, details=f"Delete failed: {e}")
        return jsonify({"ok": False, "message": f"Không xóa được project: {e}"}), 500

    with state_lock:
        STATE["projects"].pop(project, None)
        bump_state_version_locked()

    scan_projects()
    write_audit_log("project_delete", "success", project=project, details="Project deleted")
    return jsonify({"ok": True, "message": f"Đã xóa project {project}"})


@app.route("/api/project/clear_dataset", methods=["POST"])
def api_project_clear_dataset():
    lv2 = require_lv2_json()
    if lv2 is not None:
        write_audit_log("project_clear_dataset", "denied", details="LV2 password required")
        return lv2

    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    if not project:
        write_audit_log("project_clear_dataset", "failed", details="Missing project name")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        write_audit_log("project_clear_dataset", "failed", project=project, details="Project not found")
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404
    if not is_path_inside(project_path, ROOT_DIR):
        write_audit_log("project_clear_dataset", "failed", project=project, details="Invalid project path")
        return jsonify({"ok": False, "message": "Đường dẫn project không hợp lệ"}), 400

    with state_lock:
        info = STATE["projects"].get(project)
        if info and (info.get("status") == "running" or project in STATE["queue"] or STATE.get("current") == project):
            write_audit_log("project_clear_dataset", "blocked", project=project, details="Project is running or queued")
            return jsonify({"ok": False, "message": "Project đang chạy hoặc đang trong queue"}), 400

    try:
        removed = clear_project_dataset_dirs(project_path)
    except Exception as e:
        write_audit_log("project_clear_dataset", "failed", project=project, details=f"Clear dataset failed: {e}")
        return jsonify({"ok": False, "message": f"Không clear dataset được: {e}"}), 500

    if not removed:
        write_audit_log("project_clear_dataset", "noop", project=project, details="No dataset directories to remove")
        return jsonify({"ok": True, "message": "Không có thư mục dataset nào để xóa", "removed": []})

    write_audit_log("project_clear_dataset", "success", project=project, details=f"Removed: {', '.join(removed)}")
    return jsonify({
        "ok": True,
        "message": f"Đã xóa: {', '.join(removed)}",
        "removed": removed
    })


@app.route("/api/project/dataset_config", methods=["POST"])
def api_project_dataset_config_save():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    if not project:
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404

    ok, err, clean_cfg = save_dataset_config(project_path, data)
    if not ok:
        return jsonify({"ok": False, "message": err}), 400

    return jsonify({
        "ok": True,
        "message": "Đã lưu dataset config",
        "config": clean_cfg
    })


@app.route("/api/project/create_dataset", methods=["POST"])
def api_project_create_dataset():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    split_mode = str(data.get("split_mode", "count") or "count").strip().lower()
    if not project:
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    ok, message, payload = create_dataset_for_project(project, data, split_mode=split_mode)
    if not ok:
        return jsonify({"ok": False, "message": message}), 400

    return jsonify({
        "ok": True,
        "message": message,
        **(payload or {})
    })


@app.route("/api/project/merge_train_valid", methods=["POST"])
def api_project_merge_train_valid():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    if not project:
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    ok, message, payload = merge_train_valid_to_train(project)
    if not ok:
        return jsonify({"ok": False, "message": message}), 400

    return jsonify({
        "ok": True,
        "message": message,
        **(payload or {})
    })


@app.route("/api/project/revalidate_run", methods=["POST"])
def api_project_revalidate_run():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    run_folder = str(data.get("run_folder", "") or "").strip()
    if not project or not run_folder:
        return jsonify({"ok": False, "message": "Missing project or run_folder"}), 400

    ok, message, payload = revalidate_run(project, run_folder)
    write_audit_log(
        "project_revalidate_run",
        "success" if ok else "failed",
        project=project,
        target=run_folder,
        details=message,
    )
    return jsonify({
        "ok": ok,
        "message": message,
        **(payload or {}),
    }), (200 if ok else 400)


@app.route("/api/train_monitor/status")
def api_train_monitor_status():
    start_monitor_if_needed()
    with state_lock:
        return jsonify(build_monitor_status_payload_locked())


@app.route("/api/train_monitor/history")
def api_train_monitor_history():
    start_monitor_if_needed()
    with state_lock:
        return jsonify(build_monitor_history_payload_locked())


@app.route("/api/project_detail")
def api_project_detail():
    project = request.args.get("project", "").strip()
    if not project:
        return jsonify({"ok": False, "message": "Thiáº¿u tĂªn project"}), 400

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        return jsonify({"ok": False, "message": "KhĂ´ng tĂ¬m tháº¥y project"}), 404

    image_folder_count = count_images_in_dir(project_path / "image")
    train_images_count = count_images_in_dir(project_path / "train" / "images")
    valid_images_count = count_images_in_dir(project_path / "valid" / "images")
    test_images_count = count_images_in_dir(project_path / "test" / "images")
    output_info = get_output_model_train_weights_info(project_path)
    run_details = get_output_model_train_runs_info(project_path)
    dataset_cfg = load_dataset_config(project_path)

    return jsonify({
        "ok": True,
        "project": project,
        "image_folder_count": image_folder_count,
        "train_images_count": train_images_count,
        "valid_images_count": valid_images_count,
        "test_images_count": test_images_count,
        "output_folder_exists": output_info["exists"],
        "output_folder_name": output_info["folder"],
        "model_train_folders": output_info["model_train_folders"],
        "weight_files": output_info["items"],
        "weight_files_count": len(output_info["items"]),
        "run_details": run_details,
        "dataset_config": dataset_cfg,
    })


@app.route("/api/project_images")
def api_project_images():
    project = request.args.get("project", "").strip()
    if not project:
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    try:
        page = max(1, int(request.args.get("page", "1")))
    except Exception:
        page = 1

    try:
        page_size = max(1, min(int(request.args.get("page_size", "200")), 1000))
    except Exception:
        page_size = 200

    q = str(request.args.get("q", "") or "").strip().lower()
    sort_by = str(request.args.get("sort_by", "name") or "name").strip().lower()
    sort_dir = str(request.args.get("sort_dir", "asc") or "asc").strip().lower()
    class_filter_raw = str(request.args.get("class_filter", "") or "").strip()
    class_filter = None
    if class_filter_raw != "":
        try:
            class_filter = int(class_filter_raw)
        except Exception:
            class_filter = None

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404

    image_dir, all_images = list_project_images(project_path, limit=0)
    if not image_dir:
        return jsonify({
            "ok": True,
            "project": project,
            "image_dir": None,
            "images": [],
            "count": 0,
            "total": 0,
            "page": 1,
            "page_size": page_size,
            "total_pages": 0,
            "has_prev": False,
            "has_next": False,
            "message": "Không tìm thấy thư mục image/images"
        })

    items = []
    need_class_info = (sort_by == "class") or (class_filter is not None)

    for rel in all_images:
        abs_path = (image_dir / rel).resolve()
        try:
            st = abs_path.stat()
            modified_ts = float(st.st_mtime)
        except Exception:
            modified_ts = 0.0

        class_ids = []
        if need_class_info:
            label_file, _ = resolve_label_file_for_image(project, rel, create_missing=False)
            class_ids = extract_class_ids_from_label_file(label_file) if label_file else []

        items.append({
            "rel": rel,
            "name": Path(rel).name,
            "modified_ts": modified_ts,
            "modified_at": datetime.fromtimestamp(modified_ts).strftime("%Y-%m-%d %H:%M:%S") if modified_ts > 0 else "-",
            "class_ids": class_ids,
            "class_min": class_ids[0] if class_ids else 10**9
        })

    if q:
        items = [x for x in items if q in str(x.get("name", "")).lower() or q in str(x.get("rel", "")).lower()]

    if class_filter is not None:
        items = [x for x in items if class_filter in (x.get("class_ids") or [])]

    if sort_by == "date":
        items.sort(key=lambda x: (x.get("modified_ts", 0.0), str(x.get("name", "")).lower()))
    elif sort_by == "class":
        items.sort(key=lambda x: (int(x.get("class_min", 10**9)), str(x.get("name", "")).lower()))
    else:
        items.sort(key=lambda x: str(x.get("name", "")).lower())

    if sort_dir == "desc":
        items.reverse()

    total = len(items)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    if total_pages > 0:
        page = min(page, total_pages)
    else:
        page = 1

    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    if not need_class_info:
        for it in page_items:
            rel = str(it.get("rel", "") or "")
            label_file, _ = resolve_label_file_for_image(project, rel, create_missing=False)
            class_ids = extract_class_ids_from_label_file(label_file) if label_file else []
            it["class_ids"] = class_ids
            it["class_min"] = class_ids[0] if class_ids else 10**9
    images = [x.get("rel") for x in page_items]

    return jsonify({
        "ok": True,
        "project": project,
        "image_dir": str(image_dir),
        "images": images,
        "items": page_items,
        "count": len(images),
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_prev": page > 1 and total_pages > 0,
        "has_next": page < total_pages,
    })


@app.route("/api/project_classes")
def api_project_classes():
    project = request.args.get("project", "").strip()
    if not project:
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404

    classes, class_file = load_project_classes(project_path)
    return jsonify({
        "ok": True,
        "project": project,
        "classes": classes,
        "count": len(classes),
        "class_file": class_file
    })


@app.route("/api/project_image_file")
def api_project_image_file():
    project = request.args.get("project", "").strip()
    rel = request.args.get("rel", "").strip()
    if not project or not rel:
        abort(400)

    img_file, err = resolve_project_image_file(project, rel)
    if err or not img_file:
        abort(404)

    return send_file(img_file)


@app.route("/api/output_file")
def api_output_file():
    project = request.args.get("project", "").strip()
    rel = request.args.get("rel", "").strip()
    if not project or not rel:
        abort(400)

    file_path, err = resolve_output_file(project, rel)
    if err or not file_path:
        abort(404)

    return send_file(file_path)


@app.route("/api/project_image_info")
def api_project_image_info():
    project = request.args.get("project", "").strip()
    rel = request.args.get("rel", "").strip()
    if not project or not rel:
        return jsonify({"ok": False, "message": "Thiếu project hoặc rel"}), 400

    info, err = get_project_image_info(project, rel)
    if err or not info:
        return jsonify({"ok": False, "message": err or "file_not_found"}), 404

    return jsonify({
        "ok": True,
        "project": project,
        **info
    })


@app.route("/api/project_label")
def api_project_label():
    project = request.args.get("project", "").strip()
    rel = request.args.get("rel", "").strip()
    if not project or not rel:
        return jsonify({"ok": False, "message": "Thiếu project hoặc rel"}), 400

    label_file, err = resolve_label_file_for_image(project, rel, create_missing=False)
    if err:
        return jsonify({"ok": False, "message": err}), 400

    text = ""
    exists = bool(label_file and label_file.exists() and label_file.is_file())
    if exists:
        try:
            text = label_file.read_text(encoding="utf-8")
        except Exception:
            text = ""

    return jsonify({
        "ok": True,
        "project": project,
        "rel": rel,
        "exists": exists,
        "label_path": str(label_file) if label_file else None,
        "text": text
    })


@app.route("/api/project_label", methods=["POST"])
def api_project_label_save():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    rel = str(data.get("rel", "") or "").strip()
    text = str(data.get("text", "") or "")

    if not project or not rel:
        return jsonify({"ok": False, "message": "Thiếu project hoặc rel"}), 400

    label_file, err = resolve_label_file_for_image(project, rel, create_missing=True)
    if err or not label_file:
        return jsonify({"ok": False, "message": err or "cannot_resolve_label_path"}), 400

    try:
        label_file.write_text(text.replace("\r\n", "\n"), encoding="utf-8")
    except Exception:
        return jsonify({"ok": False, "message": "Không lưu được label"}), 500

    return jsonify({
        "ok": True,
        "message": "Đã lưu label",
        "label_path": str(label_file)
    })


@app.route("/api/download_weight")
def api_download_weight():
    project = request.args.get("project", "").strip()
    rel = request.args.get("rel", "").strip()

    if not project or not rel:
        abort(400)

    file_path, err = resolve_weight_download_file(project, rel)
    if err or not file_path:
        abort(404)

    project_path = resolve_project_path(project)
    if not project_path:
        abort(404)

    output_dir = find_output_dir(project_path)
    if not output_dir:
        abort(404)

    try:
        rel_parts = file_path.relative_to(output_dir).parts
    except Exception:
        abort(404)

    run_folder = rel_parts[0] if len(rel_parts) >= 1 else "Model_Train"
    original_name = file_path.name
    zip_name = f"{safe_download_part(project)}__{safe_download_part(run_folder)}__{safe_download_part(original_name)}.zip"

    tmp_dir = tempfile.mkdtemp(prefix="traincontrol_zip_")
    zip_path = Path(tmp_dir) / zip_name

    st = file_path.stat()
    dt = datetime.fromtimestamp(st.st_mtime)
    zipinfo = zipfile.ZipInfo(filename=original_name)
    zipinfo.date_time = (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    zipinfo.compress_type = zipfile.ZIP_DEFLATED

    with zipfile.ZipFile(zip_path, "w") as zf:
        with open(file_path, "rb") as f:
            data = f.read()
        zf.writestr(zipinfo, data)

    @after_this_request
    def cleanup(response):
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return response

    return send_file(
        zip_path,
        as_attachment=True,
        download_name=zip_name,
        last_modified=st.st_mtime
    )


@app.route("/api/download_success_outputs")
def api_download_success_outputs():
    rows = get_success_project_outputs()
    if not rows:
        return jsonify({
            "ok": False,
            "message": "Không có project success nào có Output để tải"
        }), 404

    tmp_dir = tempfile.mkdtemp(prefix="traincontrol_outputs_zip_")
    zip_name = f"success_outputs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    zip_path = Path(tmp_dir) / zip_name

    file_count = 0
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for row in rows:
            project_name = str(row["project_name"])
            output_dir = Path(row["output_dir"])
            project_folder = safe_download_part(project_name)
            output_folder = safe_download_part(output_dir.name)

            for file_path in output_dir.rglob("*"):
                if not file_path.is_file():
                    continue

                try:
                    rel_path = file_path.relative_to(output_dir).as_posix()
                except Exception:
                    continue

                arcname = f"{project_folder}/{output_folder}/{rel_path}"
                zf.write(file_path, arcname=arcname)
                file_count += 1

    if file_count == 0:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return jsonify({
            "ok": False,
            "message": "Không có file nào trong Output của các project success"
        }), 404

    @after_this_request
    def cleanup(response):
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return response

    return send_file(
        zip_path,
        as_attachment=True,
        download_name=zip_name,
        last_modified=zip_path.stat().st_mtime
    )


@app.route("/api/notify/state")
def api_notify_state():
    with state_lock:
        return jsonify(build_notify_state_payload_locked())


@app.route("/api/notify/toggle", methods=["POST"])
def api_notify_toggle():
    data = request.get_json(force=True) or {}
    enabled = bool(data.get("enabled", False))

    if enabled and build_notifier() is None:
        return jsonify({
            "ok": False,
            "message": "Telegram token/chat_id is not configured"
        }), 400

    with state_lock:
        NOTIFY_STATE["enabled"] = enabled
        bump_state_version_locked()

    return jsonify({
        "ok": True,
        "enabled": enabled,
        "message": "Telegram notification enabled" if enabled else "Telegram notification disabled"
    })


initialize_app_runtime()


if __name__ == "__main__":
    initialize_app_runtime()
    print(f"Web monitor is running at: http://127.0.0.1:{PORT}")
    try:
        from waitress import serve
        print(f"Using waitress with threads={WAITRESS_THREADS}")
        serve(app, host=HOST, port=PORT, threads=WAITRESS_THREADS)
    except Exception:
        app.run(host=HOST, port=PORT, debug=False, threaded=True)

