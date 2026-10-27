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
import stat
import struct
import time
import socket
import hmac
import math
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

from flask import Flask, jsonify, request, abort, send_file, after_this_request, redirect, url_for, render_template
from werkzeug.middleware.proxy_fix import ProxyFix

from tc_auth import (
    clear_auth,
    current_session_id,
    current_auth_user as auth_current_auth_user,
    get_next_url,
    is_authenticated,
    is_lv2_authenticated,
    is_lv3_authenticated,
    mark_authenticated,
    mark_lv2_authenticated,
    mark_lv3_authenticated,
    require_lv2_json,
    require_lv3_json,
)
from tc_notify import (
    build_notifier,
    is_notify_enabled,
    notify_train_finished,
    send_telegram_notification_async,
)
from tc_routes_basic import register_basic_routes
from tc_backup import (
    build_backup_status_payload as backup_build_backup_status_payload,
    start_project_backup as backup_start_project_backup,
)
from tc_config import (
    API_LOG_TAIL_LINES,
    AUDIT_LOG_FILE_NAME,
    AUTH_SECRET_KEY,
    BACKUP_COPY_CHUNK_SIZE,
    BACKUP_ROOT,
    BEHIND_HTTPS_PROXY,
    CONTINUE_IF_ERROR,
    DEFAULT_USER_CREDENTIALS,
    HOST,
    HTTPS_CERT_FILE,
    HTTPS_HOSTNAMES,
    HTTPS_KEY_FILE,
    HTTPS_PORT,
    LV2_PASSWORD,
    LV3_PASSWORD,
    OPEN_TRAIN_IN_NEW_TERMINAL,
    PORT,
    PROJECT_LOG_MAX_LINES,
    ROOT_DIR,
    TRAIN_FILE,
    TRAIN_HISTORY_FILE_NAME,
    TRAIN_MONITOR_HOST,
    TRAIN_MONITOR_LOG_TAIL,
    TRAIN_MONITOR_PORT,
    TRAIN_MONITOR_TIMEOUT,
    USE_HTTPS,
    USER_FILE_NAME,
    WAITRESS_CHANNEL_TIMEOUT,
    WAITRESS_CLEANUP_INTERVAL,
    WAITRESS_CONNECTION_LIMIT,
    WAITRESS_MAX_REQUEST_BODY_SIZE,
    WAITRESS_THREADS,
)
from tc_dataset import (
    clear_project_dataset_dirs,
    collect_importable_data_files,
    dataset_config_file_path as dataset_dataset_config_file_path,
    get_available_duplicate_name,
    is_path_inside,
    is_valid_project_name,
    load_dataset_config as dataset_load_dataset_config,
    read_shared_dataset_config as dataset_read_shared_dataset_config,
    remove_tree_best_effort,
    robust_rmtree,
    save_dataset_config as dataset_save_dataset_config,
    save_uploaded_project_zip as dataset_save_uploaded_project_zip,
    validate_dataset_config as dataset_validate_dataset_config,
    write_shared_dataset_config as dataset_write_shared_dataset_config,
)
from tc_persistence import (
    append_audit_log as persist_append_audit_log,
    append_train_history_file as persist_append_train_history_file,
    audit_log_file_path as persist_audit_log_file_path,
    authenticate_user as persist_authenticate_user,
    create_user_account as persist_create_user_account,
    delete_user_account as persist_delete_user_account,
    load_user_accounts as persist_load_user_accounts,
    load_train_history_file as persist_load_train_history_file,
    load_user_credentials as persist_load_user_credentials,
    now_str as persist_now_str,
    save_default_user_file_if_missing as persist_save_default_user_file_if_missing,
    save_user_accounts as persist_save_user_accounts,
    train_history_file_path as persist_train_history_file_path,
    user_file_path as persist_user_file_path,
    verify_password as persist_verify_password,
)
from tc_runtime import (
    CURRENT_TRAIN_CONTROL,
    MONITOR_CACHE,
    NOTIFY_STATE,
    STATE,
    state_cond,
    state_lock,
    train_queue,
)

BASE_DIR = Path(__file__).resolve().parent
project_notes_lock = threading.Lock()

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "UI"),
    static_folder=str(BASE_DIR / "UI"),
    static_url_path="/UI"
)
app.secret_key = AUTH_SECRET_KEY
if BEHIND_HTTPS_PROXY:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = bool(USE_HTTPS or BEHIND_HTTPS_PROXY)
app.config["SESSION_REFRESH_EACH_REQUEST"] = False
app.config["MAX_CONTENT_LENGTH"] = WAITRESS_MAX_REQUEST_BODY_SIZE
app.config["JSON_AS_ASCII"] = False
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False

worker_thread = None
monitor_thread = None
app_runtime_initialized = False
revalidate_task_lock = threading.Lock()
REVALIDATE_TASK = {
    "id": None,
    "project": "",
    "run_folder": "",
    "status": "idle",
    "progress": 0.0,
    "message": "",
    "detail": "",
    "started_at": 0.0,
    "ended_at": 0.0,
    "result": None,
}
model_test_task_lock = threading.Lock()
MODEL_TEST_TASK = {
    "id": None,
    "project": "",
    "run_folder": "",
    "status": "idle",
    "progress": 0.0,
    "message": "",
    "detail": "",
    "started_at": 0.0,
    "ended_at": 0.0,
    "result": None,
}
predict_task_lock = threading.Lock()
PREDICT_TASK = {
    "id": None,
    "project": "",
    "weights_rel": "",
    "status": "idle",
    "progress": 0.0,
    "message": "",
    "detail": "",
    "started_at": 0.0,
    "ended_at": 0.0,
    "result": None,
}
project_fs_task_lock = threading.Lock()
PROJECT_FS_TASK = {
    "id": None,
    "project": "",
    "operation": "",
    "target": "",
    "status": "idle",
    "progress": 0.0,
    "message": "",
    "detail": "",
    "started_at": 0.0,
    "ended_at": 0.0,
    "result": None,
}
dataset_task_lock = threading.Lock()
DATASET_TASK = {
    "id": None,
    "project": "",
    "operation": "",
    "status": "idle",
    "progress": 0.0,
    "message": "",
    "detail": "",
    "started_at": 0.0,
    "ended_at": 0.0,
    "result": None,
}
bbox_analysis_cache_lock = threading.Lock()
BBOX_ANALYSIS_CACHE = {}

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".gif", ".tif", ".tiff"}
PUBLIC_PATHS = {"/login"}


def queue_session_file_path():
    return BASE_DIR / "queue_session.json"


def build_queue_session_payload_locked():
    queue_names = [str(x or "").strip() for x in list(STATE.get("queue") or []) if str(x or "").strip()]
    current_name = str(STATE.get("current") or "").strip()
    projects = []

    if current_name:
        projects.append(current_name)
    for name in queue_names:
        if name not in projects:
            projects.append(name)

    return {
        "saved_at": now_str(),
        "current": current_name,
        "queue": queue_names,
        "projects": projects,
    }


def clear_queue_session_file():
    try:
        queue_session_file_path().unlink(missing_ok=True)
    except Exception:
        pass


def sync_queue_session_file_locked():
    payload = build_queue_session_payload_locked()
    if not payload["projects"]:
        clear_queue_session_file()
        return
    try:
        queue_session_file_path().write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def load_queue_session_file():
    p = queue_session_file_path()
    if not p.exists() or not p.is_file():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def user_file_path():
    return persist_user_file_path(BASE_DIR, USER_FILE_NAME)


def train_history_file_path():
    return persist_train_history_file_path(BASE_DIR, TRAIN_HISTORY_FILE_NAME)


def project_notes_file_path():
    return BASE_DIR / "project_notes.json"


def load_project_notes_file() -> dict:
    try:
        with project_notes_lock:
            path = project_notes_file_path()
            if not path.exists():
                return {}
            data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        str(k): str(v or "")
        for k, v in data.items()
        if str(k or "").strip()
    }


def save_project_notes_file(notes: dict):
    cleaned = {
        str(k): str(v or "")
        for k, v in (notes or {}).items()
        if str(k or "").strip() and str(v or "").strip()
    }
    path = project_notes_file_path()
    tmp_path = path.with_name(f"{path.name}.{threading.get_ident()}.tmp")
    payload = json.dumps(cleaned, ensure_ascii=False, indent=2)
    with project_notes_lock:
        tmp_path.write_text(payload, encoding="utf-8")
        os.replace(tmp_path, path)


def update_project_note_file(project: str, note: str) -> dict:
    project = str(project or "").strip()
    note = str(note or "").strip()
    path = project_notes_file_path()
    tmp_path = path.with_name(f"{path.name}.{threading.get_ident()}.tmp")
    with project_notes_lock:
        try:
            data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}

        if note:
            data[project] = note
        else:
            data.pop(project, None)

        cleaned = {
            str(k): str(v or "")
            for k, v in data.items()
            if str(k or "").strip() and str(v or "").strip()
        }
        tmp_path.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, path)
        return cleaned


def audit_log_file_path():
    return persist_audit_log_file_path(BASE_DIR, AUDIT_LOG_FILE_NAME)


def now_str():
    return persist_now_str()


def current_auth_user() -> str:
    return auth_current_auth_user()


def append_audit_log(entry: dict):
    persist_append_audit_log(BASE_DIR, AUDIT_LOG_FILE_NAME, entry)


def write_audit_log(action: str, status: str, project: str = "", target: str = "", details: str = ""):
    try:
        remote_addr = str(request.headers.get("X-Forwarded-For") or request.remote_addr or "").strip()
    except Exception:
        remote_addr = ""

    append_audit_log({
        "time": now_str(),
        "user": current_auth_user(),
        "session_id": current_session_id(),
        "ip": remote_addr,
        "action": str(action or "").strip(),
        "status": str(status or "").strip(),
        "project": str(project or "").strip(),
        "target": str(target or "").strip(),
        "details": str(details or "").strip(),
    })


def load_audit_log_items(max_items: int = 500, session_only: bool = True):
    p = audit_log_file_path()
    if not p.exists() or not p.is_file():
        return []

    current_sid = current_session_id()
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
                if not isinstance(data, dict):
                    continue
                sid = str(data.get("session_id", "") or "").strip()
                if session_only and current_sid and sid and sid != current_sid:
                    continue
                if session_only and current_sid and not sid:
                    continue
                rows.append({
                    "time": str(data.get("time", "") or "").strip(),
                    "user": str(data.get("user", "") or "").strip(),
                    "session_id": sid,
                    "ip": str(data.get("ip", "") or "").strip(),
                    "action": str(data.get("action", "") or "").strip(),
                    "status": str(data.get("status", "") or "").strip(),
                    "project": str(data.get("project", "") or "").strip(),
                    "target": str(data.get("target", "") or "").strip(),
                    "details": str(data.get("details", "") or "").strip(),
                })
    except Exception:
        return []

    return rows[-max(1, int(max_items or 500)):]


def restart_current_server_soon(delay_seconds: float = 1.0):
    def _restart():
        time.sleep(max(0.2, float(delay_seconds or 1.0)))
        try:
            restart_waitress = BASE_DIR / "restart_waitress.cmd"
            start_waitress = restart_waitress if restart_waitress.exists() else BASE_DIR / "start_waitress.cmd"
            if os.name == "nt" and start_waitress.exists():
                creationflags = 0
                if hasattr(subprocess, "CREATE_NEW_CONSOLE"):
                    creationflags |= subprocess.CREATE_NEW_CONSOLE
                if hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
                    creationflags |= subprocess.CREATE_NEW_PROCESS_GROUP
                subprocess.Popen(
                    [
                        "cmd.exe",
                        "/c",
                        str(start_waitress),
                    ],
                    cwd=str(BASE_DIR),
                    close_fds=False,
                    creationflags=creationflags,
                )
            else:
                argv = list(sys.argv or [])
                if argv:
                    script = Path(argv[0])
                    if not script.is_absolute():
                        argv[0] = str((BASE_DIR / script).resolve())
                else:
                    argv = [str(Path(__file__).resolve())]

                cmd = [sys.executable] + argv
                kwargs = {
                    "cwd": str(BASE_DIR),
                    "close_fds": False,
                }
                if os.name == "nt" and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
                    kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
                subprocess.Popen(cmd, **kwargs)
        finally:
            os._exit(0)

    threading.Thread(target=_restart, daemon=True).start()


def save_default_user_file_if_missing():
    persist_save_default_user_file_if_missing(BASE_DIR, USER_FILE_NAME, DEFAULT_USER_CREDENTIALS)


def append_train_history_file(entry: dict):
    persist_append_train_history_file(BASE_DIR, TRAIN_HISTORY_FILE_NAME, entry)


def load_train_history_file(max_items: int = 500):
    return persist_load_train_history_file(BASE_DIR, TRAIN_HISTORY_FILE_NAME, max_items=max_items)


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


def load_user_credentials():
    return persist_load_user_credentials(BASE_DIR, USER_FILE_NAME, DEFAULT_USER_CREDENTIALS)


def load_user_accounts():
    return persist_load_user_accounts(BASE_DIR, USER_FILE_NAME, DEFAULT_USER_CREDENTIALS)


def authenticate_user(username: str, password: str):
    return persist_authenticate_user(BASE_DIR, USER_FILE_NAME, DEFAULT_USER_CREDENTIALS, username, password)


def create_user_account(username: str, password: str):
    return persist_create_user_account(BASE_DIR, USER_FILE_NAME, DEFAULT_USER_CREDENTIALS, username, password)


def delete_user_account(username: str):
    return persist_delete_user_account(BASE_DIR, USER_FILE_NAME, DEFAULT_USER_CREDENTIALS, username)


def verify_password(password: str, creds: dict):
    return persist_verify_password(password, creds)


def clear_train_history_locked():
    STATE["history"] = []
    try:
        train_history_file_path().write_text("", encoding="utf-8")
    except Exception:
        pass


def dataset_config_file_path() -> Path:
    return dataset_dataset_config_file_path(BASE_DIR)


def read_shared_dataset_config():
    return dataset_read_shared_dataset_config(BASE_DIR)


def write_shared_dataset_config(data: dict):
    dataset_write_shared_dataset_config(BASE_DIR, data)


def load_dataset_config(project_path: Path):
    return dataset_load_dataset_config(BASE_DIR, project_path)


def validate_dataset_config(cfg: dict):
    return dataset_validate_dataset_config(cfg)


def save_dataset_config(project_path: Path, cfg: dict):
    return dataset_save_dataset_config(BASE_DIR, project_path, cfg)


def bump_state_version_locked():
    STATE["version"] = int(STATE.get("version", 0)) + 1
    state_cond.notify_all()


def clear_monitor_cache_locked():
    MONITOR_CACHE["status_ok"] = False
    MONITOR_CACHE["status"] = {}
    MONITOR_CACHE["history_ok"] = False
    MONITOR_CACHE["history_state"] = {}
    MONITOR_CACHE["logs"] = []
    MONITOR_CACHE["matched_project_name"] = None
    MONITOR_CACHE["status_error"] = None
    MONITOR_CACHE["status_url"] = None
    MONITOR_CACHE["history_error"] = None
    MONITOR_CACHE["history_url"] = None


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
    projects = [
        project_state_public(x)
        for x in STATE["projects"].values()
        if str(x.get("path") or "").strip()
    ]
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


    return True, f"Đã bắt đầu backup {project_name}", task_id


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


def _zip_datetime_from_ts(ts: float):
    dt = datetime.fromtimestamp(max(float(ts), 315532800.0))
    return (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


def _windows_filetime_from_ts(ts: float) -> int:
    return int((float(ts) + 11644473600) * 10_000_000)


def _ntfs_time_extra(st) -> bytes:
    # NTFS ZIP extra field stores mtime/atime/ctime for extractors that support it.
    mtime = _windows_filetime_from_ts(st.st_mtime)
    atime = _windows_filetime_from_ts(st.st_atime)
    ctime = _windows_filetime_from_ts(st.st_ctime)
    payload = struct.pack("<LHHQQQ", 0, 0x0001, 24, mtime, atime, ctime)
    return struct.pack("<HH", 0x000A, len(payload)) + payload


def write_file_to_zip_preserve_times(zf: zipfile.ZipFile, file_path: Path, arcname: str):
    st = file_path.stat()
    zi = zipfile.ZipInfo(filename=str(arcname).replace("\\", "/"))
    zi.date_time = _zip_datetime_from_ts(st.st_mtime)
    zi.compress_type = zipfile.ZIP_DEFLATED
    zi.create_system = 0
    zi.extra = _ntfs_time_extra(st)

    with open(file_path, "rb") as src:
        with zf.open(zi, "w") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)


def safe_project_name_part(text: str) -> str:
    text = str(text).strip()
    text = re.sub(r'[\\/:*?"<>|]+', "_", text)
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"_+", "_", text)
    return text.strip(" ._") or "project"

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
            "note": "",
        }
    else:
        STATE["projects"][project_name].setdefault("note", "")
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
        "note": p.get("note", ""),
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


def find_project_train_pids(project_path: Path, include_launcher: bool = True):
    pids = set()
    if not sys.platform.startswith("win"):
        return pids

    project_path = project_path.resolve()
    train_script = str((project_path / TRAIN_FILE).resolve()).replace("'", "''")
    launcher_path = str((project_path / "_train_launcher.cmd").resolve()).replace("'", "''")
    launcher_match = f" -or ($_.CommandLine -like '*{launcher_path}*')" if include_launcher else ""
    command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -and "
        f"(($_.Name -match '^python(?:w)?\\.exe$' -and $_.CommandLine -like '*{train_script}*'){launcher_match}) }} | "
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
    notes = load_project_notes_file()
    with state_lock:
        known_names = set()
        for p in projects:
            known_names.add(p.name)
            already_known = p.name in STATE["projects"]
            info = ensure_project_state(p.name)
            info["path"] = str(p)
            if not already_known:
                info["note"] = str(notes.get(p.name, "") or "")

        stale_names = []
        for name, info in STATE["projects"].items():
            if name in known_names:
                continue

            if info.get("status") in ("idle", "success", "failed", "stopped"):
                stale_names.append(name)
            else:
                info["path"] = ""

        for name in stale_names:
            STATE["projects"].pop(name, None)

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
        sync_queue_session_file_locked()
        bump_state_version_locked()

    train_queue.put(project_name)
    start_worker_if_needed()
    return True, f"Added {project_name} to queue"


def is_train_slot_busy_locked(exclude_project: str = "") -> bool:
    exclude_project = str(exclude_project or "").strip()

    current_name = str(STATE.get("current") or "").strip()
    if current_name and current_name != exclude_project:
        return True

    control_name = str(CURRENT_TRAIN_CONTROL.get("project") or "").strip()
    if control_name and control_name != exclude_project:
        return True

    for name, info in STATE["projects"].items():
        if str(name or "").strip() == exclude_project:
            continue
        if str(info.get("status") or "").strip() == "running":
            return True

    monitor_status = MONITOR_CACHE.get("status") or {}
    if bool(monitor_status.get("is_training", False)):
        monitor_project_name = str(MONITOR_CACHE.get("matched_project_name") or monitor_status.get("project_name") or "").strip()
        if not monitor_project_name or monitor_project_name != exclude_project:
            return True

    return False


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


def rebuild_train_queue_from_state_locked():
    pending = list(STATE.get("queue") or [])
    try:
        with train_queue.mutex:
            train_queue.queue.clear()
            for name in pending:
                train_queue.queue.append(name)
            train_queue.not_empty.notify_all()
    except Exception:
        while True:
            try:
                train_queue.get_nowait()
                try:
                    train_queue.task_done()
                except Exception:
                    pass
            except queue.Empty:
                break
        for name in pending:
            train_queue.put(name)


def reorder_queued_project(project_name, direction):
    project_name = str(project_name or "").strip()
    direction = str(direction or "").strip().lower()
    if direction not in ("up", "down"):
        return False, "Invalid direction"

    with state_lock:
        queue_list = list(STATE.get("queue") or [])
        if project_name not in queue_list:
            return False, f"{project_name} is not in queue"

        idx = queue_list.index(project_name)
        target_idx = idx - 1 if direction == "up" else idx + 1
        if target_idx < 0 or target_idx >= len(queue_list):
            return False, "Project is already at this priority"

        queue_list[idx], queue_list[target_idx] = queue_list[target_idx], queue_list[idx]
        STATE["queue"] = queue_list
        rebuild_train_queue_from_state_locked()
        sync_queue_session_file_locked()
        bump_state_version_locked()

    return True, f"Updated priority for {project_name}"


def restore_saved_current_project_locked(project_name: str) -> bool:
    project_name = str(project_name or "").strip()
    if not project_name:
        return False

    p = ensure_project_state(project_name)
    project_path_raw = str(p.get("path") or "").strip()
    if not project_path_raw:
        return False

    try:
        project_path = Path(project_path_raw).resolve()
    except Exception:
        return False

    monitor_status = MONITOR_CACHE.get("status") or {}
    monitor_project_name = str(MONITOR_CACHE.get("matched_project_name") or monitor_status.get("project_name") or "").strip()
    monitor_project_dir = str(monitor_status.get("project_dir") or "").strip()
    monitor_is_training = bool(monitor_status.get("is_training", False))
    monitor_matches = False

    if monitor_is_training:
        if monitor_project_name and monitor_project_name.strip().lower() == project_name.lower():
            monitor_matches = True
        elif monitor_project_dir:
            try:
                monitor_matches = str(Path(monitor_project_dir).resolve()).lower() == str(project_path).lower()
            except Exception:
                monitor_matches = False

    has_train_pid = bool(find_project_train_pids(project_path, include_launcher=False))
    if monitor_matches or has_train_pid:
        STATE["current"] = project_name
        p["status"] = "running"
        p["progress"] = float(p.get("progress", 0.0) or 0.0)
        p["pid"] = None
        if not p.get("last_start"):
            p["last_start"] = now_str()
        if project_name in STATE["queue"]:
            STATE["queue"].remove(project_name)
        sync_queue_session_file_locked()
        bump_state_version_locked()
        start_recovered_current_watcher(project_name, project_path)
        return True

    final_status, returncode = infer_completed_project_status(project_path)
    p["status"] = final_status
    p["progress"] = 100.0 if final_status == "success" else 0.0
    p["last_end"] = now_str()
    p["last_returncode"] = returncode
    p["pid"] = None
    if str(STATE.get("current") or "") == project_name:
        STATE["current"] = None
    record_train_history_locked(project_name, final_status, returncode)
    bump_state_version_locked()

    return False


def finish_recovered_current_project(project_name: str, rc: int | None = None):
    project_name = str(project_name or "").strip()
    if not project_name:
        return

    with state_lock:
        info = STATE["projects"].get(project_name)
        if not info:
            return
        if str(info.get("status") or "").strip() != "running" and str(STATE.get("current") or "").strip() != project_name:
            return

        project_path_raw = str(info.get("path") or "").strip()
        if project_path_raw:
            try:
                inferred_status, inferred_rc = infer_completed_project_status(Path(project_path_raw).resolve())
            except Exception:
                inferred_status, inferred_rc = "stopped", -15
        else:
            inferred_status, inferred_rc = "stopped", -15

        if rc is None:
            rc = inferred_rc

        if int(rc) == 0:
            final_status = "success"
            final_rc = 0
        elif int(rc) == -15:
            final_status = inferred_status if inferred_status == "success" else "stopped"
            final_rc = inferred_rc if inferred_status == "success" else -15
        else:
            final_status = inferred_status if inferred_status == "success" else "failed"
            final_rc = inferred_rc if inferred_status == "success" else int(rc)

        info["status"] = final_status
        info["progress"] = 100.0 if final_status == "success" else 0.0
        info["last_end"] = now_str()
        info["last_returncode"] = final_rc
        info["pid"] = None
        info["last_log"].append(f"[{now_str()}] TRAIN RECOVERED AFTER APP RESTART: {final_status}, rc={final_rc}")

        if str(STATE.get("current") or "").strip() == project_name:
            STATE["current"] = None

        history = STATE.get("history") or []
        last_history = history[-1] if history else None
        if not (
            isinstance(last_history, dict)
            and str(last_history.get("project") or "") == project_name
            and str(last_history.get("status") or "") == final_status
        ):
            record_train_history_locked(project_name, final_status, final_rc)

        sync_queue_session_file_locked()
        bump_state_version_locked()

    mark_current_train_control(None, None, pid=None, stop_requested=False)
    notify_train_finished(project_name, final_status, final_rc)
    start_worker_if_needed()


def start_recovered_current_watcher(project_name: str, project_path: Path):
    def _watch():
        mark_current_train_control(project_name, project_path, pid=None, stop_requested=False)
        try:
            rc = wait_train_completion_from_monitor(
                project_name,
                project_path,
                process=None,
                assume_started=True,
            )
        except Exception:
            rc = None
        finish_recovered_current_project(project_name, rc)

    threading.Thread(target=_watch, daemon=True).start()


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
        sync_queue_session_file_locked()
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
    project_name = str(project_name or "").strip()
    if not project_name:
        return None

    with state_lock:
        info = STATE["projects"].get(project_name)
        if not info or not info.get("path"):
            info = None
        try:
            if info:
                resolved = Path(info["path"]).resolve()
                if resolved.exists() and resolved.is_dir() and is_path_inside(resolved, ROOT_DIR):
                    return resolved
        except Exception:
            pass

    direct_path = (ROOT_DIR / project_name).resolve()
    if direct_path.exists() and direct_path.is_dir() and is_path_inside(direct_path, ROOT_DIR):
        return direct_path

    try:
        project_name_lower = project_name.casefold()
        for child in ROOT_DIR.iterdir():
            if not child.is_dir():
                continue
            if child.name.casefold() == project_name_lower and is_path_inside(child.resolve(), ROOT_DIR):
                return child.resolve()
    except Exception:
        pass

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
        return candidates[0], None

    target = candidates[0]
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


def count_boxes_in_label_file(label_file: Path):
    if not label_file or not label_file.exists() or not label_file.is_file():
        return 0
    try:
        raw = label_file.read_text(encoding="utf-8")
    except Exception:
        return 0

    count = 0
    for line in raw.replace("\r\n", "\n").split("\n"):
        s = line.strip()
        if not s:
            continue
        parts = s.split()
        if len(parts) < 5:
            continue
        try:
            float(parts[0]); float(parts[1]); float(parts[2]); float(parts[3]); float(parts[4])
        except Exception:
            continue
        count += 1
    return count


def parse_yolo_label_rows(text: str):
    rows = []
    for line_index, line in enumerate(str(text or "").replace("\r\n", "\n").split("\n")):
        s = line.strip()
        if not s:
            continue
        parts = s.split()
        if len(parts) < 5:
            continue
        try:
            cls = int(float(parts[0]))
            cx = float(parts[1])
            cy = float(parts[2])
            w = float(parts[3])
            h = float(parts[4])
        except Exception:
            continue
        rows.append({
            "line_index": line_index,
            "cls": cls,
            "cx": cx,
            "cy": cy,
            "w": w,
            "h": h,
        })
    return rows


def normalize_bbox_class_name(name: str):
    raw = str(name or "").strip()
    if not raw:
        return ""
    cleaned = re.sub(r"(?:[_\-\s]+)?(?:OK|NG)$", "", raw, flags=re.IGNORECASE).strip(" _-")
    return cleaned or raw


def enrich_yolo_rows_with_class_info(rows: list, class_names: list):
    enriched = []
    for row in rows or []:
        clean_row = dict(row or {})
        cls = int(clean_row.get("cls", -1))
        cls_name = class_names[cls] if 0 <= cls < len(class_names) else f"class_{cls}"
        clean_row["cls_name"] = cls_name
        clean_row["group_name"] = normalize_bbox_class_name(cls_name)
        enriched.append(clean_row)
    return enriched


def build_bbox_group_stats_from_images(image_rows: dict):
    group_values = {}
    for rows in (image_rows or {}).values():
        for row in rows or []:
            group_name = str(row.get("group_name", "") or "")
            bucket = group_values.setdefault(group_name, {"cx": [], "cy": [], "w": [], "h": []})
            bucket["cx"].append(float(row["cx"]))
            bucket["cy"].append(float(row["cy"]))
            bucket["w"].append(float(row["w"]))
            bucket["h"].append(float(row["h"]))

    group_stats = {}
    for group_name, metrics in group_values.items():
        stat_row = {"count": len(metrics["cx"])}
        for metric_name in ("cx", "cy", "w", "h"):
            vals = metrics[metric_name]
            if not vals:
                stat_row[f"{metric_name}_mean"] = 0.0
                stat_row[f"{metric_name}_std"] = 0.0
                continue
            mean = sum(vals) / len(vals)
            variance = sum((v - mean) ** 2 for v in vals) / len(vals)
            stat_row[f"{metric_name}_mean"] = mean
            stat_row[f"{metric_name}_std"] = math.sqrt(max(0.0, variance))
        group_stats[group_name] = stat_row
    return group_stats


def invalidate_project_bbox_analysis(project_name: str):
    key = str(project_name or "").strip()
    if not key:
        return
    with bbox_analysis_cache_lock:
        BBOX_ANALYSIS_CACHE.pop(key, None)


def build_project_bbox_analysis(project_name: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return {
            "project": project_name,
            "class_stats": {},
            "images": {},
        }

    _, rel_images = list_project_images(project_path, limit=0)
    class_names, _ = load_project_classes(project_path)
    image_rows = {}

    for rel in rel_images:
        label_file, _ = resolve_label_file_for_image(project_name, rel, create_missing=False)
        text = ""
        if label_file and label_file.exists() and label_file.is_file():
            try:
                text = label_file.read_text(encoding="utf-8")
            except Exception:
                text = ""
        image_rows[rel] = enrich_yolo_rows_with_class_info(parse_yolo_label_rows(text), class_names)

    group_stats = build_bbox_group_stats_from_images(image_rows)

    images = {}
    for rel, rows in image_rows.items():
        images[rel] = {
            "bbox_count": len(rows),
            "rows": rows,
        }

    return {
        "project": project_name,
        "class_names": class_names,
        "group_stats": group_stats,
        "images": images,
    }


def update_project_bbox_analysis_image(project_name: str, rel: str, text: str):
    key = str(project_name or "").strip()
    rel_key = str(rel or "").strip().replace("\\", "/")
    if not key or not rel_key:
        return

    with bbox_analysis_cache_lock:
        cached = BBOX_ANALYSIS_CACHE.get(key)
        if cached is None:
            return
        class_names = list(cached.get("class_names") or [])
        raw_images = dict(cached.get("images") or {})
        image_rows = {
            image_rel: list((info or {}).get("rows") or [])
            for image_rel, info in raw_images.items()
        }
        rows = enrich_yolo_rows_with_class_info(parse_yolo_label_rows(text), class_names)
        image_rows[rel_key] = rows
        cached["images"] = {
            image_rel: {
                "bbox_count": len(rows_for_image or []),
                "rows": rows_for_image or [],
            }
            for image_rel, rows_for_image in image_rows.items()
        }
        cached["group_stats"] = build_bbox_group_stats_from_images(image_rows)


def evaluate_bbox_image_anomalies(group_stats: dict, image_info: dict, sensitivity: str = "medium"):
    rows = list((image_info or {}).get("rows") or [])
    sensitivity_key = str(sensitivity or "medium").strip().lower()
    sensitivity_map = {
        "low": {"sigma_mult": 4.2, "floor_mult": 1.35},
        "medium": {"sigma_mult": 3.0, "floor_mult": 1.0},
        "high": {"sigma_mult": 2.2, "floor_mult": 0.78},
    }
    cfg = sensitivity_map.get(sensitivity_key, sensitivity_map["medium"])
    metric_floors = {
        "cx": 0.12 * cfg["floor_mult"],
        "cy": 0.12 * cfg["floor_mult"],
        "w": 0.10 * cfg["floor_mult"],
        "h": 0.10 * cfg["floor_mult"],
    }

    anomaly_items = []
    for idx, row in enumerate(rows):
        cls = int(row.get("cls", -1))
        cls_name = str(row.get("cls_name", "") or f"class_{cls}")
        group_name = str(row.get("group_name", "") or normalize_bbox_class_name(cls_name))
        stats = group_stats.get(group_name) or {}
        if int(stats.get("count", 0) or 0) < 8:
            continue

        reasons = []
        for metric_name, floor in metric_floors.items():
            mean = float(stats.get(f"{metric_name}_mean", 0.0) or 0.0)
            std = float(stats.get(f"{metric_name}_std", 0.0) or 0.0)
            value = float(row.get(metric_name, 0.0) or 0.0)
            tolerance = max(std * float(cfg["sigma_mult"]), floor)
            if abs(value - mean) > tolerance:
                reasons.append({
                    "metric": metric_name,
                    "value": value,
                    "mean": mean,
                    "std": std,
                    "tolerance": tolerance,
                })

        own_score = 0.0
        best_group = group_name
        best_score = float("inf")
        for candidate_group, candidate_stats in (group_stats or {}).items():
            if int(candidate_stats.get("count", 0) or 0) < 8:
                continue
            score = 0.0
            for metric_name, floor in metric_floors.items():
                mean = float(candidate_stats.get(f"{metric_name}_mean", 0.0) or 0.0)
                std = float(candidate_stats.get(f"{metric_name}_std", 0.0) or 0.0)
                value = float(row.get(metric_name, 0.0) or 0.0)
                tolerance = max(std * float(cfg["sigma_mult"]), floor)
                score += abs(value - mean) / max(tolerance, 1e-6)
            if candidate_group == group_name:
                own_score = score
            if score < best_score:
                best_score = score
                best_group = candidate_group

        if best_group != group_name and (own_score - best_score) >= 1.0 and best_score <= max(2.2, own_score * 0.75):
            reasons.append({
                "metric": "name",
                "value": cls_name,
                "group_name": group_name,
                "best_group": best_group,
                "own_score": own_score,
                "best_score": best_score,
            })

        if reasons:
            anomaly_items.append({
                "box_index": idx,
                "line_index": int(row.get("line_index", idx)),
                "cls": cls,
                "cls_name": cls_name,
                "group_name": group_name,
                "reasons": reasons,
            })

    return {
        "bbox_count": len(rows),
        "anomaly_count": len(anomaly_items),
        "has_anomaly": bool(anomaly_items),
        "anomaly_box_indices": [int(x["box_index"]) for x in anomaly_items],
        "anomaly_items": anomaly_items,
        "sensitivity": sensitivity_key,
    }


def get_project_bbox_analysis(project_name: str):
    key = str(project_name or "").strip()
    if not key:
        return {"project": "", "class_stats": {}, "images": {}}
    with bbox_analysis_cache_lock:
        cached = BBOX_ANALYSIS_CACHE.get(key)
        if cached is not None:
            return cached
    analysis = build_project_bbox_analysis(key)
    with bbox_analysis_cache_lock:
        BBOX_ANALYSIS_CACHE[key] = analysis
    return analysis


def resolve_source_image_rel_from_validation_sample(project_path: Path, sample_row: dict):
    image_dir = find_project_image_dir(project_path)
    if not image_dir:
        return None

    valid_image_root = project_path / "valid" / "images"
    rel_candidates = []

    val_rel = str(sample_row.get("val_image_rel_path", "") or "").strip().replace("\\", "/")
    if val_rel:
        rel_candidates.append(val_rel)

    image_name = str(sample_row.get("image_name", "") or "").strip()
    if image_name:
        rel_candidates.append(image_name)

    seen = set()
    for rel in rel_candidates:
        rel = str(rel or "").strip().replace("\\", "/")
        if not rel or rel in seen:
            continue
        seen.add(rel)
        source_candidate = (image_dir / rel).resolve()
        if is_path_inside(source_candidate, image_dir) and source_candidate.exists() and source_candidate.is_file():
            try:
                return source_candidate.relative_to(image_dir).as_posix()
            except Exception:
                return rel

        valid_candidate = (valid_image_root / rel).resolve()
        if is_path_inside(valid_candidate, valid_image_root) and valid_candidate.exists() and valid_candidate.is_file():
            source_from_valid = (image_dir / rel).resolve()
            if is_path_inside(source_from_valid, image_dir) and source_from_valid.exists() and source_from_valid.is_file():
                try:
                    return source_from_valid.relative_to(image_dir).as_posix()
                except Exception:
                    return rel

    if image_name:
        matches = []
        try:
            for f in image_dir.rglob("*"):
                if f.is_file() and f.name == image_name and f.suffix.lower() in IMAGE_EXTS:
                    matches.append(f)
        except Exception:
            matches = []
        if len(matches) == 1:
            try:
                return matches[0].relative_to(image_dir).as_posix()
            except Exception:
                return image_name

    return None


def promote_validation_sample_to_train(project_name: str, source_rel: str, valid_rel: str | None = None, text: str | None = None):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Không tìm thấy project", None

    source_image_file, err = resolve_project_image_file(project_name, source_rel)
    if err or not source_image_file:
        return False, "Không tìm thấy ảnh nguồn trong image", None

    label_file, err = resolve_label_file_for_image(project_name, source_rel, create_missing=True)
    if err or not label_file:
        return False, err or "Không tạo được label nguồn", None

    normalized_text = None if text is None else str(text).replace("\r\n", "\n")
    if normalized_text is not None:
        try:
            label_file.write_text(normalized_text, encoding="utf-8")
        except Exception:
            return False, "Không lưu được label nguồn", None

    rel_for_dataset = str(valid_rel or source_rel or "").strip().replace("\\", "/")
    if not rel_for_dataset:
        return False, "Thiếu đường dẫn valid/source", None

    rel_path = Path(rel_for_dataset)
    train_image_root = (project_path / "train" / "images").resolve()
    train_label_root = (project_path / "train" / "labels").resolve()
    valid_image_root = (project_path / "valid" / "images").resolve()
    valid_label_root = (project_path / "valid" / "labels").resolve()

    train_image_file = (train_image_root / rel_path).resolve()
    train_label_file = (train_label_root / rel_path).with_suffix(".txt").resolve()
    valid_image_file = (valid_image_root / rel_path).resolve()
    valid_label_file = (valid_label_root / rel_path).with_suffix(".txt").resolve()

    for candidate, root in (
        (train_image_file, train_image_root),
        (train_label_file, train_label_root),
        (valid_image_file, valid_image_root),
        (valid_label_file, valid_label_root),
    ):
        if not is_path_inside(candidate, root):
            return False, "Đường dẫn dataset không hợp lệ", None

    try:
        train_image_file.parent.mkdir(parents=True, exist_ok=True)
        train_label_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_image_file, train_image_file)
        shutil.copy2(label_file, train_label_file)
    except Exception as e:
        return False, f"Không copy được sang train: {e}", None

    removed_valid_image = False
    removed_valid_label = False
    try:
        if valid_image_file.exists() and valid_image_file.is_file():
            valid_image_file.unlink()
            removed_valid_image = True
    except Exception as e:
        return False, f"Không xóa được ảnh khỏi valid: {e}", None

    try:
        if valid_label_file.exists() and valid_label_file.is_file():
            valid_label_file.unlink()
            removed_valid_label = True
    except Exception as e:
        return False, f"Không xóa được label khỏi valid: {e}", None

    return True, "Đã relabel và chuyển ảnh sang train", {
        "source_rel": str(source_rel),
        "valid_rel": rel_for_dataset,
        "train_image": str(train_image_file),
        "train_label": str(train_label_file),
        "removed_valid_image": removed_valid_image,
        "removed_valid_label": removed_valid_label,
    }


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


def split_dataset_rows_for_train_all(image_rows: list, clean_cfg: dict, split_mode: str = "count"):
    rows = list(image_rows)
    if clean_cfg.get("shuffle", True):
        random.Random(int(clean_cfg.get("seed", 42))).shuffle(rows)

    if str(split_mode or "").strip().lower() == "class":
        groups = {}
        for row in rows:
            key = int(row.get("primary_class", -1))
            groups.setdefault(key, []).append(row)

        class_keys = sorted(groups.keys())
        if clean_cfg.get("shuffle", True):
            random.Random(int(clean_cfg.get("seed", 42))).shuffle(class_keys)

        total_groups = len(class_keys)
        valid_group_count = int(round(total_groups * clean_cfg["valid_percent"] / 100.0))
        test_group_count = int(round(total_groups * clean_cfg["test_percent"] / 100.0))
        valid_group_count = max(0, min(valid_group_count, total_groups))
        test_group_count = max(0, min(test_group_count, total_groups - valid_group_count))

        valid_keys = class_keys[:valid_group_count]
        test_keys = class_keys[valid_group_count:valid_group_count + test_group_count]
        valid_rows = []
        test_rows = []
        for key in valid_keys:
            valid_rows.extend(groups.get(key, []))
        for key in test_keys:
            test_rows.extend(groups.get(key, []))

        return {
            "train": rows,
            "valid": valid_rows,
            "test": test_rows,
        }

    total = len(rows)
    valid_count = int(round(total * clean_cfg["valid_percent"] / 100.0))
    test_count = int(round(total * clean_cfg["test_percent"] / 100.0))
    valid_count = max(0, min(valid_count, total))
    test_count = max(0, min(test_count, total - valid_count))

    return {
        "train": rows,
        "valid": rows[:valid_count],
        "test": rows[valid_count:valid_count + test_count],
    }


def create_dataset_for_project(project_name: str, cfg: dict, split_mode: str = "count", progress_cb=None):
    ok, err, clean_cfg = validate_dataset_config(cfg)
    if not ok:
        return False, err, None

    if callable(progress_cb):
        try:
            progress_cb(4.0, "Validating dataset config", project_name)
        except Exception:
            pass

    ok, err, project_path, image_rows = collect_dataset_source_rows(project_name)
    if not ok:
        return False, err, None

    if callable(progress_cb):
        try:
            progress_cb(12.0, f"Collected {len(image_rows)} source image/label pairs", project_name)
        except Exception:
            pass

    split_mode = str(split_mode or "count").strip().lower()
    if clean_cfg.get("train_all_data"):
        buckets = split_dataset_rows_for_train_all(image_rows, clean_cfg, split_mode=split_mode)
    elif split_mode == "class":
        buckets = split_dataset_rows_by_class(image_rows, clean_cfg)
    else:
        buckets = split_dataset_rows_by_count(image_rows, clean_cfg)

    if callable(progress_cb):
        try:
            progress_cb(18.0, "Clearing old dataset folders", project_name)
        except Exception:
            pass

    def _clear_progress(done, total, folder_name, stage):
        if not callable(progress_cb):
            return
        pct = 18.0 + ((float(done or 0) / max(1.0, float(total or 1))) * 10.0)
        label = "Removing" if stage != "done" else "Removed"
        progress_cb(pct, f"{label} dataset folder: {folder_name}", project_name)

    clear_project_dataset_dirs(project_path, progress_cb=_clear_progress)

    total_copy = sum(len(rows) for rows in buckets.values())
    copied = 0

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
            copied += 1
            if callable(progress_cb):
                try:
                    pct = 30.0 + ((float(copied) / max(1.0, float(total_copy or 1))) * 58.0)
                    progress_cb(pct, f"Copying {split_name}: {copied}/{total_copy}", rel_path.as_posix())
                except Exception:
                    pass
        created_counts[split_name] = count

    if callable(progress_cb):
        try:
            progress_cb(91.0, "Writing data.yaml", project_name)
        except Exception:
            pass

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

    if callable(progress_cb):
        try:
            progress_cb(96.0, "Saving dataset config", project_name)
        except Exception:
            pass

    ok, err, saved_cfg = save_dataset_config(project_path, clean_cfg)
    if not ok:
        return False, err, None

    return True, "Tạo dataset thành công", {
        "config": saved_cfg,
        "counts": created_counts,
        "total": len(image_rows),
        "split_mode": split_mode,
    }


def merge_train_valid_to_train(project_name: str, progress_cb=None):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Không tìm thấy project", None

    if callable(progress_cb):
        try:
            progress_cb(6.0, "Scanning train/valid folders", project_name)
        except Exception:
            pass

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

    if callable(progress_cb):
        try:
            progress_cb(18.0, f"Collected {len(source_rows)} files to merge", project_name)
        except Exception:
            pass

    train_root = project_path / "train"
    temp_root = Path(tempfile.mkdtemp(prefix="_train_all_data_", dir=str(project_path)))
    img_dst_root = temp_root / "images"
    lbl_dst_root = temp_root / "labels"
    img_dst_root.mkdir(parents=True, exist_ok=True)
    lbl_dst_root.mkdir(parents=True, exist_ok=True)

    try:
        copied = 0
        total_copy = len(source_rows)
        for row in source_rows:
            rel_path = Path(row["rel"])
            img_dst = img_dst_root / rel_path
            lbl_dst = (lbl_dst_root / rel_path).with_suffix(".txt")
            img_dst.parent.mkdir(parents=True, exist_ok=True)
            lbl_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(row["image_file"], img_dst)
            shutil.copy2(row["label_file"], lbl_dst)
            copied += 1
            if callable(progress_cb):
                try:
                    pct = 34.0 + ((float(copied) / max(1.0, float(total_copy or 1))) * 50.0)
                    progress_cb(pct, f"Merging train data: {copied}/{total_copy}", rel_path.as_posix())
                except Exception:
                    pass

        if train_root.exists() and train_root.is_dir():
            if callable(progress_cb):
                try:
                    progress_cb(88.0, "Replacing old train folder", str(train_root))
                except Exception:
                    pass
            shutil.rmtree(train_root)
        temp_root.replace(train_root)

        if callable(progress_cb):
            try:
                progress_cb(94.0, "Writing data.yaml", project_name)
            except Exception:
                pass

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
    except Exception as e:
        shutil.rmtree(temp_root, ignore_errors=True)
        return False, f"Không gộp được train all data: {e}", None

    return True, "Đã gộp train + valid vào train", {
        "counts": {
            "train": copied,
            "valid": count_images_in_dir(project_path / "valid" / "images"),
            "test": count_images_in_dir(project_path / "test" / "images"),
        }
    }


def find_output_dir(project_path: Path):
    for folder_name in ("Output", "output"):
        p = project_path / folder_name
        if p.exists() and p.is_dir():
            return p
    return None


def save_uploaded_project_zip(upload_file):
    return dataset_save_uploaded_project_zip(upload_file, ROOT_DIR, safe_project_name_part)


def import_project_data_zip(project_name: str, upload_file):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "project_not_found", None

    image_dir = find_project_image_dir(project_path)
    if not image_dir:
        image_dir = project_path / "image"
        try:
            image_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            return False, "cannot_create_image_dir", None

    if not upload_file:
        return False, "missing_file", None

    filename = str(upload_file.filename or "").strip()
    if not filename.lower().endswith(".zip"):
        return False, "only_zip_supported", None

    copied_images = 0
    copied_labels = 0

    with tempfile.TemporaryDirectory(prefix="traincontrol_import_") as tmp_dir:
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

        image_rows, label_rows = collect_importable_data_files(extract_root, IMAGE_EXTS)
        if not image_rows and not label_rows:
            return False, "no_data_found", None

        for src, rel_target in image_rows:
            dst = (image_dir / rel_target).resolve()
            if not is_path_inside(dst, image_dir):
                return False, "invalid_zip_path", None
            try:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
                copied_images += 1
            except Exception:
                return False, "copy_failed", None

        for src, rel_target in label_rows:
            dst = (image_dir / rel_target).resolve()
            if not is_path_inside(dst, image_dir):
                return False, "invalid_zip_path", None
            try:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
                copied_labels += 1
            except Exception:
                return False, "copy_failed", None

    return True, "imported", {
        "project": project_name,
        "images_added": copied_images,
        "labels_added": copied_labels,
        "image_dir": str(image_dir),
        "labels_dir": str(image_dir),
    }


def _unique_upload_target(folder: Path, filename: str) -> Path:
    safe_name = safe_download_part(Path(str(filename or "")).name)
    suffix = Path(safe_name).suffix
    if not suffix:
        safe_name = f"{safe_name}.jpg"
        suffix = ".jpg"
    stem = Path(safe_name).stem or "image"
    candidate = (folder / safe_name).resolve()
    idx = 2
    while candidate.exists():
        candidate = (folder / f"{stem}_{idx}{suffix}").resolve()
        idx += 1
    return candidate


def import_project_data_images(project_name: str, upload_files):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "project_not_found", None

    image_dir = find_project_image_dir(project_path)
    if not image_dir:
        image_dir = project_path / "image"
        try:
            image_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            return False, "cannot_create_image_dir", None

    files = [f for f in list(upload_files or []) if f and str(getattr(f, "filename", "") or "").strip()]
    if not files:
        return False, "missing_file", None

    copied_images = 0
    skipped_files = 0
    saved_names = []
    for upload_file in files:
        filename = str(upload_file.filename or "").strip()
        suffix = Path(filename).suffix.lower()
        if suffix not in IMAGE_EXTS:
            skipped_files += 1
            continue

        dst = _unique_upload_target(image_dir, filename)
        if not is_path_inside(dst, image_dir):
            return False, "invalid_upload_path", None
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            upload_file.save(str(dst))
            copied_images += 1
            if len(saved_names) < 20:
                saved_names.append(dst.name)
        except Exception:
            return False, "copy_failed", None

    if copied_images <= 0:
        return False, "no_image_found", {"skipped_files": skipped_files}

    return True, "imported", {
        "project": project_name,
        "images_added": copied_images,
        "labels_added": 0,
        "skipped_files": skipped_files,
        "image_dir": str(image_dir),
        "saved_names": saved_names,
    }


def get_output_model_train_weights_info(project_path: Path):
    output_dir = find_output_dir(project_path)
    model_train_dirs = []
    if output_dir:
        model_train_dirs = [
            d for d in sorted(output_dir.iterdir(), key=lambda p: p.name.lower())
            if d.is_dir() and d.name.lower().startswith("model_train")
        ]

    rows = []
    seen_paths = set()

    def _is_yolo_base_model(path: Path) -> bool:
        name = path.name.lower()
        return name.startswith("yolo") and name.endswith(".pt")

    def _append_model_file(file_path: Path, source_label: str = ""):
        try:
            resolved = file_path.resolve()
            if resolved in seen_paths:
                return
            if not resolved.is_file() or resolved.suffix.lower() != ".pt":
                return
            if _is_yolo_base_model(resolved):
                return
            if not is_path_inside(resolved, project_path):
                return
            st = resolved.stat()
            rel_project = resolved.relative_to(project_path).as_posix()
            run_folder = source_label
            if not run_folder:
                parent = resolved.parent
                run_folder = parent.parent.name if parent.name.lower() == "weights" else parent.name
            rows.append({
                "run_folder": run_folder,
                "file_name": resolved.name,
                "relative_path": rel_project,
                "project_relative_path": rel_project,
                "modified_at": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "modified_ts": st.st_mtime,
                "size_kb": round(st.st_size / 1024, 2),
                "_sort_ts": st.st_mtime
            })
            seen_paths.add(resolved)
        except Exception:
            return

    if output_dir:
        for run_dir in model_train_dirs:
            weights_dir = run_dir / "weights"
            if not weights_dir.exists() or not weights_dir.is_dir():
                continue
            for f in weights_dir.rglob("*.pt"):
                _append_model_file(f, run_dir.name)

    try:
        skip_dirs = {"train", "valid", "test", "runs", "__pycache__"}
        for root, dir_names, file_names in os.walk(project_path):
            current = Path(root)
            dir_names[:] = [
                d for d in dir_names
                if d.lower() not in skip_dirs and not d.startswith("_train_all_data_")
            ]
            for file_name in file_names:
                if not file_name.lower().endswith(".pt"):
                    continue
                _append_model_file(current / file_name)
    except Exception:
        pass

    rows.sort(key=lambda x: x["_sort_ts"], reverse=True)
    for row in rows:
        row.pop("_sort_ts", None)

    return {
        "exists": bool(output_dir) or bool(rows),
        "folder": output_dir.name if output_dir else None,
        "model_train_folders": [d.name for d in model_train_dirs],
        "items": rows
    }


def find_latest_best_weight_rel(project_name: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return None, "Project not found"

    output_info = get_output_model_train_weights_info(project_path)
    for item in list(output_info.get("items") or []):
        if str(item.get("file_name") or "").lower() == "best.pt":
            rel = str(item.get("relative_path") or item.get("project_relative_path") or "").strip().replace("\\", "/")
            if rel:
                return rel, None

    return None, "No best.pt found"


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
            "mis_as_other_classes": int(float(row.get("mis_as_other_classes", 0) or 0)),
            "missed_as_background": int(float(row.get("missed_as_background", 0) or 0)),
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
            "val_image_rel_path": str(row.get("val_image_rel_path", "") or "").strip().replace("\\", "/"),
            "gt_label_rel_path": str(row.get("gt_label_rel_path", "") or "").strip().replace("\\", "/"),
            "sample_rel_path": rel_path,
            "gt_class_id": str(row.get("gt_class_id", "") or "").strip(),
            "gt_class_name": str(row.get("gt_class_name", "") or "").strip(),
            "pred_class_id": str(row.get("pred_class_id", "") or "").strip(),
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


def build_testing_summary(project_path: Path, run_dir: Path):
    output_dir = find_output_dir(project_path)
    if not output_dir:
        return {"exists": False}

    testing_dir = (run_dir / "ModelTesting").resolve()
    if not testing_dir.exists() or not testing_dir.is_dir() or not is_path_inside(testing_dir, run_dir):
        return {"exists": False}

    results_csv = testing_dir / "results.csv"
    results_info = read_results_csv_preview(results_csv)
    results_summary = summarize_results_csv(results_csv)
    confusion_summary = summarize_confusion_analysis(testing_dir)

    sample_items = []
    sample_rows = list(confusion_summary.get("sample_items") or [])
    unique_sample_images = set()
    for row in sample_rows:
        rel_path = str(row.get("sample_rel_path", "") or "").strip().replace("\\", "/")
        if not rel_path:
            continue
        source_rel = resolve_source_image_rel_from_validation_sample(project_path, row)
        val_rel = str(row.get("val_image_rel_path", "") or "").strip().replace("\\", "/")
        if val_rel:
            unique_sample_images.add(val_rel)
        sample_items.append({
            **row,
            "relative_path": f"{run_dir.name}/ModelTesting/{rel_path}",
            "source_image_rel": source_rel,
            "can_label_source": bool(source_rel),
        })
    confusion_summary["sample_items"] = sample_items

    counts_csv = testing_dir / "misclassified_counts.csv"
    count_rows = read_csv_rows(counts_csv, max_rows=2000)
    total_gt_objects = 0
    total_correct_objects = 0
    total_error_objects = 0
    for row in count_rows:
        try:
            total_gt_objects += int(float(row.get("gt_total", 0) or 0))
            total_correct_objects += int(float(row.get("correct", 0) or 0))
            total_error_objects += int(float(row.get("total_errors", 0) or 0))
        except Exception:
            continue

    export_files = []
    for name in [
        "results.csv",
        "predictions_val.csv",
        "misclassified_counts.csv",
        "misclassified_pairs.csv",
        "misclassified_samples.csv",
        "per_class_metrics.csv",
        "metrics_summary.json",
    ]:
        file_path = testing_dir / name
        if not file_path.exists() or not file_path.is_file():
            continue
        export_files.append({
            "name": name,
            "relative_path": file_path.relative_to(output_dir).as_posix(),
        })

    return {
        "exists": bool(results_info.get("exists") or confusion_summary.get("exists") or export_files),
        "run_folder": run_dir.name,
        "testing_folder": "ModelTesting",
        "results_csv": {
            "relative_path": results_csv.relative_to(output_dir).as_posix() if results_csv.exists() else None,
            "summary": results_summary,
            **results_info,
        },
        "confusion_analysis": confusion_summary,
        "export_files": export_files,
        "summary_counts": {
            "valid_sample_images": len(unique_sample_images),
            "misclassified_sample_images": len({str(x.get("sample_rel_path", "") or "") for x in sample_rows if str(x.get("sample_rel_path", "") or "").strip()}),
            "total_gt_objects": total_gt_objects,
            "total_correct_objects": total_correct_objects,
            "total_error_objects": total_error_objects,
        },
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
        msa_testing = build_msa_testing_summary(project_path, run_dir)
        sample_items = []
        for row in list(confusion_summary.get("sample_items") or []):
            rel_path = str(row.get("sample_rel_path", "") or "").strip().replace("\\", "/")
            if not rel_path:
                continue
            source_rel = resolve_source_image_rel_from_validation_sample(project_path, row)
            sample_items.append({
                **row,
                "relative_path": f"{run_dir.name}/{rel_path}",
                "source_image_rel": source_rel,
                "can_label_source": bool(source_rel),
            })
        confusion_summary["sample_items"] = sample_items
        runs.append({
            "run_folder": run_dir.name,
            "image_files": image_items,
            "image_count": len(image_items),
            "confusion_analysis": confusion_summary,
            "model_testing": build_testing_summary(project_path, run_dir),
            "msa_testing": msa_testing,
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

    rel = str(relative_path or "").strip().replace("\\", "/")
    if not rel:
        return None, "missing_path"

    candidate = (project_path / rel).resolve()
    if not is_path_inside(candidate, project_path):
        return None, "invalid_path"
    if not candidate.exists() or not candidate.is_file():
        return None, "file_not_found"
    if candidate.suffix.lower() != ".pt":
        return None, "not_pt_file"
    if candidate.name.lower().startswith("yolo"):
        return None, "yolo_base_model_hidden"

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


def resolve_output_weight_file(project_name: str, relative_path: str):
    candidate, err = resolve_weight_download_file(project_name, relative_path)
    if err or not candidate:
        return None, err or "file_not_found"
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


def build_revalidate_status_payload():
    with revalidate_task_lock:
        return {
            "ok": True,
            **dict(REVALIDATE_TASK),
        }


def build_model_test_status_payload():
    with model_test_task_lock:
        return {
            "ok": True,
            **dict(MODEL_TEST_TASK),
        }


def build_predict_status_payload():
    with predict_task_lock:
        return {
            "ok": True,
            **dict(PREDICT_TASK),
        }


def build_project_fs_task_status_payload():
    with project_fs_task_lock:
        return {
            "ok": True,
            **dict(PROJECT_FS_TASK),
        }


def build_dataset_task_status_payload():
    with dataset_task_lock:
        return {
            "ok": True,
            **dict(DATASET_TASK),
        }


def _set_revalidate_task_locked(**kwargs):
    REVALIDATE_TASK.update(kwargs)


def _set_model_test_task_locked(**kwargs):
    MODEL_TEST_TASK.update(kwargs)


def _set_predict_task_locked(**kwargs):
    PREDICT_TASK.update(kwargs)


def _set_project_fs_task_locked(**kwargs):
    PROJECT_FS_TASK.update(kwargs)


def _set_dataset_task_locked(**kwargs):
    DATASET_TASK.update(kwargs)


def _run_revalidate_task(task_id: str, project_name: str, run_folder: str):
    with revalidate_task_lock:
        _set_revalidate_task_locked(
            progress=8.0,
            message="Preparing re-validation",
            detail=f"{project_name} | {run_folder}",
        )

    ok, message, payload = revalidate_run(project_name, run_folder)

    with revalidate_task_lock:
        if REVALIDATE_TASK.get("id") != task_id:
            return
        _set_revalidate_task_locked(
            status="success" if ok else "failed",
            progress=100.0 if ok else max(1.0, float(REVALIDATE_TASK.get("progress") or 0.0)),
            message=message,
            detail=f"{project_name} | {run_folder}",
            ended_at=time.time(),
            result=payload if ok else None,
        )


def start_revalidate_task(project_name: str, run_folder: str):
    with revalidate_task_lock:
        if REVALIDATE_TASK.get("status") == "running":
            same_task = (
                str(REVALIDATE_TASK.get("project") or "") == str(project_name or "")
                and str(REVALIDATE_TASK.get("run_folder") or "") == str(run_folder or "")
            )
            if same_task:
                return True, "Re-validation is already running", {
                    "task_id": REVALIDATE_TASK.get("id"),
                    "status": REVALIDATE_TASK.get("status"),
                }
            return False, "Another re-validation is already running", None

        task_id = f"revalidate-{int(time.time() * 1000)}"
        _set_revalidate_task_locked(
            id=task_id,
            project=project_name,
            run_folder=run_folder,
            status="running",
            progress=3.0,
            message="Starting re-validation",
            detail=f"{project_name} | {run_folder}",
            started_at=time.time(),
            ended_at=0.0,
            result=None,
        )

    thread = threading.Thread(
        target=_run_revalidate_task,
        args=(task_id, project_name, run_folder),
        daemon=True,
    )
    thread.start()
    return True, "Re-validation started", {
        "task_id": task_id,
        "status": "running",
    }


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
        with revalidate_task_lock:
            if REVALIDATE_TASK.get("status") == "running":
                _set_revalidate_task_locked(
                    progress=15.0,
                    message="Loading validation module",
                    detail=f"{project_name} | {run_folder}",
                )
        module = load_train_module()
        append_log(project_name, f"[{now_str()}] REVALIDATE START: {run_folder}")
        with revalidate_task_lock:
            if REVALIDATE_TASK.get("status") == "running":
                _set_revalidate_task_locked(
                    progress=35.0,
                    message="Running validation export",
                    detail=f"{project_name} | {run_folder}",
                )
        info = module.validate_and_export(
            weights=str(weights_path),
            data_yaml=str(data_yaml),
            img_size=int(args_info.get("imgsz", 640) or 640),
            device=args_info.get("device", 0),
            iou=0.65,
            conf=0.50,
            output_dir=str(run_dir),
        )
        with revalidate_task_lock:
            if REVALIDATE_TASK.get("status") == "running":
                _set_revalidate_task_locked(
                    progress=92.0,
                    message="Refreshing artifacts",
                    detail=f"{project_name} | {run_folder}",
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


def get_model_testing_dir(project_name: str, run_folder: str):
    run_dir, err = resolve_run_dir(project_name, run_folder)
    if err or not run_dir:
        return None, err or "run_not_found"
    testing_dir = (run_dir / "ModelTesting").resolve()
    if not is_path_inside(testing_dir, run_dir):
        return None, "invalid_testing_dir"
    return testing_dir, None


def run_model_testing(project_name: str, run_folder: str):
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
    testing_dir = (run_dir / "ModelTesting").resolve()
    if not is_path_inside(testing_dir, run_dir):
        return False, "Invalid testing directory", None

    try:
        if testing_dir.exists() and testing_dir.is_dir():
            shutil.rmtree(testing_dir)
        testing_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        return False, f"Cannot prepare ModelTesting dir: {e}", None

    try:
        with model_test_task_lock:
            if MODEL_TEST_TASK.get("status") == "running":
                _set_model_test_task_locked(
                    progress=18.0,
                    message="Loading testing module",
                    detail=f"{project_name} | {run_folder}",
                )
        module = load_train_module()
        append_log(project_name, f"[{now_str()}] MODEL TEST START: {run_folder}")
        with model_test_task_lock:
            if MODEL_TEST_TASK.get("status") == "running":
                _set_model_test_task_locked(
                    progress=40.0,
                    message="Predicting valid split",
                    detail=f"{project_name} | {run_folder}",
                )
        info = module.validate_and_export(
            weights=str(weights_path),
            data_yaml=str(data_yaml),
            img_size=int(args_info.get("imgsz", 640) or 640),
            device=args_info.get("device", 0),
            iou=0.65,
            conf=0.50,
            output_dir=str(testing_dir),
        )
        append_log(project_name, f"[{now_str()}] MODEL TEST DONE: {run_folder}")
        return True, f"Model testing completed for {run_folder}", {
            "run_folder": run_folder,
            "artifacts_dir": str(testing_dir),
            "info": info,
        }
    except Exception as e:
        append_log(project_name, f"[{now_str()}] MODEL TEST FAILED: {run_folder}: {e}")
        return False, f"Model testing failed: {e}", None


def _read_yolo_label_rows(text: str):
    rows = []
    for line in str(text or "").replace("\r\n", "\n").split("\n"):
        s = line.strip()
        if not s:
            continue
        parts = s.split()
        if len(parts) < 5:
            continue
        try:
            cls = int(float(parts[0]))
            cx = float(parts[1])
            cy = float(parts[2])
            w = float(parts[3])
            h = float(parts[4])
        except Exception:
            continue
        try:
            confidence = float(parts[5]) if len(parts) > 5 else None
        except (ValueError, TypeError):
            confidence = None
        rows.append({
            "cls": cls,
            "cx": cx,
            "cy": cy,
            "w": w,
            "h": h,
            **({"confidence": confidence} if confidence is not None else {}),
        })
    return rows


def _read_yolo_label_file(label_file: Path | None):
    if not label_file or not label_file.exists() or not label_file.is_file():
        return []
    try:
        return _read_yolo_label_rows(label_file.read_text(encoding="utf-8"))
    except Exception:
        return []


def _load_class_names(data_yaml_path: Path):
    if not data_yaml_path or not data_yaml_path.exists() or not data_yaml_path.is_file():
        return {}
    try:
        import yaml
        data = yaml.safe_load(data_yaml_path.read_text(encoding="utf-8")) or {}
        names = data.get("names", {})
        if isinstance(names, list):
            return {i: str(x).strip() for i, x in enumerate(names) if str(x).strip()}
        if isinstance(names, dict):
            result = {}
            for k, v in names.items():
                try:
                    result[int(k)] = str(v).strip()
                except Exception:
                    pass
            return result
    except Exception:
        pass
    return {}



def _draw_label_overlay(image_path: Path, label_rows: list[dict], class_names: dict, output_path: Path, cv2_module):
    if cv2_module is None:
        try:
            output_path.write_bytes(image_path.read_bytes())
        except Exception:
            pass
        return

    try:
        img = cv2_module.imread(str(image_path))
        if img is None:
            output_path.write_bytes(image_path.read_bytes())
            return
        h, w = img.shape[:2]
        for row in label_rows:
            x1 = int(max(0, (row["cx"] - row["w"] / 2.0) * w))
            y1 = int(max(0, (row["cy"] - row["h"] / 2.0) * h))
            x2 = int(min(w - 1, (row["cx"] + row["w"] / 2.0) * w))
            y2 = int(min(h - 1, (row["cy"] + row["h"] / 2.0) * h))
            cls_name = str(class_names.get(int(row["cls"]), str(row["cls"])))
            cv2_module.rectangle(img, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2_module.putText(img, cls_name, (x1, max(12, y1 - 6)), cv2_module.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1, cv2_module.LINE_AA)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cv2_module.imwrite(str(output_path), img)
    except Exception:
        try:
            output_path.write_bytes(image_path.read_bytes())
        except Exception:
            pass


def _collect_msa_sample_pairs(sample_root: Path):
    image_files = {}
    label_files = {}
    for path in sample_root.rglob("*"):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix in IMAGE_EXTS:
            image_files[path.stem] = path
        elif suffix == ".txt":
            label_files[path.stem] = path
    pairs = []
    for stem, img_file in sorted(image_files.items()):
        label_file = label_files.get(stem)
        if label_file:
            pairs.append((stem, img_file, label_file))
    return pairs


def msa_root_for_project(project_name: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.is_dir():
        return None
    return project_path / "MSA"


def list_msa_models(project_name: str):
    project_path = resolve_project_path(project_name)
    output_dir = find_output_dir(project_path) if project_path else None
    if not output_dir:
        return []
    models = []
    for run_dir in sorted(output_dir.iterdir(), key=lambda p: p.name.lower()):
        if not run_dir.is_dir():
            continue
        for name in ("best.pt", "last.pt"):
            if (run_dir / "weights" / name).is_file():
                models.append({"run_folder": run_dir.name, "weight": name})
    return models


def import_msa_zip(project_name: str, upload_file, progress_cb=None):
    msa_root = msa_root_for_project(project_name)
    if msa_root is None:
        return False, "Project not found", None
    with tempfile.TemporaryDirectory(prefix="msa_import_") as tmp_name:
        tmp_root = Path(tmp_name)
        try:
            zip_path = tmp_root / "upload.zip"
            if isinstance(upload_file, Path):
                shutil.copy2(upload_file, zip_path)
            else:
                upload_file.save(str(zip_path))
            with zipfile.ZipFile(zip_path) as archive:
                files = [entry for entry in archive.infolist() if not entry.is_dir()]
                if len(files) > 20000 or sum(entry.file_size for entry in files) > 2 * 1024**3:
                    return False, "MSA ZIP is too large", None
                if progress_cb:
                    progress_cb(5, "Kiểm tra ZIP", 0, len(files))
                for index, entry in enumerate(files, 1):
                    parts = Path(entry.filename.replace("\\", "/")).parts
                    if not parts or entry.filename.startswith("/") or ".." in parts or ":" in parts[0]:
                        return False, "Invalid ZIP path", None
                    suffix = Path(parts[-1]).suffix.lower()
                    if suffix not in IMAGE_EXTS and suffix != ".txt":
                        continue
                    target = (tmp_root / Path(*parts)).resolve()
                    if not is_path_inside(target, tmp_root):
                        return False, "Invalid ZIP path", None
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(entry) as source, target.open("wb") as dest:
                        shutil.copyfileobj(source, dest)
                    if progress_cb:
                        progress_cb(5 + round(65 * index / max(1, len(files))), "Giải nén ZIP", index, len(files))
        except (zipfile.BadZipFile, OSError, ValueError) as exc:
            return False, f"Cannot read MSA ZIP: {exc}", None

        image_files = {}
        label_files = {}
        for path in tmp_root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() in IMAGE_EXTS:
                image_files[path.stem] = path
            elif path.suffix.lower() == ".txt":
                label_files[path.stem] = path
        if not image_files:
            return False, "ZIP must contain MSA images", None
        image_dir = msa_root / "images"
        label_dir = msa_root / "labels"
        image_dir.mkdir(parents=True, exist_ok=True)
        label_dir.mkdir(parents=True, exist_ok=True)
        for stem, image_path in image_files.items():
            for old_image in image_dir.glob(f"{stem}.*"):
                if old_image.suffix.lower() in IMAGE_EXTS and old_image.name != image_path.name:
                    return False, f"Image name already exists with another extension: {stem}", None
        labeled = 0
        for index, (stem, image_path) in enumerate(sorted(image_files.items()), 1):
            shutil.copy2(image_path, image_dir / image_path.name)
            label_path = label_files.get(stem)
            if label_path:
                shutil.copy2(label_path, label_dir / f"{stem}.txt")
                labeled += 1
            if progress_cb:
                progress_cb(70 + round(30 * index / len(image_files)), "Nhập mẫu MSA", index, len(image_files))
        return True, f"Imported {len(image_files)} MSA images ({labeled} labels)", {
            "count": len(image_files), "labeled": labeled, "unlabeled": len(image_files) - labeled,
        }


def msa_box_iou(a: dict, b: dict) -> float:
    ax1, ay1 = a["cx"] - a["w"] / 2, a["cy"] - a["h"] / 2
    ax2, ay2 = a["cx"] + a["w"] / 2, a["cy"] + a["h"] / 2
    bx1, by1 = b["cx"] - b["w"] / 2, b["cy"] - b["h"] / 2
    bx2, by2 = b["cx"] + b["w"] / 2, b["cy"] + b["h"] / 2
    overlap = max(0.0, min(ax2, bx2) - max(ax1, bx1)) * max(0.0, min(ay2, by2) - max(ay1, by1))
    union = a["w"] * a["h"] + b["w"] * b["h"] - overlap
    return overlap / union if union > 0 else 0.0


def classify_msa_boxes(gt_rows: list, pred_rows: list) -> tuple[list[str], list[int]]:
    candidates = sorted(
        ((msa_box_iou(gt, pred), gi, pi) for gi, gt in enumerate(gt_rows)
         for pi, pred in enumerate(pred_rows)), reverse=True
    )
    matched_gt, matched_pred = set(), set()
    pred_statuses = ["extra"] * len(pred_rows)
    missed_gt = set(range(len(gt_rows)))
    for iou, gi, pi in candidates:
        if iou < 0.5:
            break
        if gi in matched_gt or pi in matched_pred:
            continue
        matched_gt.add(gi)
        matched_pred.add(pi)
        if int(gt_rows[gi]["cls"]) == int(pred_rows[pi]["cls"]):
            pred_statuses[pi] = "correct"
            missed_gt.discard(gi)
        else:
            pred_statuses[pi] = "wrong_class"
    return pred_statuses, sorted(missed_gt)


def compare_msa_boxes(gt_rows: list, pred_rows: list, class_names: dict) -> list:
    pred_statuses, missed_gt = classify_msa_boxes(gt_rows, pred_rows)
    counts = {}

    def add(class_id: int, field: str):
        item = counts.setdefault(class_id, {"class_id": class_id,
                                            "class_name": class_names.get(class_id, str(class_id)),
                                            "missed": 0, "extra": 0})
        item[field] += 1

    for gi in missed_gt:
        add(int(gt_rows[gi]["cls"]), "missed")
    for pi, pred in enumerate(pred_rows):
        if pred_statuses[pi] != "correct":
            add(int(pred["cls"]), "extra")
    return [counts[key] for key in sorted(counts)]


def run_project_msa_test(project_name: str, run_folder: str, weight_name: str, progress_cb=None):
    msa_root = msa_root_for_project(project_name)
    run_dir, err = resolve_run_dir(project_name, run_folder)
    if msa_root is None or err or not run_dir:
        return False, "Project or model run not found", None
    if weight_name not in {"best.pt", "last.pt"}:
        return False, "Invalid model weight", None
    weights_path = run_dir / "weights" / weight_name
    if not weights_path.is_file():
        return False, "Model weight not found", None
    image_dir = msa_root / "images"
    label_dir = msa_root / "labels"
    pairs = []
    if image_dir.is_dir() and label_dir.is_dir():
        all_images = [image for image in sorted(image_dir.iterdir())
                      if image.is_file() and image.suffix.lower() in IMAGE_EXTS]
        missing_labels = [image.name for image in all_images
                          if not (label_dir / f"{image.stem}.txt").is_file()]
        if missing_labels:
            return False, f"{len(missing_labels)} MSA images have no label; predict or edit them first", None
        pairs = [(image.stem, image, label_dir / f"{image.stem}.txt") for image in all_images]
    if not pairs:
        return False, "No paired images and labels in project MSA folder", None

    result_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f") + "_" + uuid.uuid4().hex[:6]
    result_dir = msa_root / "results" / result_id
    result_dir.mkdir(parents=True, exist_ok=False)
    try:
        if progress_cb:
            progress_cb(5, "Tải model", 0, len(pairs))
        module = load_train_module()
        model = module.YOLO(str(weights_path))
        model_names = getattr(model, "names", None)
        if isinstance(model_names, dict):
            class_names = {int(k): str(v) for k, v in model_names.items() if str(k).isdigit()}
        elif isinstance(model_names, (list, tuple)):
            class_names = {i: str(v) for i, v in enumerate(model_names)}
        else:
            classes, _ = load_project_classes(msa_root.parent)
            class_names = {i: name for i, name in enumerate(classes)}

        args_info = _read_run_args(run_dir)
        predictions = model.predict(
            source=str(image_dir), imgsz=int(args_info.get("imgsz", 640) or 640),
            device=args_info.get("device", 0), conf=0.25, iou=0.45,
            save=True, save_txt=True, save_conf=True, project=str(result_dir),
            name="prediction", exist_ok=True, stream=True,
        )
        for index, _ in enumerate(predictions, 1):
            if progress_cb:
                progress_cb(10 + round(70 * min(index, len(pairs)) / len(pairs)),
                            "Dự đoán ảnh MSA", min(index, len(pairs)), len(pairs))
        prediction_dir = result_dir / "prediction"
        gt_vis_dir = result_dir / "gt"
        gt_vis_dir.mkdir()
        sample_items = []
        class_errors = {}
        wrong_images = 0
        for index, (stem, image_path, label_path) in enumerate(pairs, 1):
            gt_rows = _read_yolo_label_file(label_path)
            pred_rows = _read_yolo_label_file(prediction_dir / "labels" / f"{stem}.txt")
            issues = compare_msa_boxes(gt_rows, pred_rows, class_names)
            for issue in issues:
                class_id = issue["class_id"]
                stat = class_errors.setdefault(str(class_id), {
                    "class_name": issue["class_name"], "wrong_images": 0,
                    "missed": 0, "extra": 0,
                })
                stat["wrong_images"] += 1
                stat["missed"] += issue["missed"]
                stat["extra"] += issue["extra"]
            if issues:
                wrong_images += 1
            gt_file = gt_vis_dir / image_path.name
            _draw_label_overlay(image_path, gt_rows, class_names, gt_file, getattr(module, "cv2", None))
            pred_file = prediction_dir / image_path.name
            sample_items.append({
                "image_name": image_path.name, "status": "wrong" if issues else "correct",
                "issues": issues, "gt_image": f"results/{result_id}/gt/{image_path.name}",
                "pred_image": f"results/{result_id}/prediction/{image_path.name}" if pred_file.is_file() else f"images/{image_path.name}",
                "gt_class_ids": sorted({int(row["cls"]) for row in gt_rows}),
                "pred_class_ids": sorted({int(row["cls"]) for row in pred_rows}),
            })
            if progress_cb:
                progress_cb(80 + round(19 * index / len(pairs)), "Đối chiếu label", index, len(pairs))
        total = len(pairs)
        for stat in class_errors.values():
            stat["percentage"] = round(stat["wrong_images"] * 100 / total, 2)
        results = {
            "id": result_id, "run_folder": run_folder, "weight": weight_name,
            "created_at": now_str(), "total_images": total, "wrong_images": wrong_images,
            "error_rate": round(wrong_images * 100 / total, 2),
            "class_errors": class_errors, "sample_items": sample_items,
            "class_names": class_names,
        }
        (result_dir / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        if progress_cb:
            progress_cb(100, "Hoàn tất", len(pairs), len(pairs))
        return True, "MSA test completed", results
    except Exception as exc:
        return False, f"MSA test failed: {exc}", None


def predict_missing_msa_labels(project_name: str, run_folder: str, weight_name: str, progress_cb=None):
    msa_root = msa_root_for_project(project_name)
    run_dir, err = resolve_run_dir(project_name, run_folder)
    if msa_root is None or err or not run_dir or weight_name not in {"best.pt", "last.pt"}:
        return False, "Project or model not found", None
    weights_path = run_dir / "weights" / weight_name
    if not weights_path.is_file():
        return False, "Model weight not found", None
    image_dir = msa_root / "images"
    label_dir = msa_root / "labels"
    label_dir.mkdir(parents=True, exist_ok=True)
    images = sorted((image for image in image_dir.iterdir()
                     if image.is_file() and image.suffix.lower() in IMAGE_EXTS
                     and not (label_dir / f"{image.stem}.txt").exists()), key=lambda image: image.name.lower()) if image_dir.is_dir() else []
    if not images:
        return False, "All MSA images already have labels", None
    if progress_cb:
        progress_cb(5, "Tải model", 0, len(images))
    try:
        module = load_train_module()
        model = module.YOLO(str(weights_path))
        args_info = _read_run_args(run_dir)
        generated = 0
        processed = set()
        with tempfile.TemporaryDirectory(prefix="msa_label_predict_", dir=str(msa_root)) as tmp_name:
            source_dir = Path(tmp_name) / "images"
            source_dir.mkdir()
            for image in images:
                staged_image = source_dir / image.name
                try:
                    os.link(image, staged_image)
                except OSError:
                    shutil.copy2(image, staged_image)
            predictions = model.predict(
                source=str(source_dir),
                imgsz=int(args_info.get("imgsz", 640) or 640),
                device=args_info.get("device", 0), conf=0.25, iou=0.45,
                save=False, stream=True, verbose=False,
            )
            expected = {image.name for image in images}
            for index, result in enumerate(predictions, 1):
                image_name = Path(str(result.path)).name
                if image_name not in expected or image_name in processed:
                    return False, f"Unexpected prediction image: {image_name}", None
                processed.add(image_name)
                label_path = label_dir / f"{Path(image_name).stem}.txt"
                temp_label = Path(tmp_name) / f"{Path(image_name).stem}.txt"
                result.save_txt(str(temp_label), save_conf=False)
                text = temp_label.read_text(encoding="utf-8") if temp_label.is_file() else ""
                try:
                    with label_path.open("x", encoding="utf-8") as dest:
                        dest.write(text)
                    generated += 1
                except FileExistsError:
                    pass
                if progress_cb:
                    progress_cb(10 + round(90 * min(index, len(images)) / len(images)),
                                "Predict label gốc", min(index, len(images)), len(images))
        if len(processed) != len(images):
            return False, f"Model returned {len(processed)}/{len(images)} images", None
        return True, f"Created {generated} MSA labels; kept {len(images) - generated} existing labels", {
            "generated": generated, "total": len(images),
        }
    except Exception as exc:
        return False, f"MSA label prediction failed: {exc}", None


MSA_TASK = {"status": "idle", "project": "", "message": "", "result_id": "", "progress": 0, "processed": 0, "total": 0}
MSA_UPLOAD_TASK = {"status": "idle", "project": "", "message": "", "progress": 0, "processed": 0, "total": 0}
MSA_LABEL_TASK = {"status": "idle", "project": "", "message": "", "progress": 0, "processed": 0, "total": 0}
msa_task_lock = threading.Lock()


def msa_audit_actor():
    return {"user": current_auth_user(), "session_id": current_session_id(),
            "ip": str(request.headers.get("X-Forwarded-For") or request.remote_addr or "").strip()}


def write_msa_task_audit(action: str, status: str, project: str, target: str, details: str, actor: dict):
    try:
        append_audit_log({"time": now_str(), **actor, "action": action, "status": status,
                          "project": project, "target": target, "details": details})
    except Exception:
        pass


def run_project_msa_test_task(project_name: str, run_folder: str, weight_name: str, actor: dict):
    def update_progress(percent, message, processed, total):
        with msa_task_lock:
            MSA_TASK.update(progress=percent, message=message, processed=processed, total=total)

    try:
        ok, message, results = run_project_msa_test(project_name, run_folder, weight_name, progress_cb=update_progress)
    except Exception as exc:
        ok, message, results = False, f"MSA test failed: {exc}", None
    write_msa_task_audit("project_model_testing_msa", "success" if ok else "failed",
                         project_name, run_folder, message, actor)
    with msa_task_lock:
        MSA_TASK.update(status="success" if ok else "failed", message=message,
                        progress=100 if ok else MSA_TASK["progress"],
                        result_id=results["id"] if results else "")


def import_msa_zip_task(project_name: str, zip_path: Path, temp_dir, actor: dict):
    def update_progress(percent, message, processed, total):
        with msa_task_lock:
            MSA_UPLOAD_TASK.update(progress=percent, message=message, processed=processed, total=total)

    try:
        ok, message, payload = import_msa_zip(project_name, zip_path, progress_cb=update_progress)
    except Exception as exc:
        ok, message, payload = False, f"MSA import failed: {exc}", None
    finally:
        temp_dir.cleanup()
    write_msa_task_audit("project_msa_import", "success" if ok else "failed",
                         project_name, zip_path.name, message, actor)
    with msa_task_lock:
        MSA_UPLOAD_TASK.update(status="success" if ok else "failed", message=message,
                               progress=100 if ok else MSA_UPLOAD_TASK["progress"],
                               count=(payload or {}).get("count", 0))


def predict_missing_msa_labels_task(project_name: str, run_folder: str, weight_name: str, actor: dict):
    def update_progress(percent, message, processed, total):
        with msa_task_lock:
            MSA_LABEL_TASK.update(progress=percent, message=message, processed=processed, total=total)

    try:
        ok, message, payload = predict_missing_msa_labels(
            project_name, run_folder, weight_name, progress_cb=update_progress
        )
    except Exception as exc:
        ok, message, payload = False, f"MSA label prediction failed: {exc}", None
    write_msa_task_audit("project_msa_predict_labels", "success" if ok else "failed",
                         project_name, f"{run_folder}/{weight_name}", message, actor)
    with msa_task_lock:
        MSA_LABEL_TASK.update(status="success" if ok else "failed", message=message,
                              progress=100 if ok else MSA_LABEL_TASK["progress"],
                              generated=(payload or {}).get("generated", 0))


def build_msa_testing_summary(project_path: Path, run_dir: Path):
    msa_dir = (run_dir / "ModelTesting_MSA").resolve()
    if not msa_dir.exists() or not msa_dir.is_dir() or not is_path_inside(msa_dir, run_dir):
        return {"exists": False}

    results_file = msa_dir / "results.json"
    if not results_file.exists() or not results_file.is_file():
        return {"exists": False}

    try:
        results = json.loads(results_file.read_text(encoding="utf-8")) or {}
    except Exception:
        return {"exists": False}

    output_dir = find_output_dir(project_path)
    export_files = []
    if output_dir:
        for name in ["results.json"]:
            file_path = msa_dir / name
            if file_path.exists() and file_path.is_file():
                export_files.append({
                    "name": name,
                    "relative_path": file_path.relative_to(output_dir).as_posix(),
                })

    return {
        "exists": True,
        "results": results,
        "export_files": export_files,
    }


def run_msa_model_testing(project_name: str, run_folder: str, upload_file):
    run_dir, err = resolve_run_dir(project_name, run_folder)
    if err or not run_dir:
        return False, "Run folder not found", None

    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Project not found", None

    best_path = run_dir / "weights" / "best.pt"
    last_path = run_dir / "weights" / "last.pt"
    weights_path = best_path if best_path.exists() else last_path
    if not weights_path.exists():
        return False, "Missing best.pt/last.pt in run folder", None

    if not upload_file:
        return False, "Missing sample ZIP file", None

    with tempfile.TemporaryDirectory(prefix="msa_upload_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        zip_path = tmp_dir / "upload.zip"
        try:
            upload_file.save(str(zip_path))
        except Exception:
            return False, "Unable to save upload file", None

        extract_root = tmp_dir / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for info in zf.infolist():
                    name = str(info.filename or "").replace("\\", "/")
                    parts = Path(name).parts
                    if name.startswith("/") or ".." in parts:
                        return False, "Invalid zip path", None
                zf.extractall(path=str(extract_root))
        except zipfile.BadZipFile:
            return False, "Bad zip file", None
        except Exception as e:
            return False, f"Unable to extract ZIP: {e}", None

        pairs = _collect_msa_sample_pairs(extract_root)
        if not pairs:
            return False, "No matching image/label pairs found in ZIP", None

        msa_dir = (run_dir / "ModelTesting_MSA").resolve()
        if not is_path_inside(msa_dir, run_dir):
            return False, "Invalid MSA output directory", None
        try:
            if msa_dir.exists() and msa_dir.is_dir():
                shutil.rmtree(msa_dir)
            msa_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            return False, f"Cannot prepare MSA results dir: {e}", None

        gt_vis_dir = msa_dir / "msa_vis" / "gt"
        pred_vis_dir = msa_dir / "msa_vis" / "pred"
        gt_vis_dir.mkdir(parents=True, exist_ok=True)
        pred_vis_dir.mkdir(parents=True, exist_ok=True)

        args_info = _read_run_args(run_dir)
        module = load_train_module()
        model = module.YOLO(str(weights_path))
        model_names = getattr(model, "names", None)
        if isinstance(model_names, dict):
            class_names = {int(k): str(v) for k, v in model_names.items() if str(k).isdigit()}
        elif isinstance(model_names, (list, tuple)):
            class_names = {i: str(v) for i, v in enumerate(model_names)}
        else:
            class_names = {}
        if not class_names:
            project_classes, _ = load_project_classes(project_path)
            class_names = {i: name for i, name in enumerate(project_classes)}

        image_source_dir = tmp_dir / "msa_images"
        image_source_dir.mkdir(parents=True, exist_ok=True)
        for stem, img_path, label_path in pairs:
            dest = image_source_dir / img_path.name
            shutil.copy2(img_path, dest)

        try:
            output_predict = tmp_dir / "msa_predict"
            output_predict.mkdir(parents=True, exist_ok=True)
            model.predict(
                source=str(image_source_dir),
                imgsz=int(args_info.get("imgsz", 640) or 640),
                device=args_info.get("device", 0),
                conf=0.25,
                iou=0.45,
                save=True,
                save_txt=True,
                project=str(tmp_dir),
                name="msa_predict",
                exist_ok=True,
            )
        except Exception as e:
            return False, f"MSA prediction failed: {e}", None

        # determine predicted labels and images
        predicted_labels_dir = output_predict / "labels"
        predicted_images_dir = output_predict
        sample_items = []
        total_images = 0
        wrong_images = 0
        for stem, img_path, label_path in pairs:
            total_images += 1
            gt_rows = _read_yolo_label_file(label_path)
            gt_ids = sorted({int(row["cls"]) for row in gt_rows})
            gt_vis_path = gt_vis_dir / f"{stem}.jpg"
            _draw_label_overlay(img_path, gt_rows, class_names, gt_vis_path, getattr(module, "cv2", None))

            pred_label_file = predicted_labels_dir / f"{stem}.txt"
            pred_rows = _read_yolo_label_file(pred_label_file)
            pred_ids = sorted({int(row["cls"]) for row in pred_rows})

            predicted_image_path = predicted_images_dir / img_path.name
            pred_vis_path = pred_vis_dir / f"{stem}.jpg"
            try:
                if predicted_image_path.exists() and predicted_image_path.is_file():
                    shutil.copy2(predicted_image_path, pred_vis_path)
                else:
                    shutil.copy2(img_path, pred_vis_path)
            except Exception:
                try:
                    pred_vis_path.write_bytes(img_path.read_bytes())
                except Exception:
                    pass

            status = "correct" if gt_ids == pred_ids else "wrong"
            if status == "wrong":
                wrong_images += 1

            sample_items.append({
                "image_name": img_path.name,
                "gt_class_ids": gt_ids,
                "pred_class_ids": pred_ids,
                "status": status,
                "gt_vis_relative_path": f"{run_dir.name}/ModelTesting_MSA/msa_vis/gt/{gt_vis_path.name}",
                "pred_vis_relative_path": f"{run_dir.name}/ModelTesting_MSA/msa_vis/pred/{pred_vis_path.name}",
            })

        results = {
            "total_images": total_images,
            "wrong_images": wrong_images,
            "error_rate": round(((wrong_images / max(1, total_images)) * 100.0), 2),
            "sample_items": sample_items,
        }

        results_file = msa_dir / "results.json"
        try:
            results_file.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            return False, f"Unable to save MSA results: {e}", None

        return True, f"MSA model testing completed for {run_folder}", {
            "run_folder": run_folder,
            "run_dir": str(run_dir),
            "results": results,
        }


def _run_model_test_task(task_id: str, project_name: str, run_folder: str):
    with model_test_task_lock:
        _set_model_test_task_locked(
            progress=8.0,
            message="Preparing model testing",
            detail=f"{project_name} | {run_folder}",
        )

    ok, message, payload = run_model_testing(project_name, run_folder)

    with model_test_task_lock:
        if MODEL_TEST_TASK.get("id") != task_id:
            return
        _set_model_test_task_locked(
            status="success" if ok else "failed",
            progress=100.0 if ok else max(1.0, float(MODEL_TEST_TASK.get("progress") or 0.0)),
            message=message,
            detail=f"{project_name} | {run_folder}",
            ended_at=time.time(),
            result=payload if ok else None,
        )


def start_model_test_task(project_name: str, run_folder: str):
    with model_test_task_lock:
        if MODEL_TEST_TASK.get("status") == "running":
            same_task = (
                str(MODEL_TEST_TASK.get("project") or "") == str(project_name or "")
                and str(MODEL_TEST_TASK.get("run_folder") or "") == str(run_folder or "")
            )
            if same_task:
                return True, "Model testing is already running", {
                    "task_id": MODEL_TEST_TASK.get("id"),
                    "status": MODEL_TEST_TASK.get("status"),
                }
            return False, "Another model testing task is already running", None

        task_id = f"modeltest-{int(time.time() * 1000)}"
        _set_model_test_task_locked(
            id=task_id,
            project=project_name,
            run_folder=run_folder,
            status="running",
            progress=3.0,
            message="Starting model testing",
            detail=f"{project_name} | {run_folder}",
            started_at=time.time(),
            ended_at=0.0,
            result=None,
        )

    thread = threading.Thread(
        target=_run_model_test_task,
        args=(task_id, project_name, run_folder),
        daemon=True,
    )
    thread.start()
    return True, "Model testing started", {
        "task_id": task_id,
        "status": "running",
    }


def _parse_predict_options(raw: dict):
    raw = raw or {}

    def as_float(name: str, default: float, min_value: float, max_value: float):
        try:
            value = float(raw.get(name, default))
        except Exception:
            value = default
        return max(min_value, min(max_value, value))

    def as_int(name: str, default: int, min_value: int, max_value: int):
        try:
            value = int(float(raw.get(name, default)))
        except Exception:
            value = default
        return max(min_value, min(max_value, value))

    device = str(raw.get("device", "") if raw.get("device", "") is not None else "").strip()
    return {
        "conf": as_float("conf", 0.5, 0.0, 1.0),
        "iou": as_float("iou", 0.65, 0.0, 1.0),
        "imgsz": as_int("imgsz", 640, 32, 4096),
        "max_det": as_int("max_det", 300, 1, 10000),
        "device": device,
        "save_conf": bool(raw.get("save_conf", False)),
        "include_empty_labels": bool(raw.get("include_empty_labels", False)),
        "save_empty_labels": bool(raw.get("save_empty_labels", True)),
    }


def find_unlabeled_project_images(project_name: str, include_empty_labels: bool = False):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return None, [], "project_not_found"

    image_dir, all_images = list_project_images(project_path, limit=0)
    if not image_dir:
        return None, [], "image_dir_not_found"

    rows = []
    for rel in all_images:
        label_file, err = resolve_label_file_for_image(project_name, rel, create_missing=False)
        if err:
            continue
        has_label = bool(label_file and label_file.exists() and label_file.is_file())
        if has_label and include_empty_labels:
            try:
                has_label = bool(label_file.read_text(encoding="utf-8").strip())
            except Exception:
                has_label = True
        if has_label:
            continue
        img_file = (image_dir / rel).resolve()
        if is_path_inside(img_file, image_dir) and img_file.exists() and img_file.is_file():
            rows.append({
                "rel": rel,
                "image_file": img_file,
                "label_file": (image_dir / Path(rel).with_suffix(".txt")).resolve(),
            })

    return image_dir, rows, None


def run_predict_unlabeled(project_name: str, weights_rel: str, options: dict):
    weights_path, err = resolve_output_weight_file(project_name, weights_rel)
    if err or not weights_path:
        return False, f"Model weight not found: {err or 'file_not_found'}", None

    opts = _parse_predict_options(options)
    image_dir, rows, err = find_unlabeled_project_images(
        project_name,
        include_empty_labels=bool(opts.get("include_empty_labels")),
    )
    if err or not image_dir:
        return False, f"Cannot find project images: {err or 'image_dir_not_found'}", None
    if not rows:
        return True, "No images need prediction", {
            "project": project_name,
            "weights_rel": weights_rel,
            "total": 0,
            "written": 0,
            "empty_written": 0,
            "skipped": 0,
        }

    try:
        from ultralytics import YOLO
    except Exception as e:
        return False, f"Cannot import ultralytics YOLO: {e}", None

    with predict_task_lock:
        if PREDICT_TASK.get("status") == "running":
            _set_predict_task_locked(
                progress=8.0,
                message="Loading model",
                detail=f"{project_name} | {Path(weights_rel).name}",
            )

    append_log(project_name, f"[{now_str()}] PREDICT START: {weights_rel} ({len(rows)} image(s))")
    try:
        model = YOLO(str(weights_path))
    except Exception as e:
        append_log(project_name, f"[{now_str()}] PREDICT FAILED: cannot load model: {e}")
        return False, f"Cannot load model: {e}", None

    written = 0
    empty_written = 0
    skipped = 0
    errors = []
    total = len(rows)
    device = opts.get("device")
    predict_kwargs = {
        "conf": float(opts.get("conf", 0.5)),
        "iou": float(opts.get("iou", 0.65)),
        "imgsz": int(opts.get("imgsz", 640)),
        "max_det": int(opts.get("max_det", 300)),
        "verbose": False,
    }
    if device:
        predict_kwargs["device"] = device

    for idx, row in enumerate(rows, start=1):
        rel = str(row.get("rel") or "")
        img_file = row.get("image_file")
        label_file = row.get("label_file")
        pct = 10.0 + ((idx - 1) / max(1, total)) * 84.0
        with predict_task_lock:
            if PREDICT_TASK.get("status") == "running":
                _set_predict_task_locked(
                    progress=pct,
                    message=f"Predicting {idx}/{total}",
                    detail=rel,
                )
        try:
            if not label_file or not is_path_inside(label_file, image_dir):
                skipped += 1
                continue
            if label_file.exists() and label_file.is_file() and not opts.get("include_empty_labels"):
                skipped += 1
                continue
            results = model.predict(source=str(img_file), **predict_kwargs)
            result = results[0] if results else None
            lines = []
            boxes = getattr(result, "boxes", None) if result is not None else None
            if boxes is not None and len(boxes) > 0:
                cls_values = boxes.cls.detach().cpu().numpy().tolist()
                xywhn_values = boxes.xywhn.detach().cpu().numpy().tolist()
                conf_values = boxes.conf.detach().cpu().numpy().tolist()
                for cls_id, xywhn, conf_value in zip(cls_values, xywhn_values, conf_values):
                    values = [int(cls_id), *[float(x) for x in xywhn]]
                    if opts.get("save_conf"):
                        values.append(float(conf_value))
                    lines.append(" ".join([str(values[0])] + [f"{v:.6f}" for v in values[1:]]))
            if lines or opts.get("save_empty_labels"):
                label_file.parent.mkdir(parents=True, exist_ok=True)
                label_file.write_text(("\n".join(lines) + ("\n" if lines else "")), encoding="utf-8")
                written += 1
                if not lines:
                    empty_written += 1
            else:
                skipped += 1
        except Exception as e:
            skipped += 1
            if len(errors) < 10:
                errors.append(f"{rel}: {e}")

    append_log(project_name, f"[{now_str()}] PREDICT DONE: written={written}, empty={empty_written}, skipped={skipped}")
    return True, f"Predict completed: {written}/{total} label file(s) written", {
        "project": project_name,
        "weights_rel": weights_rel,
        "total": total,
        "written": written,
        "empty_written": empty_written,
        "skipped": skipped,
        "errors": errors,
        "options": opts,
    }


def _run_predict_task(task_id: str, project_name: str, weights_rel: str, options: dict):
    with predict_task_lock:
        _set_predict_task_locked(
            progress=5.0,
            message="Preparing prediction",
            detail=f"{project_name} | {Path(weights_rel).name}",
        )

    ok, message, payload = run_predict_unlabeled(project_name, weights_rel, options)

    with predict_task_lock:
        if PREDICT_TASK.get("id") != task_id:
            return
        _set_predict_task_locked(
            status="success" if ok else "failed",
            progress=100.0 if ok else max(1.0, float(PREDICT_TASK.get("progress") or 0.0)),
            message=message,
            detail=f"{project_name} | {Path(weights_rel).name}",
            ended_at=time.time(),
            result=payload if ok else None,
        )


def start_predict_task(project_name: str, weights_rel: str, options: dict):
    with predict_task_lock:
        if PREDICT_TASK.get("status") == "running":
            same_task = (
                str(PREDICT_TASK.get("project") or "") == str(project_name or "")
                and str(PREDICT_TASK.get("weights_rel") or "") == str(weights_rel or "")
            )
            if same_task:
                return True, "Prediction is already running", {
                    "task_id": PREDICT_TASK.get("id"),
                    "status": PREDICT_TASK.get("status"),
                }
            return False, "Another prediction task is already running", None

        task_id = f"predict-{int(time.time() * 1000)}"
        _set_predict_task_locked(
            id=task_id,
            project=project_name,
            weights_rel=weights_rel,
            status="running",
            progress=2.0,
            message="Starting prediction",
            detail=f"{project_name} | {Path(weights_rel).name}",
            started_at=time.time(),
            ended_at=0.0,
            result=None,
        )

    thread = threading.Thread(
        target=_run_predict_task,
        args=(task_id, project_name, weights_rel, options or {}),
        daemon=True,
    )
    thread.start()
    return True, "Prediction started", {
        "task_id": task_id,
        "status": "running",
    }


def _project_task_progress_cb(progress: float, message: str, detail: str = ""):
    with project_fs_task_lock:
        if PROJECT_FS_TASK.get("status") == "running":
            _set_project_fs_task_locked(
                progress=max(0.0, min(99.0, float(progress or 0.0))),
                message=str(message or ""),
                detail=str(detail or ""),
            )


def _ensure_project_task_allowed(project_name: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Không tìm thấy project", None
    if not is_path_inside(project_path, ROOT_DIR):
        return False, "Đường dẫn project không hợp lệ", None
    with state_lock:
        info = STATE["projects"].get(project_name)
        if info and (info.get("status") == "running" or project_name in STATE["queue"] or STATE.get("current") == project_name):
            return False, "Project đang chạy hoặc đang trong queue", None
    return True, "", project_path


def _collect_tree_paths(root_path: Path):
    files = []
    dirs = [root_path]
    for current_root, dir_names, file_names in os.walk(root_path):
        current = Path(current_root)
        for dir_name in dir_names:
            dirs.append(current / dir_name)
        for file_name in file_names:
            files.append(current / file_name)
    return files, dirs


def duplicate_project_with_progress(project_name: str, new_name: str):
    ok, err, project_path = _ensure_project_task_allowed(project_name)
    if not ok:
        return False, err, None

    if not new_name:
        new_name = get_available_duplicate_name(project_name)
    if not is_valid_project_name(new_name):
        return False, "Tên project không hợp lệ", None

    target_path = ROOT_DIR / new_name
    if target_path.exists():
        return False, "Project đích đã tồn tại", None

    files, dirs = _collect_tree_paths(project_path)
    total_steps = max(1, len(files) + len(dirs))

    _project_task_progress_cb(6.0, "Preparing duplicate", project_name)
    target_path.mkdir(parents=True, exist_ok=False)
    completed = 1
    _project_task_progress_cb(10.0, "Created target folder", str(target_path))

    for dir_path in sorted(dirs[1:], key=lambda p: len(p.parts)):
        rel = dir_path.relative_to(project_path)
        (target_path / rel).mkdir(parents=True, exist_ok=True)
        completed += 1
        pct = 10.0 + (completed / total_steps) * 82.0
        _project_task_progress_cb(pct, f"Creating folders: {completed}/{total_steps}", rel.as_posix())

    for file_path in files:
        rel = file_path.relative_to(project_path)
        dst = target_path / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, dst)
        completed += 1
        pct = 10.0 + (completed / total_steps) * 82.0
        _project_task_progress_cb(pct, f"Copying files: {completed}/{total_steps}", rel.as_posix())

    _project_task_progress_cb(96.0, "Refreshing project list", new_name)
    scan_projects()
    return True, f"Đã nhân bản project thành {new_name}", {"project": new_name}


def rename_project_with_progress(project_name: str, new_name: str):
    ok, err, project_path = _ensure_project_task_allowed(project_name)
    if not ok:
        return False, err, None
    if not new_name:
        return False, "Thiếu tên project", None
    if not is_valid_project_name(new_name):
        return False, "Tên project không hợp lệ", None
    if project_name == new_name:
        return True, "Tên project không thay đổi", {"project": new_name}

    target_path = ROOT_DIR / new_name
    if target_path.exists():
        return False, "Project đích đã tồn tại", None

    _project_task_progress_cb(18.0, "Preparing rename", project_name)
    shutil.move(str(project_path), str(target_path))
    _project_task_progress_cb(88.0, "Refreshing project list", new_name)
    scan_projects()
    return True, f"Đã đổi tên project thành {new_name}", {"project": new_name}


def delete_project_with_progress(project_name: str):
    ok, err, project_path = _ensure_project_task_allowed(project_name)
    if not ok:
        return False, err, None

    files, dirs = _collect_tree_paths(project_path)
    total_steps = max(1, len(files) + len(dirs))
    completed = 0
    _project_task_progress_cb(8.0, "Preparing delete", project_name)

    for file_path in files:
        rel = file_path.relative_to(project_path)
        file_path.unlink(missing_ok=True)
        completed += 1
        pct = 8.0 + (completed / total_steps) * 84.0
        _project_task_progress_cb(pct, f"Deleting files: {completed}/{total_steps}", rel.as_posix())

    for dir_path in sorted(dirs, key=lambda p: len(p.parts), reverse=True):
        if dir_path.exists():
            dir_path.rmdir()
        completed += 1
        rel = "." if dir_path == project_path else dir_path.relative_to(project_path).as_posix()
        pct = 8.0 + (completed / total_steps) * 84.0
        _project_task_progress_cb(pct, f"Removing folders: {completed}/{total_steps}", rel)

    with state_lock:
        STATE["projects"].pop(project_name, None)
        bump_state_version_locked()

    _project_task_progress_cb(96.0, "Refreshing project list", project_name)
    scan_projects()
    return True, f"Đã xóa project {project_name}", {"project": project_name}


def run_project_fs_task(project_name: str, operation: str, payload: dict | None = None):
    payload = dict(payload or {})
    op = str(operation or "").strip().lower()
    if op == "duplicate":
        return duplicate_project_with_progress(project_name, str(payload.get("new_name", "") or "").strip())
    if op == "rename":
        return rename_project_with_progress(project_name, str(payload.get("new_name", "") or "").strip())
    if op == "delete":
        return delete_project_with_progress(project_name)
    return False, f"Unsupported project task: {operation}", None


def _run_project_fs_task(task_id: str, project_name: str, operation: str, payload: dict | None = None):
    with project_fs_task_lock:
        _set_project_fs_task_locked(
            progress=4.0,
            message="Preparing project task",
            detail=f"{operation} | {project_name}",
        )

    ok, message, result = run_project_fs_task(project_name, operation, payload)

    with project_fs_task_lock:
        if PROJECT_FS_TASK.get("id") != task_id:
            return
        _set_project_fs_task_locked(
            status="success" if ok else "failed",
            progress=100.0 if ok else max(1.0, float(PROJECT_FS_TASK.get("progress") or 0.0)),
            message=message,
            detail=str((result or {}).get("project") or f"{operation} | {project_name}"),
            ended_at=time.time(),
            result=result if ok else None,
        )


def start_project_fs_task(project_name: str, operation: str, payload: dict | None = None):
    op = str(operation or "").strip().lower()
    with project_fs_task_lock:
        if PROJECT_FS_TASK.get("status") == "running":
            same_task = (
                str(PROJECT_FS_TASK.get("project") or "") == str(project_name or "")
                and str(PROJECT_FS_TASK.get("operation") or "") == op
            )
            if same_task:
                return True, "Project task is already running", {
                    "task_id": PROJECT_FS_TASK.get("id"),
                    "status": PROJECT_FS_TASK.get("status"),
                }
            return False, "Another project task is already running", None

        task_id = f"projectfs-{int(time.time() * 1000)}"
        _set_project_fs_task_locked(
            id=task_id,
            project=project_name,
            operation=op,
            target=str((payload or {}).get("new_name") or ""),
            status="running",
            progress=2.0,
            message="Starting project task",
            detail=f"{op} | {project_name}",
            started_at=time.time(),
            ended_at=0.0,
            result=None,
        )

    thread = threading.Thread(
        target=_run_project_fs_task,
        args=(task_id, project_name, op, dict(payload or {})),
        daemon=True,
    )
    thread.start()
    return True, "Project task started", {
        "task_id": task_id,
        "status": "running",
    }


def clear_dataset_for_project(project_name: str, progress_cb=None):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Không tìm thấy project", None
    if not is_path_inside(project_path, ROOT_DIR):
        return False, "Đường dẫn project không hợp lệ", None

    with state_lock:
        info = STATE["projects"].get(project_name)
        if info and (info.get("status") == "running" or project_name in STATE["queue"] or STATE.get("current") == project_name):
            return False, "Project đang chạy hoặc đang trong queue", None

    if callable(progress_cb):
        try:
            progress_cb(8.0, "Preparing dataset cleanup", project_name)
        except Exception:
            pass

    def _clear_progress(done, total, folder_name, stage):
        if not callable(progress_cb):
            return
        pct = 18.0 + ((float(done or 0) / max(1.0, float(total or 1))) * 72.0)
        label = "Removing" if stage != "done" else "Removed"
        progress_cb(pct, f"{label} dataset folder: {folder_name}", project_name)

    try:
        removed = clear_project_dataset_dirs(project_path, progress_cb=_clear_progress)
    except Exception as e:
        return False, f"Không clear dataset được: {e}", None

    if not removed:
        return True, "Không có thư mục dataset nào để xóa", {"removed": []}

    return True, f"Đã xóa: {', '.join(removed)}", {"removed": removed}


def clear_split_dataset_for_project(project_name: str):
    project_path = resolve_project_path(project_name)
    if not project_path or not project_path.exists():
        return False, "Không tìm thấy project", {"removed": []}
    if not is_path_inside(project_path, ROOT_DIR):
        return False, "Đường dẫn project không hợp lệ", {"removed": []}

    with state_lock:
        info = STATE["projects"].get(project_name)
        if info and (info.get("status") == "running" or project_name in STATE["queue"] or STATE.get("current") == project_name):
            return False, "Project đang chạy hoặc đang trong queue", {"removed": []}

    removed = []
    pending = []
    errors = []
    for folder_name in ("train", "test", "valid"):
        target = (project_path / folder_name).resolve()
        if not is_path_inside(target, project_path):
            continue
        if target.exists() and target.is_dir():
            ok, pending_path, err = remove_tree_best_effort(target, project_path)
            if ok:
                removed.append(folder_name)
                if pending_path:
                    pending.append(f"{folder_name} -> {pending_path}")
            elif err:
                errors.append(f"{folder_name}: {err}")

    for file_name in ("data.yaml", "data.yml"):
        target = (project_path / file_name).resolve()
        if not is_path_inside(target, project_path):
            continue
        if target.exists() and target.is_file():
            try:
                try:
                    os.chmod(target, stat.S_IWRITE | stat.S_IREAD)
                except Exception:
                    pass
                target.unlink()
                removed.append(file_name)
            except Exception as e:
                errors.append(f"{file_name}: {e}")

    if errors:
        return False, "Không xóa được dataset: " + "; ".join(errors), {
            "removed": removed,
            "pending_cleanup": pending,
        }

    if not removed:
        return True, "Không có dataset split để xóa", {"removed": []}
    message = f"Đã xóa: {', '.join(removed)}"
    if pending:
        message += f" (còn pending cleanup: {len(pending)})"
    return True, message, {"removed": removed, "pending_cleanup": pending}


def clear_split_dataset_for_projects(project_names: list[str]):
    cleaned = []
    seen = set()
    for item in project_names or []:
        name = str(item or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        cleaned.append(name)

    results = []
    ok_count = 0
    failed_count = 0
    for name in cleaned:
        try:
            ok, message, payload = clear_split_dataset_for_project(name)
        except Exception as e:
            ok, message, payload = False, f"Không xóa được dataset: {e}", {"removed": []}
        if ok:
            ok_count += 1
            append_log(name, f"[{now_str()}] CLEAR SELECTED DATASET: {message}")
        else:
            failed_count += 1
            append_log(name, f"[{now_str()}] CLEAR SELECTED DATASET FAILED: {message}")
        results.append({
            "project": name,
            "ok": ok,
            "message": message,
            **(payload or {}),
        })

    return ok_count, failed_count, results


def run_dataset_task(project_name: str, operation: str, payload: dict | None = None):
    payload = dict(payload or {})
    op = str(operation or "").strip().lower()

    def progress_cb(progress, message, detail=""):
        with dataset_task_lock:
            if DATASET_TASK.get("status") == "running":
                _set_dataset_task_locked(
                    progress=max(0.0, min(99.0, float(progress or 0.0))),
                    message=str(message or ""),
                    detail=str(detail or project_name),
                )

    if op == "create":
        split_mode = str(payload.get("split_mode", "count") or "count").strip().lower()
        return create_dataset_for_project(project_name, payload, split_mode=split_mode, progress_cb=progress_cb)
    if op == "merge":
        return merge_train_valid_to_train(project_name, progress_cb=progress_cb)
    if op == "clear":
        return clear_dataset_for_project(project_name, progress_cb=progress_cb)
    return False, f"Unsupported dataset operation: {operation}", None


def _run_dataset_task(task_id: str, project_name: str, operation: str, payload: dict | None = None):
    with dataset_task_lock:
        _set_dataset_task_locked(
            progress=6.0,
            message="Preparing dataset task",
            detail=f"{operation} | {project_name}",
        )

    ok, message, result = run_dataset_task(project_name, operation, payload)

    with dataset_task_lock:
        if DATASET_TASK.get("id") != task_id:
            return
        _set_dataset_task_locked(
            status="success" if ok else "failed",
            progress=100.0 if ok else max(1.0, float(DATASET_TASK.get("progress") or 0.0)),
            message=message,
            detail=f"{operation} | {project_name}",
            ended_at=time.time(),
            result=result if ok else None,
        )


def start_dataset_task(project_name: str, operation: str, payload: dict | None = None):
    op = str(operation or "").strip().lower()
    with dataset_task_lock:
        if DATASET_TASK.get("status") == "running":
            same_task = (
                str(DATASET_TASK.get("project") or "") == str(project_name or "")
                and str(DATASET_TASK.get("operation") or "") == op
            )
            if same_task:
                return True, "Dataset task is already running", {
                    "task_id": DATASET_TASK.get("id"),
                    "status": DATASET_TASK.get("status"),
                }
            return False, "Another dataset task is already running", None

        task_id = f"dataset-{int(time.time() * 1000)}"
        _set_dataset_task_locked(
            id=task_id,
            project=project_name,
            operation=op,
            status="running",
            progress=2.0,
            message="Starting dataset task",
            detail=f"{op} | {project_name}",
            started_at=time.time(),
            ended_at=0.0,
            result=None,
        )

    thread = threading.Thread(
        target=_run_dataset_task,
        args=(task_id, project_name, op, dict(payload or {})),
        daemon=True,
    )
    thread.start()
    return True, "Dataset task started", {
        "task_id": task_id,
        "status": "running",
    }


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


def infer_completed_project_status(project_path: Path) -> tuple[str, int]:
    output_dir = find_output_dir(project_path)
    if output_dir and output_dir.exists() and output_dir.is_dir():
        try:
            for run_dir in output_dir.iterdir():
                if not run_dir.is_dir() or not run_dir.name.lower().startswith("model_train"):
                    continue
                if (run_dir / "results.csv").exists():
                    return "success", 0
                weights_dir = run_dir / "weights"
                if weights_dir.exists() and any(
                    p.exists() for p in (weights_dir / "best.pt", weights_dir / "last.pt")
                ):
                    return "success", 0
        except Exception:
            pass
    return "stopped", -15


def finalize_stale_running_projects_locked(monitor_is_training: bool, matched_project_name: str | None):
    changed = False
    matched_project_name = str(matched_project_name or "").strip()
    active_project_name = str(CURRENT_TRAIN_CONTROL.get("project") or "").strip()

    if monitor_is_training:
        return changed

    for name, info in STATE["projects"].items():
        if str(info.get("status") or "").strip() != "running":
            continue
        # run_project or the recovered watcher owns the active project lifecycle.
        # Finalizing it here races with wait_train_completion_from_monitor and can
        # write success first, then have the worker overwrite it with a stale RC.
        if active_project_name and name == active_project_name:
            continue
        if matched_project_name and name == matched_project_name:
            continue

        project_path_raw = str(info.get("path") or "").strip()
        if not project_path_raw:
            continue

        try:
            project_path = Path(project_path_raw).resolve()
        except Exception:
            continue

        if find_project_train_pids(project_path, include_launcher=False):
            continue

        final_status, returncode = infer_completed_project_status(project_path)
        info["status"] = final_status
        info["progress"] = 100.0 if final_status == "success" else 0.0
        info["last_end"] = now_str()
        info["last_returncode"] = returncode
        info["pid"] = None

        if final_status == "success":
            info["last_log"].append(f"[{now_str()}] TRAIN RECOVERED AS SUCCESS AFTER APP RESTART")
        else:
            info["last_log"].append(f"[{now_str()}] TRAIN RECOVERED AS STOPPED AFTER APP RESTART")

        history = STATE.get("history") or []
        last_history = history[-1] if history else None
        if not (
            isinstance(last_history, dict)
            and str(last_history.get("project") or "") == name
            and str(last_history.get("status") or "") == final_status
        ):
            record_train_history_locked(name, final_status, returncode)

        if str(STATE.get("current") or "").strip() == name:
            STATE["current"] = None

        sync_queue_session_file_locked()
        changed = True

    return changed


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

    if not matched_project_name and monitor_is_training:
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

    stale_matched_name = matched_project_name if monitor_is_training else ""
    if finalize_stale_running_projects_locked(monitor_is_training, stale_matched_name):
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

            should_start_worker = bool(STATE.get("queue")) and not bool(STATE.get("current"))

        if should_start_worker:
            start_worker_if_needed()
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
    seen_training = bool(assume_started)
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
                if not seen_training:
                    # Ignore a failed message left behind by the previous run.
                    saw_failed_signal = False
                seen_training = True

            if seen_training and is_match and (monitor_msg.startswith("failed") or "error" in monitor_msg):
                saw_failed_signal = True

            if seen_training and is_match and not monitor_is_training and not find_project_train_pids(project_path, include_launcher=False):
                if stop_requested:
                    return -15
                if monitor_msg.startswith("completed") or monitor_msg in {"complete", "done", "finished"}:
                    return 0
                if saw_failed_signal:
                    return 1
                return 0

            if seen_training and not monitor_is_training and not find_project_train_pids(project_path, include_launcher=False):
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

                if port_closed and find_project_train_pids(project_path, include_launcher=False):
                    time.sleep(1.0)
                    continue
                if port_closed and stop_requested:
                    return -15
                if port_closed and (time.time() - start_ts) >= grace_seconds:
                    return 1

        if (time.time() - start_ts) >= grace_seconds and not seen_training:
            if not find_project_train_pids(project_path, include_launcher=False):
                return -15 if stop_requested else 1

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
        sync_queue_session_file_locked()
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
            sync_queue_session_file_locked()
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
                    sync_queue_session_file_locked()
                    bump_state_version_locked()
                    return
            continue

        should_wait = False
        with state_lock:
            p = ensure_project_state(project_name)

            if project_name not in STATE["queue"] and p.get("status") != "queued":
                should_wait = False
                skip_item = True
            else:
                skip_item = False
                if is_train_slot_busy_locked(exclude_project=project_name):
                    p["status"] = "queued"
                    p["progress"] = 0.0
                    if project_name not in STATE["queue"]:
                        STATE["queue"].append(project_name)
                        sync_queue_session_file_locked()
                        bump_state_version_locked()
                    should_wait = True
                else:
                    if project_name in STATE["queue"]:
                        STATE["queue"].remove(project_name)
                        sync_queue_session_file_locked()

        if skip_item:
            try:
                train_queue.task_done()
            except Exception:
                pass
            continue

        if should_wait:
            train_queue.put(project_name)
            try:
                train_queue.task_done()
            except Exception:
                pass
            time.sleep(1.0)
            continue

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
                    sync_queue_session_file_locked()
                    bump_state_version_locked()
                    return

        try:
            train_queue.task_done()
        except Exception:
            pass


def start_worker_if_needed():
    global worker_thread
    with state_lock:
        running = bool(STATE["worker_running"])
        if running and worker_thread is not None and not worker_thread.is_alive():
            STATE["worker_running"] = False
            running = False
            bump_state_version_locked()

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
        maybe_log_user_request(path)
        return None

    if path.startswith("/api/"):
        return jsonify({"ok": False, "message": "Unauthorized"}), 401

    return redirect(url_for("login", next=get_next_url(default=path)))


def maybe_log_user_request(path: str):
    try:
        method = str(request.method or "").upper()
        path = str(path or request.path or "").strip()
        if path.startswith("/UI/") or path in {"/healthz"}:
            return
        if method == "GET":
            if path not in {"/", "/project_editor"}:
                return
        elif method not in {"POST", "PUT", "PATCH", "DELETE"}:
            return
        if path in {"/api/train_monitor/status", "/api/state", "/api/log", "/api/activity_log"}:
            return
        if path.endswith("/status"):
            return
        project = ""
        target = path
        details = method
        if method == "GET":
            project = str(request.args.get("project", "") or "").strip()
        else:
            data = request.get_json(silent=True) if request.is_json else None
            if isinstance(data, dict):
                project = str(data.get("project", "") or data.get("source_project", "") or "").strip()
                target = str(data.get("rel", "") or data.get("target", "") or path).strip()
            else:
                project = str(request.form.get("project", "") or "").strip()
        write_audit_log("request", "seen", project=project, target=target, details=details)
        if method == "GET" and path == "/project_editor" and project:
            write_audit_log("label_editor_open", "success", project=project, target=path, details="opened")
    except Exception:
        return


register_basic_routes(
    app,
    get_next_url=get_next_url,
    is_authenticated=is_authenticated,
    load_user_credentials=load_user_credentials,
    authenticate_user=authenticate_user,
    verify_password=verify_password,
    mark_authenticated=mark_authenticated,
    clear_auth=clear_auth,
    state_lock=state_lock,
    state=STATE,
    monitor_cache=MONITOR_CACHE,
    now_str=now_str,
    root_dir=ROOT_DIR,
    train_monitor_host=TRAIN_MONITOR_HOST,
    train_monitor_port=TRAIN_MONITOR_PORT,
    resolve_project_path=resolve_project_path,
)


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
    filename = str(getattr(upload_file, "filename", "") or "").strip()
    try:
        ok, status, project_name = save_uploaded_project_zip(upload_file)
    except Exception as e:
        write_audit_log("upload_project", "failed", target=filename, details=str(e))
        return jsonify({
            "ok": False,
            "status": "exception",
            "message": f"Upload failed: {e}"
        }), 500

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
        write_audit_log("upload_project", "failed", project=project_name or "", target=filename, details=status)
        return jsonify({
            "ok": False,
            "status": status,
            "message": msg_map.get(status, "Upload failed")
        }), 400

    projects = scan_projects()
    write_audit_log("upload_project", "success", project=project_name or "", target=filename, details="uploaded")
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
        payload = build_state_payload_locked()
    return jsonify(payload)


@app.route("/api/project/note", methods=["POST"])
def api_project_note():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    note = str(data.get("note", "") or "")
    note = note.replace("\r", " ").replace("\n", " ").strip()
    if len(note) > 500:
        note = note[:500]

    if not project:
        write_audit_log("project_note", "failed", details="Missing project name")
        return jsonify({"ok": False, "message": "Missing project name"}), 400

    with state_lock:
        info = STATE["projects"].get(project)
        if not info or not str(info.get("path") or "").strip():
            write_audit_log("project_note", "failed", project=project, details="Project not found")
            return jsonify({"ok": False, "message": "Project not found"}), 404

    try:
        update_project_note_file(project, note)
    except Exception as e:
        write_audit_log("project_note", "failed", project=project, details=str(e))
        return jsonify({"ok": False, "message": f"Save note failed: {e}"}), 500

    with state_lock:
        info = STATE["projects"].get(project)
        if info is not None:
            info["note"] = note
            bump_state_version_locked()

    write_audit_log("project_note", "success", project=project, details="updated" if note else "cleared")
    return jsonify({"ok": True, "project": project, "note": note, "message": "Saved"})


@app.route("/api/snapshot")
def api_snapshot():
    log_project = request.args.get("log_project", "")
    log_tail = request.args.get("log_tail", str(API_LOG_TAIL_LINES))
    with state_lock:
        payload = build_snapshot_payload_locked(log_project, log_tail)
    return jsonify(payload)


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
    with_state = str(request.args.get("with_state", "0")).strip().lower() in {"1", "true", "yes"}
    log_project = request.args.get("log_project", "")
    log_tail = request.args.get("log_tail", str(API_LOG_TAIL_LINES))

    payload = {
        "ok": True,
        "changed": changed,
        "version": version,
    }
    if changed and with_snapshot:
        with state_lock:
            snapshot = build_snapshot_payload_locked(log_project, log_tail)
        payload["snapshot"] = snapshot
    elif changed and with_state:
        with state_lock:
            payload["state"] = build_state_payload_locked()
    return jsonify(payload)


@app.route("/api/log")
def api_log():
    project = request.args.get("project", "")
    tail = request.args.get("tail", str(API_LOG_TAIL_LINES))
    with state_lock:
        payload = build_project_log_payload_locked(project, tail)
    return jsonify(payload)


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


@app.route("/api/queue/reorder", methods=["POST"])
def api_queue_reorder():
    data = request.get_json(force=True)
    project = str(data.get("project", "") or "").strip()
    direction = str(data.get("direction", "") or "").strip().lower()

    if not project:
        return jsonify({"ok": False, "message": "Missing project name"}), 400

    ok, msg = reorder_queued_project(project, direction)
    status = 200 if ok else 400
    return jsonify({"ok": ok, "message": msg}), status


@app.route("/api/queue_session/status")
def api_queue_session_status():
    data = load_queue_session_file()
    projects = []
    for name in list(data.get("projects") or []):
        text = str(name or "").strip()
        if text and text not in projects:
            projects.append(text)

    with state_lock:
        active_now = bool(STATE.get("current")) or bool(STATE.get("queue"))

    return jsonify({
        "ok": True,
        "pending": bool(projects) and not active_now,
        "projects": projects,
        "saved_at": str(data.get("saved_at") or ""),
        "current": str(data.get("current") or ""),
        "queue": [str(x or "").strip() for x in list(data.get("queue") or []) if str(x or "").strip()],
    })


@app.route("/api/queue_session/ignore", methods=["POST"])
def api_queue_session_ignore():
    clear_queue_session_file()
    write_audit_log("queue_session_ignore", "success", details="Ignored last queue session")
    return jsonify({"ok": True, "message": "Ignored last session"})


@app.route("/api/queue_session/continue", methods=["POST"])
def api_queue_session_continue():
    data = load_queue_session_file()
    saved_current = str(data.get("current") or "").strip()
    projects = []
    for name in list(data.get("projects") or []):
        text = str(name or "").strip()
        if text and text not in projects:
            projects.append(text)

    if not projects:
        return jsonify({"ok": False, "message": "No saved queue session"}), 404

    restored_current = False
    if saved_current and saved_current in projects:
        with state_lock:
            restored_current = restore_saved_current_project_locked(saved_current)
        projects = [name for name in projects if name != saved_current]

    clear_queue_session_file()
    added, skipped = queue_projects(projects)
    if restored_current:
        with state_lock:
            sync_queue_session_file_locked()
        added = [saved_current] + added
    write_audit_log(
        "queue_session_continue",
        "success",
        details=f"Continue last session: restored_current={restored_current}, added={len(added)}, skipped={len(skipped)}",
    )
    return jsonify({
        "ok": True,
        "added": added,
        "skipped": skipped,
        "restored_current": restored_current,
        "message": f"Continued last session: added {len(added)}, skipped {len(skipped)}",
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


@app.route("/api/lv3/verify", methods=["POST"])
def api_lv3_verify():
    data = request.get_json(force=True) or {}
    password = str(data.get("password", "") or "")
    if password != LV3_PASSWORD:
        write_audit_log("lv3_verify", "failed", details="Invalid LV3 password")
        return jsonify({"ok": False, "message": "Sai mật khẩu LV3"}), 403
    mark_lv3_authenticated()
    write_audit_log("lv3_verify", "success", details="LV3 verified")
    return jsonify({"ok": True, "message": "LV3 verified"})


@app.route("/api/activity_log")
def api_activity_log():
    lv3 = require_lv3_json()
    if lv3 is not None:
        write_audit_log("activity_log", "denied", details="LV3 password required")
        return lv3

    try:
        limit = max(1, min(int(request.args.get("limit", "500")), 5000))
    except Exception:
        limit = 500
    session_only = str(request.args.get("session", "1") or "1").strip().lower() not in {"0", "false", "no"}
    items = load_audit_log_items(limit, session_only=session_only)
    return jsonify({
        "ok": True,
        "items": items,
        "count": len(items),
        "session_id": current_session_id(),
        "session_only": session_only,
    })


@app.route("/api/accounts")
def api_accounts():
    lv3 = require_lv3_json()
    if lv3 is not None:
        write_audit_log("accounts_list", "denied", details="LV3 password required")
        return lv3

    accounts = load_user_accounts()
    current_user = current_auth_user()
    return jsonify({
        "ok": True,
        "current_user": current_user,
        "items": [
            {
                "username": str(x.get("username", "") or ""),
                "created_at": str(x.get("created_at", "") or ""),
                "current": str(x.get("username", "") or "") == current_user,
            }
            for x in accounts
        ],
    })


@app.route("/api/accounts", methods=["POST"])
def api_accounts_create():
    lv3 = require_lv3_json()
    if lv3 is not None:
        write_audit_log("account_create", "denied", details="LV3 password required")
        return lv3

    data = request.get_json(force=True) or {}
    username = str(data.get("username", "") or "").strip()
    password = str(data.get("password", "") or "")
    ok, status, details = create_user_account(username, password)
    msg_map = {
        "created": "Đã tạo tài khoản",
        "missing_username": "Thiếu tên tài khoản",
        "invalid_username": "Tên tài khoản chỉ được dùng chữ, số, dấu ., _, @, -",
        "password_too_short": "Mật khẩu phải có ít nhất 4 ký tự",
        "user_exists": "Tài khoản đã tồn tại",
    }
    write_audit_log("account_create", "success" if ok else "failed", target=username, details=msg_map.get(status, status))
    return jsonify({
        "ok": ok,
        "status": status,
        "message": msg_map.get(status, status),
        **(details or {}),
    }), (200 if ok else 400)


@app.route("/api/accounts/delete", methods=["POST"])
def api_accounts_delete():
    lv3 = require_lv3_json()
    if lv3 is not None:
        write_audit_log("account_delete", "denied", details="LV3 password required")
        return lv3

    data = request.get_json(force=True) or {}
    username = str(data.get("username", "") or "").strip()
    if username.lower() == current_auth_user().lower():
        write_audit_log("account_delete", "failed", target=username, details="Cannot delete current user")
        return jsonify({"ok": False, "message": "Không thể xóa tài khoản đang đăng nhập"}), 400

    ok, status, details = delete_user_account(username)
    msg_map = {
        "deleted": "Đã xóa tài khoản",
        "not_found": "Không tìm thấy tài khoản",
        "last_user": "Không thể xóa tài khoản cuối cùng",
    }
    write_audit_log("account_delete", "success" if ok else "failed", target=username, details=msg_map.get(status, status))
    return jsonify({
        "ok": ok,
        "status": status,
        "message": msg_map.get(status, status),
        **(details or {}),
    }), (200 if ok else 400)


@app.route("/api/server/restart", methods=["POST"])
def api_server_restart():
    lv3 = require_lv3_json()
    if lv3 is not None:
        write_audit_log("server_restart", "denied", details="LV3 password required")
        return lv3

    with state_lock:
        sync_queue_session_file_locked()
    write_audit_log("server_restart", "success", details="Restart requested from toolbar")
    restart_current_server_soon(1.0)
    return jsonify({
        "ok": True,
        "message": "Server is restarting. Reload the page in a few seconds.",
    })


@app.route("/api/history/clear", methods=["POST"])
def api_history_clear():
    lv2 = require_lv2_json()
    if lv2 is not None:
        write_audit_log("history_clear", "denied", details="LV2 password required")
        return lv2

    with state_lock:
        clear_train_history_locked()
        clear_monitor_cache_locked()
        bump_state_version_locked()

    write_audit_log("history_clear", "success", details="Training history and status cleared")
    return jsonify({"ok": True, "message": "Đã xóa lịch sử train và trạng thái"})


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
    ok, message, payload = start_project_fs_task(project, "rename", {"new_name": new_name})
    write_audit_log("project_rename", "started" if ok else "failed", project=project, target=new_name, details=message)
    return jsonify({"ok": ok, "message": message, **(payload or {})}), (200 if ok else 400)


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

    ok, message, payload = start_project_fs_task(project, "duplicate", {"new_name": new_name})
    write_audit_log("project_duplicate", "started" if ok else "failed", project=project, target=new_name, details=message)
    return jsonify({"ok": ok, "message": message, **(payload or {})}), (200 if ok else 400)


@app.route("/api/project/backup", methods=["POST"])
def api_project_backup():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    if not project:
        write_audit_log("project_backup", "failed", details="Missing project name")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    ok, message, task_id = backup_start_project_backup(
        project,
        BACKUP_ROOT,
        BACKUP_COPY_CHUNK_SIZE,
        resolve_project_path,
    )
    if not ok:
        write_audit_log("project_backup", "failed", project=project, details=message)
        return jsonify({"ok": False, "message": message}), 400

    write_audit_log("project_backup", "started", project=project, target=str(BACKUP_ROOT), details=message)
    return jsonify({
        "ok": True,
        "message": message,
        "task_id": task_id,
        "target_root": str(BACKUP_ROOT),
    })


@app.route("/api/project/backup_status")
def api_project_backup_status():
    return jsonify(backup_build_backup_status_payload())


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

    ok, message, payload = start_project_fs_task(project, "delete", {"project": project})
    write_audit_log("project_delete", "started" if ok else "failed", project=project, details=message)
    return jsonify({"ok": ok, "message": message, **(payload or {})}), (200 if ok else 400)


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
    ok, message, payload = start_dataset_task(project, "clear", {"project": project})
    write_audit_log("project_clear_dataset", "started" if ok else "failed", project=project, details=message)
    return jsonify({
        "ok": ok,
        "message": message,
        **(payload or {}),
    }), (200 if ok else 400)


@app.route("/api/projects/clear_selected_dataset", methods=["POST"])
def api_projects_clear_selected_dataset():
    lv2 = require_lv2_json()
    if lv2 is not None:
        write_audit_log("projects_clear_selected_dataset", "denied", details="LV2 password required")
        return lv2

    data = request.get_json(force=True) or {}
    projects = data.get("projects", [])
    if not isinstance(projects, list) or not projects:
        write_audit_log("projects_clear_selected_dataset", "failed", details="No project selected")
        return jsonify({"ok": False, "message": "No project selected"}), 400

    ok_count, failed_count, results = clear_split_dataset_for_projects(projects)
    message = f"Cleared dataset for {ok_count} project(s), failed {failed_count}"
    if failed_count:
        failed_names = [str(x.get("project") or "") for x in results if not x.get("ok")]
        if failed_names:
            message = f"{message}: {', '.join(failed_names[:10])}"
    write_audit_log(
        "projects_clear_selected_dataset",
        "success" if failed_count == 0 else "partial",
        details=message,
    )
    return jsonify({
        "ok": failed_count == 0,
        "message": message,
        "cleared": ok_count,
        "failed": failed_count,
        "results": results,
    }), (200 if failed_count == 0 else 207)


@app.route("/api/project/import_data_zip", methods=["POST"])
def api_project_import_data_zip():
    project = str(request.form.get("project", "") or "").strip()
    upload_file = request.files.get("file")

    if not project:
        write_audit_log("project_import_data_zip", "failed", details="Missing project name")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    ok, status, details = import_project_data_zip(project, upload_file)
    msg_map = {
        "project_not_found": "Không tìm thấy project",
        "cannot_create_image_dir": "Không tạo được thư mục image",
        "cannot_create_label_dir": "Không tạo được thư mục labels",
        "missing_file": "Thiếu file upload",
        "only_zip_supported": "Chỉ hỗ trợ file .zip",
        "zip_empty": "File zip trống",
        "invalid_zip_path": "Đường dẫn trong zip không hợp lệ",
        "bad_zip_file": "File zip bị lỗi",
        "extract_failed": "Không giải nén được file zip",
        "no_data_found": "Không tìm thấy ảnh hoặc label hợp lệ trong zip",
        "copy_failed": "Không copy được dữ liệu vào project",
    }

    if not ok:
        write_audit_log("project_import_data_zip", "failed", project=project, details=msg_map.get(status, status))
        return jsonify({
            "ok": False,
            "status": status,
            "message": msg_map.get(status, "Import data failed"),
        }), 400

    scan_projects()
    images_added = int((details or {}).get("images_added", 0) or 0)
    labels_added = int((details or {}).get("labels_added", 0) or 0)
    message = f"Đã thêm dữ liệu vào {project}: {images_added} ảnh, {labels_added} label"
    write_audit_log("project_import_data_zip", "success", project=project, details=message)
    return jsonify({
        "ok": True,
        "status": "imported",
        "project": project,
        "images_added": images_added,
        "labels_added": labels_added,
        "message": message,
    })


@app.route("/api/project/import_data_images", methods=["POST"])
def api_project_import_data_images():
    project = str(request.form.get("project", "") or "").strip()
    upload_files = request.files.getlist("files")
    if not project:
        write_audit_log("project_import_data_images", "failed", details="Missing project name")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    ok, status, details = import_project_data_images(project, upload_files)
    msg_map = {
        "project_not_found": "Không tìm thấy project",
        "cannot_create_image_dir": "Không tạo được thư mục image",
        "missing_file": "Thiếu file upload",
        "invalid_upload_path": "Đường dẫn file upload không hợp lệ",
        "no_image_found": "Không có file ảnh hợp lệ",
        "copy_failed": "Không copy được file vào project",
    }

    if not ok:
        write_audit_log("project_import_data_images", "failed", project=project, details=msg_map.get(status, status))
        return jsonify({
            "ok": False,
            "status": status,
            "message": msg_map.get(status, status),
            **(details or {}),
        }), 400

    invalidate_project_bbox_analysis(project)
    images_added = int((details or {}).get("images_added", 0) or 0)
    skipped_files = int((details or {}).get("skipped_files", 0) or 0)
    message = f"Đã thêm ảnh vào {project}: {images_added} ảnh"
    if skipped_files:
        message += f", bỏ qua {skipped_files} file không phải ảnh"
    write_audit_log("project_import_data_images", "success", project=project, details=message)
    return jsonify({
        "ok": True,
        "status": "imported",
        "project": project,
        "images_added": images_added,
        "labels_added": 0,
        "skipped_files": skipped_files,
        "message": message,
        "saved_names": (details or {}).get("saved_names", []),
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

    ok, message, payload = start_dataset_task(project, "create", {
        **data,
        "project": project,
        "split_mode": split_mode,
    })
    write_audit_log("project_create_dataset", "started" if ok else "failed", project=project, details=message)
    return jsonify({
        "ok": ok,
        "message": message,
        **(payload or {})
    }), (200 if ok else 400)


@app.route("/api/project/merge_train_valid", methods=["POST"])
def api_project_merge_train_valid():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    if not project:
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    ok, message, payload = start_dataset_task(project, "merge", {"project": project})
    write_audit_log("project_merge_train_valid", "started" if ok else "failed", project=project, details=message)
    return jsonify({
        "ok": ok,
        "message": message,
        **(payload or {})
    }), (200 if ok else 400)


@app.route("/api/project/dataset_task/status")
def api_project_dataset_task_status():
    return jsonify(build_dataset_task_status_payload())


@app.route("/api/project/fs_task/status")
def api_project_fs_task_status():
    return jsonify(build_project_fs_task_status_payload())


@app.route("/api/project/revalidate_run", methods=["POST"])
def api_project_revalidate_run():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    run_folder = str(data.get("run_folder", "") or "").strip()
    if not project or not run_folder:
        return jsonify({"ok": False, "message": "Missing project or run_folder"}), 400

    ok, message, payload = start_revalidate_task(project, run_folder)
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


@app.route("/api/project/revalidate_run/status")
def api_project_revalidate_run_status():
    return jsonify(build_revalidate_status_payload())


@app.route("/api/project/model_testing", methods=["POST"])
def api_project_model_testing():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    run_folder = str(data.get("run_folder", "") or "").strip()
    if not project or not run_folder:
        return jsonify({"ok": False, "message": "Missing project or run_folder"}), 400

    ok, message, payload = start_model_test_task(project, run_folder)
    write_audit_log(
        "project_model_testing",
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


@app.route("/api/project/model_testing_msa", methods=["POST"])
def api_project_model_testing_msa():
    project = request.form.get("project", "").strip()
    run_folder = request.form.get("run_folder", "").strip()
    upload_file = request.files.get("file")

    if not project or not run_folder:
        return jsonify({"ok": False, "message": "Missing project or run_folder"}), 400

    if not upload_file or not upload_file.filename:
        return jsonify({"ok": False, "message": "Missing sample ZIP file"}), 400

    ok, message, payload = run_msa_model_testing(project, run_folder, upload_file)
    write_audit_log(
        "project_model_testing_msa",
        "success" if ok else "failed",
        project=project,
        target=run_folder,
        details=message,
    )

    if not ok:
        return jsonify({
            "ok": False,
            "message": message
        }), 400

    project_path = resolve_project_path(project)
    run_dir = Path(payload.get("run_dir", "")) if payload and payload.get("run_dir") else None
    if project_path and project_path.exists() and run_dir and run_dir.exists():
        msa_summary = build_msa_testing_summary(project_path, run_dir)
    else:
        msa_summary = {}

    return jsonify({
        "ok": True,
        "message": message,
        "run_folder": run_folder,
        "msa_summary": msa_summary,
    })


@app.route("/msa_testing")
def msa_testing_page():
    project = request.args.get("project", "").strip()
    msa_root = msa_root_for_project(project)
    if msa_root is None:
        abort(404)
    for folder in ("images", "labels", "results"):
        (msa_root / folder).mkdir(parents=True, exist_ok=True)
    return render_template("msa_testing.html", project_name=project)


@app.route("/api/project/msa")
def api_project_msa():
    project = request.args.get("project", "").strip()
    msa_root = msa_root_for_project(project)
    if msa_root is None:
        return jsonify({"ok": False, "message": "Project not found"}), 404
    image_dir = msa_root / "images"
    label_dir = msa_root / "labels"
    samples = []
    if image_dir.is_dir():
        samples = [{"name": image.name, "has_label": (label_dir / f"{image.stem}.txt").is_file()}
                   for image in sorted(image_dir.iterdir())
                   if image.is_file() and image.suffix.lower() in IMAGE_EXTS]
    results = []
    results_dir = msa_root / "results"
    if results_dir.is_dir():
        for folder in sorted(results_dir.iterdir(), reverse=True):
            file_path = folder / "results.json"
            if folder.is_dir() and file_path.is_file():
                try:
                    data = json.loads(file_path.read_text(encoding="utf-8"))
                    results.append({key: data.get(key) for key in (
                        "id", "run_folder", "weight", "created_at", "total_images", "wrong_images", "error_rate")})
                except (OSError, ValueError):
                    continue
    with msa_task_lock:
        task = dict(MSA_TASK)
        upload_task = dict(MSA_UPLOAD_TASK)
        label_task = dict(MSA_LABEL_TASK)
    return jsonify({"ok": True, "models": list_msa_models(project),
                    "samples": samples, "results": results, "task": task,
                    "upload_task": upload_task, "label_task": label_task})


@app.route("/api/project/msa/upload", methods=["POST"])
def api_project_msa_upload():
    project = request.form.get("project", "").strip()
    upload_file = request.files.get("file")
    if not upload_file or not upload_file.filename:
        return jsonify({"ok": False, "message": "Missing ZIP file"}), 400
    if msa_root_for_project(project) is None:
        return jsonify({"ok": False, "message": "Project not found"}), 404
    with msa_task_lock:
        if any(task["status"] == "running" for task in (MSA_UPLOAD_TASK, MSA_TASK, MSA_LABEL_TASK)):
            return jsonify({"ok": False, "message": "Another MSA task is running"}), 409
        MSA_UPLOAD_TASK.update(status="running", project=project, message="Đang nhận ZIP",
                               progress=0, processed=0, total=0)
    temp_dir = tempfile.TemporaryDirectory(prefix="msa_upload_")
    zip_path = Path(temp_dir.name) / "upload.zip"
    try:
        upload_file.save(str(zip_path))
    except Exception as exc:
        temp_dir.cleanup()
        with msa_task_lock:
            MSA_UPLOAD_TASK.update(status="failed", message=f"Unable to save ZIP: {exc}")
        return jsonify({"ok": False, "message": "Unable to save ZIP"}), 500
    actor = msa_audit_actor()
    threading.Thread(target=import_msa_zip_task, args=(project, zip_path, temp_dir, actor), daemon=True).start()
    return jsonify({"ok": True, "message": "MSA import started"})


@app.route("/api/project/msa/run", methods=["POST"])
def api_project_msa_run():
    data = request.get_json(silent=True) or {}
    project = str(data.get("project") or "").strip()
    run_folder = str(data.get("run_folder") or "").strip()
    weight = str(data.get("weight") or "").strip()
    msa_root = msa_root_for_project(project)
    run_dir, err = resolve_run_dir(project, run_folder)
    if msa_root is None or err or not run_dir or weight not in {"best.pt", "last.pt"} or not (run_dir / "weights" / weight).is_file():
        return jsonify({"ok": False, "message": "Project or model not found"}), 400
    with msa_task_lock:
        if any(task["status"] == "running" for task in (MSA_UPLOAD_TASK, MSA_TASK, MSA_LABEL_TASK)):
            return jsonify({"ok": False, "message": "Another MSA test is running"}), 409
        MSA_TASK.update(status="running", project=project, message="Tải model",
                        result_id="", progress=0, processed=0, total=0)
    actor = msa_audit_actor()
    threading.Thread(target=run_project_msa_test_task, args=(project, run_folder, weight, actor), daemon=True).start()
    return jsonify({"ok": True, "message": "MSA test started"})


@app.route("/api/project/msa/predict_labels", methods=["POST"])
def api_project_msa_predict_labels():
    data = request.get_json(silent=True) or {}
    project = str(data.get("project") or "").strip()
    run_folder = str(data.get("run_folder") or "").strip()
    weight = str(data.get("weight") or "").strip()
    msa_root = msa_root_for_project(project)
    run_dir, err = resolve_run_dir(project, run_folder)
    if msa_root is None or err or not run_dir or weight not in {"best.pt", "last.pt"} or not (run_dir / "weights" / weight).is_file():
        return jsonify({"ok": False, "message": "Project or model not found"}), 400
    with msa_task_lock:
        if any(task["status"] == "running" for task in (MSA_UPLOAD_TASK, MSA_TASK, MSA_LABEL_TASK)):
            return jsonify({"ok": False, "message": "Another MSA task is running"}), 409
        MSA_LABEL_TASK.update(status="running", project=project, message="Tải model",
                              progress=0, processed=0, total=0, generated=0)
    actor = msa_audit_actor()
    threading.Thread(target=predict_missing_msa_labels_task,
                     args=(project, run_folder, weight, actor), daemon=True).start()
    return jsonify({"ok": True, "message": "MSA label prediction started"})


@app.route("/api/project/msa/status")
def api_project_msa_status():
    with msa_task_lock:
        task = dict(MSA_TASK)
        upload_task = dict(MSA_UPLOAD_TASK)
        label_task = dict(MSA_LABEL_TASK)
    return jsonify({"ok": True, "task": task, "upload_task": upload_task, "label_task": label_task})


@app.route("/api/project/msa/result")
def api_project_msa_result():
    msa_root = msa_root_for_project(request.args.get("project", "").strip())
    result_id = request.args.get("id", "").strip()
    if msa_root is None or not re.fullmatch(r"[0-9]{8}_[0-9]{6}_[0-9]{6}_[0-9a-f]{6}", result_id):
        abort(404)
    result_file = msa_root / "results" / result_id / "results.json"
    if not result_file.is_file():
        abort(404)
    return jsonify({"ok": True, "result": json.loads(result_file.read_text(encoding="utf-8"))})


@app.route("/api/project/msa/sample")
def api_project_msa_sample():
    project = request.args.get("project", "").strip()
    result_id = request.args.get("id", "").strip()
    image_name = request.args.get("image", "").strip()
    msa_root = msa_root_for_project(project)
    if msa_root is None or not re.fullmatch(r"[0-9]{8}_[0-9]{6}_[0-9]{6}_[0-9a-f]{6}", result_id):
        abort(404)
    image_path, label_path = resolve_msa_image_and_label(project, image_name)
    result_file = msa_root / "results" / result_id / "results.json"
    if image_path is None or not result_file.is_file():
        abort(404)
    results = json.loads(result_file.read_text(encoding="utf-8"))
    if not any(item.get("image_name") == image_name for item in results.get("sample_items", [])):
        abort(404)
    pred_label = result_file.parent / "prediction" / "labels" / f"{image_path.stem}.txt"
    gt_rows = _read_yolo_label_file(label_path)
    pred_rows = _read_yolo_label_file(pred_label)
    pred_statuses, missed_gt = classify_msa_boxes(gt_rows, pred_rows)
    class_names = results.get("class_names") or {}
    if not class_names:
        classes, _ = load_project_classes(msa_root.parent)
        class_names = {str(i): name for i, name in enumerate(classes)}
    return jsonify({"ok": True, "image_rel": f"images/{image_name}",
                    "gt_boxes": gt_rows, "pred_boxes": pred_rows,
                    "pred_statuses": pred_statuses, "missed_gt_indices": missed_gt,
                    "class_names": class_names})


@app.route("/api/project/msa/file")
def api_project_msa_file():
    msa_root = msa_root_for_project(request.args.get("project", "").strip())
    rel = request.args.get("rel", "").strip()
    if msa_root is None or not rel:
        abort(404)
    file_path = (msa_root / rel).resolve()
    if not is_path_inside(file_path, msa_root) or file_path.suffix.lower() not in IMAGE_EXTS or not file_path.is_file():
        abort(404)
    return send_file(file_path)


def resolve_msa_image_and_label(project: str, rel: str):
    msa_root = msa_root_for_project(project)
    if msa_root is None or not rel or Path(rel).name != rel:
        return None, None
    image_path = (msa_root / "images" / rel).resolve()
    if not is_path_inside(image_path, msa_root / "images") or image_path.suffix.lower() not in IMAGE_EXTS or not image_path.is_file():
        return None, None
    label_path = msa_root / "labels" / f"{image_path.stem}.txt"
    return image_path, label_path


@app.route("/api/project/msa/images")
def api_project_msa_images():
    project = request.args.get("project", "").strip()
    msa_root = msa_root_for_project(project)
    if msa_root is None:
        return jsonify({"ok": False, "message": "Project not found"}), 404
    image_dir = msa_root / "images"
    images = sorted((image for image in image_dir.iterdir() if image.is_file() and image.suffix.lower() in IMAGE_EXTS),
                    key=lambda image: image.name.lower()) if image_dir.is_dir() else []
    items = [{"rel": image.name, "name": image.name, "modified_at": datetime.fromtimestamp(image.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
              "bbox_count": len(_read_yolo_label_file(msa_root / "labels" / f"{image.stem}.txt")), "class_ids": []}
             for image in images]
    return jsonify({"ok": True, "images": [image.name for image in images], "items": items,
                    "total": len(images), "page": 1, "total_pages": 1})


@app.route("/api/project/msa/label", methods=["GET", "POST"])
def api_project_msa_label():
    data = request.get_json(silent=True) or {} if request.method == "POST" else request.args
    project = str(data.get("project") or "").strip()
    rel = str(data.get("rel") or "").strip()
    image_path, label_path = resolve_msa_image_and_label(project, rel)
    if image_path is None:
        return jsonify({"ok": False, "message": "MSA image not found"}), 404
    if request.method == "GET":
        text = label_path.read_text(encoding="utf-8") if label_path.is_file() else ""
        return jsonify({"ok": True, "text": text, "exists": label_path.is_file(),
                        "bbox_count": len(_read_yolo_label_rows(text)), "bbox_anomaly_count": 0,
                        "anomaly_box_indices": [], "anomaly_items": []})
    text = str(data.get("text") or "").replace("\r\n", "\n")
    try:
        label_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = label_path.with_name(label_path.name + ".tmp")
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, label_path)
    except OSError as exc:
        return jsonify({"ok": False, "message": f"Cannot save MSA label: {exc}"}), 500
    write_audit_log("label_editor_msa_save_label", "success", project=project, target=rel,
                    details=f"bbox={len(_read_yolo_label_rows(text))}")
    return jsonify({"ok": True, "message": "MSA label saved"})


@app.route("/api/project/model_testing/status")
def api_project_model_testing_status():
    return jsonify(build_model_test_status_payload())


@app.route("/api/project/predict_unlabeled", methods=["POST"])
def api_project_predict_unlabeled():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    weights_rel = str(data.get("weights_rel", "") or "").strip().replace("\\", "/")
    options = data.get("options") if isinstance(data.get("options"), dict) else {}
    if not project or not weights_rel:
        return jsonify({"ok": False, "message": "Missing project or weights_rel"}), 400

    ok, message, payload = start_predict_task(project, weights_rel, options)
    write_audit_log(
        "project_predict_unlabeled",
        "success" if ok else "failed",
        project=project,
        target=weights_rel,
        details=message,
    )
    return jsonify({
        "ok": ok,
        "message": message,
        **(payload or {}),
    }), (200 if ok else 400)


@app.route("/api/project/predict_unlabeled/quick", methods=["POST"])
def api_project_predict_unlabeled_quick():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    options = data.get("options") if isinstance(data.get("options"), dict) else {}
    if not project:
        return jsonify({"ok": False, "message": "Missing project"}), 400

    weights_rel, err = find_latest_best_weight_rel(project)
    if err or not weights_rel:
        return jsonify({"ok": False, "message": err or "No best.pt found"}), 404

    quick_options = {
        "conf": 0.5,
        "iou": 0.65,
        "imgsz": 640,
        "device": "",
        "max_det": 300,
        "save_conf": False,
        "include_empty_labels": False,
        "save_empty_labels": False,
    }
    quick_options.update(options or {})

    ok, message, payload = start_predict_task(project, weights_rel, quick_options)
    write_audit_log(
        "project_predict_unlabeled_quick",
        "success" if ok else "failed",
        project=project,
        target=weights_rel,
        details=message,
    )
    return jsonify({
        "ok": ok,
        "message": message,
        "weights_rel": weights_rel,
        **(payload or {}),
    }), (200 if ok else 400)


@app.route("/api/project/predict_unlabeled/status")
def api_project_predict_unlabeled_status():
    return jsonify(build_predict_status_payload())


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
    _, unlabeled_rows, _ = find_unlabeled_project_images(project, include_empty_labels=False)
    output_info = get_output_model_train_weights_info(project_path)
    run_details = get_output_model_train_runs_info(project_path)
    dataset_cfg = load_dataset_config(project_path)

    return jsonify({
        "ok": True,
        "project": project,
        "image_folder_count": image_folder_count,
        "unlabeled_image_count": len(unlabeled_rows or []),
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
        write_audit_log("label_editor_list_images", "failed", details="Missing project")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    try:
        page = max(1, int(request.args.get("page", "1")))
    except Exception:
        page = 1

    try:
        page_size = max(1, min(int(request.args.get("page_size", "200")), 1000000))
    except Exception:
        page_size = 200

    q = str(request.args.get("q", "") or "").strip().lower()
    sort_by = str(request.args.get("sort_by", "name") or "name").strip().lower()
    sort_dir = str(request.args.get("sort_dir", "asc") or "asc").strip().lower()
    class_filter_raw = str(request.args.get("class_filter", "") or "").strip()
    expected_boxes_raw = str(request.args.get("expected_boxes", "") or "").strip()
    box_compare = str(request.args.get("box_compare", "all") or "all").strip().lower()
    anomaly_filter = str(request.args.get("anomaly_filter", "all") or "all").strip().lower()
    anomaly_sensitivity = str(request.args.get("anomaly_sensitivity", "medium") or "medium").strip().lower()
    target_rel = str(request.args.get("rel", "") or "").strip().replace("\\", "/")
    class_filter_ids = set()
    expected_boxes = None
    if class_filter_raw != "":
        for part in class_filter_raw.replace(";", ",").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                class_filter_ids.add(int(part))
            except Exception:
                continue
    if expected_boxes_raw != "":
        try:
            expected_boxes = max(0, int(expected_boxes_raw))
        except Exception:
            expected_boxes = None

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        write_audit_log("label_editor_list_images", "failed", project=project, details="Project not found")
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404

    image_dir, all_images = list_project_images(project_path, limit=0)
    bbox_analysis = get_project_bbox_analysis(project)
    image_analysis_map = dict((bbox_analysis or {}).get("images") or {})
    group_stats = dict((bbox_analysis or {}).get("group_stats") or {})
    if not image_dir:
        write_audit_log(
            "label_editor_list_images",
            "success",
            project=project,
            details="image_dir_missing count=0",
        )
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

    for rel in all_images:
        abs_path = (image_dir / rel).resolve()
        try:
            st = abs_path.stat()
            modified_ts = float(st.st_mtime)
        except Exception:
            modified_ts = 0.0

        image_analysis = dict(image_analysis_map.get(rel) or {})
        label_rows = list(image_analysis.get("rows") or [])
        class_ids = sorted(set(int(row.get("cls", -1)) for row in label_rows if int(row.get("cls", -1)) >= 0))
        analysis_info = evaluate_bbox_image_anomalies(group_stats, image_analysis, anomaly_sensitivity)
        bbox_count = int(analysis_info.get("bbox_count", 0) or 0)

        items.append({
            "rel": rel,
            "name": Path(rel).name,
            "modified_ts": modified_ts,
            "modified_at": datetime.fromtimestamp(modified_ts).strftime("%Y-%m-%d %H:%M:%S") if modified_ts > 0 else "-",
            "class_ids": class_ids,
            "class_min": class_ids[0] if class_ids else 10**9,
            "bbox_count": bbox_count,
            "bbox_anomaly_count": int(analysis_info.get("anomaly_count", 0) or 0),
            "bbox_has_anomaly": bool(analysis_info.get("has_anomaly", False)),
        })

    if q:
        items = [x for x in items if q in str(x.get("name", "")).lower() or q in str(x.get("rel", "")).lower()]

    if class_filter_ids:
        items = [x for x in items if class_filter_ids.intersection(set(x.get("class_ids") or []))]

    if expected_boxes is not None:
        if box_compare == "less":
            items = [x for x in items if int(x.get("bbox_count", 0) or 0) < expected_boxes]
        elif box_compare == "greater":
            items = [x for x in items if int(x.get("bbox_count", 0) or 0) > expected_boxes]
        elif box_compare == "equal":
            items = [x for x in items if int(x.get("bbox_count", 0) or 0) == expected_boxes]
        elif box_compare == "notequal":
            items = [x for x in items if int(x.get("bbox_count", 0) or 0) != expected_boxes]

    if anomaly_filter == "has":
        items = [x for x in items if bool(x.get("bbox_has_anomaly"))]
    elif anomaly_filter == "clean":
        items = [x for x in items if not bool(x.get("bbox_has_anomaly"))]

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
    if target_rel and total > 0:
        target_rel_cf = target_rel.casefold()
        for pos, item in enumerate(items):
            if str(item.get("rel", "") or "").casefold() == target_rel_cf:
                page = (pos // page_size) + 1
                break
    if total_pages > 0:
        page = min(page, total_pages)
    else:
        page = 1

    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    images = [x.get("rel") for x in page_items]

    write_audit_log(
        "label_editor_list_images",
        "success",
        project=project,
        details=(
            f"count={len(images)} total={total} page={page}/{total_pages} "
            f"q={q or '-'} class={class_filter_raw or '-'} "
            f"bbox={box_compare}:{expected_boxes_raw or '-'} anomaly={anomaly_filter}:{anomaly_sensitivity}"
        ),
    )
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
        write_audit_log("label_editor_classes", "failed", details="Missing project")
        return jsonify({"ok": False, "message": "Thiếu tên project"}), 400

    project_path = resolve_project_path(project)
    if not project_path or not project_path.exists():
        write_audit_log("label_editor_classes", "failed", project=project, details="Project not found")
        return jsonify({"ok": False, "message": "Không tìm thấy project"}), 404

    classes, class_file = load_project_classes(project_path)
    write_audit_log("label_editor_classes", "success", project=project, details=f"count={len(classes)}")
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
        write_audit_log("label_editor_view_image", "failed", project=project, target=rel, details="Missing project or rel")
        abort(400)

    img_file, err = resolve_project_image_file(project, rel)
    if err or not img_file:
        write_audit_log("label_editor_view_image", "failed", project=project, target=rel, details=err or "file_not_found")
        abort(404)

    write_audit_log("label_editor_view_image", "success", project=project, target=rel, details=img_file.name)
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
        write_audit_log("label_editor_image_info", "failed", project=project, target=rel, details="Missing project or rel")
        return jsonify({"ok": False, "message": "Thiếu project hoặc rel"}), 400

    info, err = get_project_image_info(project, rel)
    if err or not info:
        write_audit_log("label_editor_image_info", "failed", project=project, target=rel, details=err or "file_not_found")
        return jsonify({"ok": False, "message": err or "file_not_found"}), 404

    write_audit_log(
        "label_editor_image_info",
        "success",
        project=project,
        target=rel,
        details=f"width={info.get('width', '-')} height={info.get('height', '-')} size={info.get('size', '-')}",
    )
    return jsonify({
        "ok": True,
        "project": project,
        **info
    })


@app.route("/api/project_image", methods=["DELETE", "POST"])
def api_project_image_delete():
    data = request.get_json(silent=True) or {}
    project = str(data.get("project") or request.args.get("project", "") or "").strip()
    rel = str(data.get("rel") or request.args.get("rel", "") or "").strip().replace("\\", "/")
    if not project or not rel:
        write_audit_log("label_editor_image_delete", "failed", project=project, target=rel, details="Missing project or rel")
        return jsonify({"ok": False, "message": "Thiếu project hoặc rel"}), 400

    img_file, err = resolve_project_image_file(project, rel)
    if err or not img_file:
        write_audit_log("label_editor_image_delete", "failed", project=project, target=rel, details=err or "file_not_found")
        return jsonify({"ok": False, "message": err or "file_not_found"}), 404

    label_file, _ = resolve_label_file_for_image(project, rel, create_missing=False)
    removed_label = False
    try:
        if label_file and label_file.exists() and label_file.is_file():
            label_file.unlink()
            removed_label = True
        img_file.unlink()
    except Exception as e:
        write_audit_log("project_image_delete", "failed", project=project, target=rel, details=str(e))
        write_audit_log("label_editor_image_delete", "failed", project=project, target=rel, details=str(e))
        return jsonify({"ok": False, "message": f"Không xóa được ảnh: {e}"}), 500

    invalidate_project_bbox_analysis(project)
    write_audit_log("project_image_delete", "success", project=project, target=rel, details="deleted")
    write_audit_log(
        "label_editor_image_delete",
        "success",
        project=project,
        target=rel,
        details=f"removed_label={removed_label}",
    )
    return jsonify({
        "ok": True,
        "project": project,
        "rel": rel,
        "removed_label": removed_label,
        "message": "Đã xóa ảnh" + (" và label" if removed_label else ""),
    })


@app.route("/api/project_label")
def api_project_label():
    project = request.args.get("project", "").strip()
    rel = request.args.get("rel", "").strip()
    anomaly_sensitivity = str(request.args.get("anomaly_sensitivity", "medium") or "medium").strip().lower()
    if not project or not rel:
        write_audit_log("label_editor_load_label", "failed", project=project, target=rel, details="Missing project or rel")
        return jsonify({"ok": False, "message": "Thiếu project hoặc rel"}), 400

    label_file, err = resolve_label_file_for_image(project, rel, create_missing=False)
    if err:
        write_audit_log("label_editor_load_label", "failed", project=project, target=rel, details=err)
        return jsonify({"ok": False, "message": err}), 400

    text = ""
    exists = bool(label_file and label_file.exists() and label_file.is_file())
    if exists:
        try:
            text = label_file.read_text(encoding="utf-8")
        except Exception:
            text = ""

    bbox_analysis = get_project_bbox_analysis(project)
    image_rows_info = dict(((bbox_analysis or {}).get("images") or {}).get(rel) or {})
    group_stats = dict((bbox_analysis or {}).get("group_stats") or {})
    image_analysis = evaluate_bbox_image_anomalies(group_stats, image_rows_info, anomaly_sensitivity)

    write_audit_log(
        "label_editor_load_label",
        "success",
        project=project,
        target=rel,
        details=(
            f"exists={exists} bbox={int(image_analysis.get('bbox_count', 0) or 0)} "
            f"anomaly={int(image_analysis.get('anomaly_count', 0) or 0)} sensitivity={anomaly_sensitivity}"
        ),
    )
    return jsonify({
        "ok": True,
        "project": project,
        "rel": rel,
        "exists": exists,
        "label_path": str(label_file) if label_file else None,
        "text": text,
        "bbox_count": int(image_analysis.get("bbox_count", 0) or 0),
        "bbox_anomaly_count": int(image_analysis.get("anomaly_count", 0) or 0),
        "bbox_has_anomaly": bool(image_analysis.get("has_anomaly", False)),
        "anomaly_box_indices": list(image_analysis.get("anomaly_box_indices") or []),
        "anomaly_items": list(image_analysis.get("anomaly_items") or []),
        "anomaly_sensitivity": str(image_analysis.get("sensitivity") or anomaly_sensitivity),
    })


@app.route("/api/project_label", methods=["POST"])
def api_project_label_save():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    rel = str(data.get("rel", "") or "").strip()
    text = str(data.get("text", "") or "")

    if not project or not rel:
        write_audit_log("label_editor_save_label", "failed", project=project, target=rel, details="Missing project or rel")
        return jsonify({"ok": False, "message": "Thiếu project hoặc rel"}), 400

    label_file, err = resolve_label_file_for_image(project, rel, create_missing=True)
    if err or not label_file:
        write_audit_log("label_editor_save_label", "failed", project=project, target=rel, details=err or "cannot_resolve_label_path")
        return jsonify({"ok": False, "message": err or "cannot_resolve_label_path"}), 400

    try:
        label_file.write_text(text.replace("\r\n", "\n"), encoding="utf-8")
    except Exception as e:
        write_audit_log("label_editor_save_label", "failed", project=project, target=rel, details=str(e))
        return jsonify({"ok": False, "message": "Không lưu được label"}), 500

    update_project_bbox_analysis_image(project, rel, text)
    bbox_count = len(parse_yolo_label_rows(text))
    write_audit_log(
        "label_editor_save_label",
        "success",
        project=project,
        target=rel,
        details=f"bbox={bbox_count} chars={len(text)} label={label_file.name}",
    )

    return jsonify({
        "ok": True,
        "message": "Đã lưu label",
        "label_path": str(label_file)
    })


@app.route("/api/project/promote_valid_to_train", methods=["POST"])
def api_project_promote_valid_to_train():
    data = request.get_json(force=True) or {}
    project = str(data.get("project", "") or "").strip()
    source_rel = str(data.get("source_rel", "") or "").strip()
    valid_rel = str(data.get("valid_rel", "") or "").strip()
    text = data.get("text")

    if not project or not source_rel:
        return jsonify({"ok": False, "message": "Thiếu project hoặc source_rel"}), 400

    ok, message, payload = promote_validation_sample_to_train(
        project_name=project,
        source_rel=source_rel,
        valid_rel=valid_rel or None,
        text=text,
    )
    if ok:
        invalidate_project_bbox_analysis(project)
        write_audit_log(
            "project_promote_valid_to_train",
            "success",
            project=project,
            details=f"source={source_rel} valid={valid_rel or source_rel}",
        )
        return jsonify({"ok": True, "message": message, **(payload or {})})

    write_audit_log(
        "project_promote_valid_to_train",
        "failed",
        project=project,
        details=f"source={source_rel} valid={valid_rel or source_rel}: {message}",
    )
    return jsonify({"ok": False, "message": message}), 400


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

    try:
        rel_parts = file_path.relative_to(project_path).parts
    except Exception:
        abort(404)

    run_folder = rel_parts[-3] if len(rel_parts) >= 3 and rel_parts[-2].lower() == "weights" else file_path.parent.name
    original_name = file_path.name
    zip_name = f"{safe_download_part(project)}__{safe_download_part(run_folder)}__{safe_download_part(original_name)}.zip"

    tmp_dir = tempfile.mkdtemp(prefix="traincontrol_zip_")
    zip_path = Path(tmp_dir) / zip_name

    with zipfile.ZipFile(zip_path, "w") as zf:
        write_file_to_zip_preserve_times(zf, file_path, original_name)

    st = file_path.stat()

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
    manifest = []
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
                write_file_to_zip_preserve_times(zf, file_path, arcname)
                try:
                    st = file_path.stat()
                    manifest.append({
                        "path": arcname,
                        "modified_at": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                        "created_at": datetime.fromtimestamp(st.st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
                        "modified_ts": st.st_mtime,
                        "created_ts": st.st_ctime,
                        "size": st.st_size,
                    })
                except Exception:
                    pass
                file_count += 1

        if manifest:
            zf.writestr(
                "traincontrol_zip_manifest.json",
                json.dumps(manifest, ensure_ascii=False, indent=2),
            )

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


@app.route("/api/download_selected_outputs")
def api_download_selected_outputs():
    raw_projects = request.args.getlist("project")
    if len(raw_projects) == 1 and "," in raw_projects[0]:
        raw_projects = raw_projects[0].split(",")

    projects = []
    for item in raw_projects:
        name = str(item or "").strip()
        if name and name not in projects:
            projects.append(name)

    if not projects:
        return jsonify({"ok": False, "message": "Chưa chọn project nào"}), 400

    selected = []
    skipped = []
    for project in projects:
        weights_rel, err = find_latest_best_weight_rel(project)
        if err or not weights_rel:
            skipped.append({"project": project, "reason": err or "No best.pt found"})
            continue
        file_path, resolve_err = resolve_output_weight_file(project, weights_rel)
        if resolve_err or not file_path:
            skipped.append({"project": project, "reason": resolve_err or "File not found"})
            continue
        selected.append({
            "project": project,
            "weights_rel": weights_rel,
            "file_path": file_path,
        })

    if not selected:
        return jsonify({
            "ok": False,
            "message": "Không tìm thấy best.pt mới nhất trong các project đã chọn",
            "skipped": skipped,
        }), 404

    tmp_dir = tempfile.mkdtemp(prefix="traincontrol_selected_best_")
    zip_name = f"selected_best_weights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    zip_path = Path(tmp_dir) / zip_name
    manifest = []

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for item in selected:
            project = str(item["project"])
            weights_rel = str(item["weights_rel"])
            file_path = Path(item["file_path"])
            rel_parts = Path(weights_rel).parts
            run_folder = rel_parts[-3] if len(rel_parts) >= 3 and rel_parts[-2].lower() == "weights" else file_path.parent.name
            arcname = f"{safe_download_part(project)}/{safe_download_part(run_folder)}/best.pt"
            write_file_to_zip_preserve_times(zf, file_path, arcname)
            try:
                st = file_path.stat()
                manifest.append({
                    "project": project,
                    "weights_rel": weights_rel,
                    "path": arcname,
                    "modified_at": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    "modified_ts": st.st_mtime,
                    "size": st.st_size,
                })
            except Exception:
                pass

        zf.writestr(
            "traincontrol_selected_best_manifest.json",
            json.dumps({
                "items": manifest,
                "skipped": skipped,
            }, ensure_ascii=False, indent=2),
        )

    write_audit_log(
        "download_selected_outputs",
        "success",
        details=f"selected={len(selected)}, skipped={len(skipped)}",
    )

    @after_this_request
    def cleanup_selected_outputs(response):
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


def build_https_ssl_context():
    if bool(HTTPS_CERT_FILE) != bool(HTTPS_KEY_FILE):
        print("HTTPS configuration needs both TRAIN_CONTROL_HTTPS_CERT and TRAIN_CONTROL_HTTPS_KEY.")
        raise SystemExit(1)

    if HTTPS_CERT_FILE and HTTPS_KEY_FILE:
        cert_path = Path(HTTPS_CERT_FILE)
        key_path = Path(HTTPS_KEY_FILE)
        if cert_path.exists() and key_path.exists():
            return (str(cert_path), str(key_path))
        print("HTTPS cert/key not found:")
        print(f"  TRAIN_CONTROL_HTTPS_CERT={HTTPS_CERT_FILE}")
        print(f"  TRAIN_CONTROL_HTTPS_KEY={HTTPS_KEY_FILE}")
        raise SystemExit(1)

    if importlib.util.find_spec("cryptography") is None:
        print("HTTPS startup needs a certificate.")
        print("No TRAIN_CONTROL_HTTPS_CERT/TRAIN_CONTROL_HTTPS_KEY was provided,")
        print("and Flask adhoc certificates require the cryptography library.")
        print("")
        print("Fix option 1 - install dependency:")
        print("  python -m pip install cryptography")
        print("")
        print("Fix option 2 - provide an existing PEM cert/key:")
        print("  set TRAIN_CONTROL_HTTPS_CERT=D:\\path\\to\\cert.pem")
        print("  set TRAIN_CONTROL_HTTPS_KEY=D:\\path\\to\\key.pem")
        print("  python TrainControl.py")
        raise SystemExit(1)

    cert_dir = BASE_DIR / "certs"
    cert_path = cert_dir / "traincontrol-cert.pem"
    key_path = cert_dir / "traincontrol-key.pem"
    ca_cert_path = cert_dir / "traincontrol-ca.pem"
    ca_key_path = cert_dir / "traincontrol-ca-key.pem"
    ca_install_path = cert_dir / "traincontrol-ca.cer"
    if all(path.exists() for path in (cert_path, key_path, ca_cert_path, ca_key_path, ca_install_path)):
        return (str(cert_path), str(key_path))

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
    import ipaddress

    cert_dir.mkdir(parents=True, exist_ok=True)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    common_name = HTTPS_HOSTNAMES[0] if HTTPS_HOSTNAMES else "localhost"
    ca_subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "TrainControl Local CA"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "TrainControl"),
    ])
    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "TrainControl"),
    ])

    dns_names = {"localhost", socket.gethostname(), *HTTPS_HOSTNAMES}
    ip_addresses = {ipaddress.ip_address("127.0.0.1")}
    try:
        for item in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip_addresses.add(ipaddress.ip_address(item[4][0]))
    except OSError:
        pass

    now = datetime.utcnow()
    ca_certificate = (
        x509.CertificateBuilder()
        .subject_name(ca_subject)
        .issuer_name(ca_subject)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_certificate.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(name) for name in sorted(dns_names) if name]
                + [x509.IPAddress(address) for address in sorted(ip_addresses, key=str)]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    ca_key_path.write_bytes(ca_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    key_path.write_bytes(key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    ca_pem = ca_certificate.public_bytes(serialization.Encoding.PEM)
    ca_cert_path.write_bytes(ca_pem)
    ca_install_path.write_bytes(ca_certificate.public_bytes(serialization.Encoding.DER))
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM) + ca_pem)
    print(f"Generated persistent HTTPS certificate: {cert_path}")
    print(f"Install this CA once on client devices to remove browser warnings: {ca_install_path}")
    return (str(cert_path), str(key_path))


if __name__ == "__main__":
    initialize_app_runtime()
    if USE_HTTPS:
        ssl_context = build_https_ssl_context()
        print(f"Web monitor is running at: https://127.0.0.1:{HTTPS_PORT}")
        app.run(host=HOST, port=HTTPS_PORT, debug=False, threaded=True, ssl_context=ssl_context)
        raise SystemExit(0)

    print(f"Web monitor is running at: http://127.0.0.1:{PORT}")
    try:
        from waitress import serve
        print(
            "Using waitress "
            f"threads={WAITRESS_THREADS} "
            f"connection_limit={WAITRESS_CONNECTION_LIMIT} "
            f"channel_timeout={WAITRESS_CHANNEL_TIMEOUT}s "
            f"cleanup_interval={WAITRESS_CLEANUP_INTERVAL}s "
            f"max_request_body_size={WAITRESS_MAX_REQUEST_BODY_SIZE}"
        )
        serve(
            app,
            host=HOST,
            port=PORT,
            threads=WAITRESS_THREADS,
            connection_limit=WAITRESS_CONNECTION_LIMIT,
            channel_timeout=WAITRESS_CHANNEL_TIMEOUT,
            cleanup_interval=WAITRESS_CLEANUP_INTERVAL,
            max_request_body_size=WAITRESS_MAX_REQUEST_BODY_SIZE,
        )
    except Exception:
        app.run(host=HOST, port=PORT, debug=False, threaded=True)

