import base64
import hashlib
import hmac
import json
import re
import os
from datetime import datetime
from pathlib import Path


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def user_file_path(base_dir: Path, user_file_name: str) -> Path:
    return base_dir / user_file_name


def train_history_file_path(base_dir: Path, history_file_name: str) -> Path:
    return base_dir / history_file_name


def audit_log_file_path(base_dir: Path, audit_file_name: str) -> Path:
    return base_dir / audit_file_name


def append_audit_log(base_dir: Path, audit_file_name: str, entry: dict):
    try:
        p = audit_log_file_path(base_dir, audit_file_name)
        line = json.dumps(entry or {}, ensure_ascii=False)
        with p.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def save_default_user_file_if_missing(base_dir: Path, user_file_name: str, default_user_credentials: dict):
    p = user_file_path(base_dir, user_file_name)
    if p.exists():
        return
    try:
        p.write_text(json.dumps(default_user_credentials, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def append_train_history_file(base_dir: Path, history_file_name: str, entry: dict):
    try:
        p = train_history_file_path(base_dir, history_file_name)
        line = json.dumps(entry or {}, ensure_ascii=False)
        with p.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_train_history_file(base_dir: Path, history_file_name: str, max_items: int = 500):
    p = train_history_file_path(base_dir, history_file_name)
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


def parse_user_file_text(text: str):
    raw = str(text or "").strip()
    if not raw:
        return {}

    if raw.startswith("module.exports"):
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if m:
            raw = m.group(0)

    return json.loads(raw)


def load_user_credentials(base_dir: Path, user_file_name: str, default_user_credentials: dict):
    save_default_user_file_if_missing(base_dir, user_file_name, default_user_credentials)

    try:
        raw = user_file_path(base_dir, user_file_name).read_text(encoding="utf-8")
        data = parse_user_file_text(raw)
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

    return dict(default_user_credentials)


def make_password_record(password: str, iterations: int = 200000):
    salt = os.urandom(16)
    got = hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), salt, int(iterations))
    return {
        "algorithm": "pbkdf2_sha256",
        "iterations": int(iterations),
        "salt": base64.b64encode(salt).decode("utf-8"),
        "hash": base64.b64encode(got).decode("utf-8"),
    }


def _normalize_user_accounts(data: dict, default_user_credentials: dict):
    rows = []
    if isinstance(data, dict) and isinstance(data.get("users"), list):
        source = data.get("users") or []
    elif isinstance(data, dict):
        source = [data]
    else:
        source = [default_user_credentials]

    seen = set()
    for item in source:
        if not isinstance(item, dict):
            continue
        username = str(item.get("username", "") or "").strip()
        pwd = item.get("password", {})
        if not username or username.lower() in seen or not isinstance(pwd, dict):
            continue
        algo = str(pwd.get("algorithm", "")).strip().lower()
        try:
            iterations = int(pwd.get("iterations", 0))
        except Exception:
            iterations = 0
        salt = str(pwd.get("salt", "") or "").strip()
        hash_b64 = str(pwd.get("hash", "") or "").strip()
        if algo != "pbkdf2_sha256" or iterations <= 0 or not salt or not hash_b64:
            continue
        seen.add(username.lower())
        rows.append({
            "username": username,
            "password": {
                "algorithm": algo,
                "iterations": iterations,
                "salt": salt,
                "hash": hash_b64,
            },
            "created_at": str(item.get("created_at", "") or "").strip(),
        })

    if not rows:
        rows = _normalize_user_accounts(dict(default_user_credentials), default_user_credentials)
    return rows


def load_user_accounts(base_dir: Path, user_file_name: str, default_user_credentials: dict):
    save_default_user_file_if_missing(base_dir, user_file_name, default_user_credentials)
    try:
        raw = user_file_path(base_dir, user_file_name).read_text(encoding="utf-8")
        data = parse_user_file_text(raw)
    except Exception:
        data = dict(default_user_credentials)
    return _normalize_user_accounts(data, default_user_credentials)


def save_user_accounts(base_dir: Path, user_file_name: str, accounts: list):
    rows = []
    for item in accounts or []:
        if not isinstance(item, dict):
            continue
        username = str(item.get("username", "") or "").strip()
        pwd = item.get("password", {})
        if username and isinstance(pwd, dict):
            rows.append({
                "username": username,
                "password": pwd,
                "created_at": str(item.get("created_at", "") or "").strip(),
            })
    payload = {"users": rows}
    user_file_path(base_dir, user_file_name).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def authenticate_user(base_dir: Path, user_file_name: str, default_user_credentials: dict, username: str, password: str):
    needle = str(username or "").strip()
    for account in load_user_accounts(base_dir, user_file_name, default_user_credentials):
        if hmac.compare_digest(str(account.get("username", "") or ""), needle) and verify_password(password, account):
            return str(account.get("username", "") or "").strip()
    return ""


def create_user_account(base_dir: Path, user_file_name: str, default_user_credentials: dict, username: str, password: str):
    username = str(username or "").strip()
    password = str(password or "")
    if not username:
        return False, "missing_username", None
    if len(username) > 64 or not re.match(r"^[A-Za-z0-9_.@-]+$", username):
        return False, "invalid_username", None
    if len(password) < 4:
        return False, "password_too_short", None

    accounts = load_user_accounts(base_dir, user_file_name, default_user_credentials)
    if any(str(x.get("username", "")).strip().lower() == username.lower() for x in accounts):
        return False, "user_exists", None
    account = {
        "username": username,
        "password": make_password_record(password),
        "created_at": now_str(),
    }
    accounts.append(account)
    save_user_accounts(base_dir, user_file_name, accounts)
    return True, "created", {"username": username}


def delete_user_account(base_dir: Path, user_file_name: str, default_user_credentials: dict, username: str):
    username = str(username or "").strip()
    accounts = load_user_accounts(base_dir, user_file_name, default_user_credentials)
    if len(accounts) <= 1:
        return False, "last_user", None
    kept = [x for x in accounts if str(x.get("username", "")).strip().lower() != username.lower()]
    if len(kept) == len(accounts):
        return False, "not_found", None
    save_user_accounts(base_dir, user_file_name, kept)
    return True, "deleted", {"username": username}


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
