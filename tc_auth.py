import time
import secrets

from flask import jsonify, request, session

from tc_config import AUTH_SESSION_TTL_SECONDS, LV2_SESSION_TTL_SECONDS, LV3_SESSION_TTL_SECONDS


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
    session["auth_session_id"] = secrets.token_urlsafe(16)


def current_auth_user() -> str:
    return str(session.get("auth_user", "") or "").strip() or "anonymous"


def current_session_id() -> str:
    sid = str(session.get("auth_session_id", "") or "").strip()
    if not sid:
        sid = secrets.token_urlsafe(16)
        session["auth_session_id"] = sid
    return sid


def is_lv2_authenticated():
    try:
        exp = int(session.get("auth_lv2_exp", 0))
    except Exception:
        return False
    return exp > int(time.time())


def mark_lv2_authenticated():
    session["auth_lv2"] = True
    session["auth_lv2_exp"] = int(time.time()) + LV2_SESSION_TTL_SECONDS


def is_lv3_authenticated():
    try:
        exp = int(session.get("auth_lv3_exp", 0))
    except Exception:
        return False
    return exp > int(time.time())


def mark_lv3_authenticated():
    session["auth_lv3"] = True
    session["auth_lv3_exp"] = int(time.time()) + LV3_SESSION_TTL_SECONDS


def clear_auth():
    session.pop("auth", None)
    session.pop("auth_user", None)
    session.pop("auth_exp", None)
    session.pop("auth_lv2", None)
    session.pop("auth_lv2_exp", None)
    session.pop("auth_lv3", None)
    session.pop("auth_lv3_exp", None)
    session.pop("auth_session_id", None)


def require_lv2_json():
    if is_lv2_authenticated():
        return None
    return jsonify({"ok": False, "message": "LV2 password required"}), 403


def require_lv3_json():
    if is_lv3_authenticated():
        return None
    return jsonify({"ok": False, "message": "LV3 password required"}), 403
