import os
from pathlib import Path


def int_env(name: str, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


ROOT_DIR = Path(r"D:\Object Detection\admin")
TRAIN_FILE = "Train_model_AI.py"
PORT = 820
HOST = "0.0.0.0"
CONTINUE_IF_ERROR = True
WAITRESS_THREADS = int_env("TRAIN_CONTROL_WAITRESS_THREADS", 48, minimum=8, maximum=256)
WAITRESS_CONNECTION_LIMIT = int_env("TRAIN_CONTROL_WAITRESS_CONNECTION_LIMIT", 1000, minimum=32, maximum=10000)
WAITRESS_CHANNEL_TIMEOUT = int_env("TRAIN_CONTROL_WAITRESS_CHANNEL_TIMEOUT", 120, minimum=30, maximum=600)
WAITRESS_CLEANUP_INTERVAL = int_env("TRAIN_CONTROL_WAITRESS_CLEANUP_INTERVAL", 30, minimum=5, maximum=300)
WAITRESS_MAX_REQUEST_BODY_SIZE = int_env("TRAIN_CONTROL_WAITRESS_MAX_REQUEST_BODY_SIZE", 1073741824, minimum=1048576)
BACKUP_ROOT = Path(os.getenv("TRAIN_CONTROL_BACKUP_ROOT", r"F:\Object Detection\admin"))
BACKUP_COPY_CHUNK_SIZE = int_env("TRAIN_CONTROL_BACKUP_CHUNK_SIZE", 1024 * 1024, minimum=64 * 1024, maximum=8 * 1024 * 1024)

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
OPEN_TRAIN_IN_NEW_TERMINAL = True

DEFAULT_USER_CREDENTIALS = {
    "username": "admin",
    "password": {
        "algorithm": "pbkdf2_sha256",
        "iterations": 200000,
        "salt": "NrTlFkGs4SYsNQ6kxWuZyg==",
        "hash": "r5H5ClA1hq9LzlKTK5xqXmPcZ+5eKZNthomUmbmWZ9k=",
    }
}
