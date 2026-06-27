import queue
import threading


state_lock = threading.Lock()
state_cond = threading.Condition(state_lock)
train_queue = queue.Queue()

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
