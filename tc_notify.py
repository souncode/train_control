import threading

from notify import TelegramNotifier

from tc_config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from tc_runtime import NOTIFY_STATE


def build_notifier():
    token = str(TELEGRAM_BOT_TOKEN or "").strip()
    chat_id = str(TELEGRAM_CHAT_ID or "").strip()
    if not token or not chat_id:
        return None
    return TelegramNotifier(token=token, chat_id=chat_id)


def is_notify_enabled():
    return bool(NOTIFY_STATE.get("enabled", False)) and build_notifier() is not None


def send_telegram_notification_async(text: str):
    if not is_notify_enabled():
        return False

    notifier = build_notifier()
    if notifier is None:
        return False

    def _sender():
        try:
            notifier.send_message(str(text or "").strip())
        except Exception:
            pass

    threading.Thread(target=_sender, daemon=True).start()
    return True


def notify_train_finished(project_name: str, status: str, returncode=None):
    if not is_notify_enabled():
        return False

    lines = [
        "AI Train Monitor",
        f"Project: {project_name}",
        f"Status: {status}",
    ]
    if returncode is not None:
        lines.append(f"Return code: {returncode}")
    return send_telegram_notification_async("\n".join(lines))
