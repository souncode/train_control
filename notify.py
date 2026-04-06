import requests
import time


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = str(chat_id)
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.last_update_id = None

    # =============================
    # SEND MESSAGE
    # =============================
    def send_message(self, text: str):
        url = f"{self.base_url}/sendMessage"
        data = {
            "chat_id": self.chat_id,
            "text": text
        }

        try:
            r = requests.post(url, data=data, timeout=10)
            print("Telegram:", r.text)
        except Exception as e:
            print("Send error:", e)

    # =============================
    # CLEAR OLD UPDATES (QUAN TRỌNG)
    # =============================
    def _clear_old_updates(self):
        url = f"{self.base_url}/getUpdates"
        try:
            r = requests.get(url, timeout=10).json()
            if r.get("result"):
                self.last_update_id = r["result"][-1]["update_id"]
        except:
            pass

    # =============================
    # CHECK USER REPLY OK
    # =============================
    def _check_for_ok(self):
        url = f"{self.base_url}/getUpdates"

        params = {}
        if self.last_update_id is not None:
            params["offset"] = self.last_update_id + 1

        try:
            r = requests.get(url, params=params, timeout=10).json()
        except:
            return False

        if not r.get("ok"):
            return False

        for update in r.get("result", []):
            self.last_update_id = update["update_id"]

            message = update.get("message", {})
            chat = message.get("chat", {})
            text = message.get("text", "")

            if str(chat.get("id")) == self.chat_id:
                if text.strip().upper() == "OK":
                    return True

        return False

    # =============================
    # ALARM UNTIL OK
    # =============================
    def alarm_until_ok(self, run_name=None, best_map=None, save_dir=None):
        message = f"""
🚨🚨 YOLO TRAINING COMPLETED 🚨🚨

Run: {run_name}
Best mAP50-95: {best_map}
Save dir: {save_dir}

Reply OK to stop alarm.
"""

        # XÓA update cũ để tránh auto stop
        self._clear_old_updates()

        print("Alarm started... waiting for OK")

        while True:
            self.send_message(message)

            # spam mỗi 3 giây
            for _ in range(3):
                time.sleep(1)
                if self._check_for_ok():
                    self.send_message("✅ Alarm stopped.")
                    print("Alarm stopped by user.")
                    return