import logging
import time

import requests

logger = logging.getLogger(__name__)


class JulesHarnessBackend:
    def __init__(self, bot_token: str, chat_id: str, jules_api_key: str, repo_source: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.jules_api_key = jules_api_key
        self.repo_source = repo_source
        self.jules_api_base = "https://jules.googleapis.com/v1alpha"
        self.session_id = None
        self.last_update_id = 0
        self.processed_activities = set()

    def send_telegram_message(self, text: str):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text}
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to send telegram message: {e}")

    def poll_telegram_messages(self):
        url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        params = {"offset": self.last_update_id + 1, "timeout": 10}
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            if data.get("ok"):
                messages = []
                for update in data.get("result", []):
                    self.last_update_id = update["update_id"]
                    message = update.get("message")
                    if message and str(message.get("chat", {}).get("id")) == self.chat_id:
                        text = message.get("text")
                        if text:
                            messages.append(text)
                return messages
        except requests.RequestException as e:
            logger.error(f"Failed to poll telegram messages: {e}")
        return []

    def create_session(self, prompt: str):
        url = f"{self.jules_api_base}/sessions"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.jules_api_key,
        }
        payload = {
            "prompt": prompt,
            "sourceContext": {
                "source": self.repo_source,
                "githubRepoContext": {"startingBranch": "main"},
            },
        }
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            self.session_id = data.get("id")
            if not self.session_id:
                name = data.get("name", "")
                if "/" in name:
                    self.session_id = name.split("/")[-1]
            return self.session_id
        except requests.RequestException as e:
            logger.error(f"Failed to create jules session: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return None

    def list_activities(self):
        if not self.session_id:
            return []
        url = f"{self.jules_api_base}/sessions/{self.session_id}/activities"
        headers = {"X-Goog-Api-Key": self.jules_api_key}
        params = {"pageSize": 100}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("activities", [])
        except requests.RequestException as e:
            logger.error(f"Failed to list jules activities: {e}")
            return []

    def send_message_to_jules(self, text: str):
        if not self.session_id:
            return False
        url = f"{self.jules_api_base}/sessions/{self.session_id}:sendMessage"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.jules_api_key,
        }
        payload = {"prompt": text}
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to send message to jules: {e}")
            return False

    def format_activity(self, activity: dict) -> str:
        originator = activity.get("originator", "unknown").capitalize()

        if "planGenerated" in activity:
            steps = activity["planGenerated"].get("plan", {}).get("steps", [])
            plan_text = "\n".join([f"- {s.get('title', '')}" for s in steps])
            return f"[{originator}] Generated Plan:\n{plan_text}"

        if "planApproved" in activity:
            return f"[{originator}] Plan Approved"

        if "progressUpdated" in activity:
            progress = activity["progressUpdated"]
            title = progress.get("title", "")
            desc = progress.get("description", "")
            if desc:
                return f"[{originator}] Progress: {title}\n{desc}"
            return f"[{originator}] Progress: {title}"

        if "sessionCompleted" in activity:
            return f"[{originator}] Session Completed"

        return f"[{originator}] Activity {activity.get('id', 'unknown')}"

    def poll_jules_activities(self):
        activities = self.list_activities()
        # Sort activities by createTime to process them in order
        try:
            activities.sort(key=lambda x: x.get("createTime", ""))
        except Exception:
            pass

        for activity in activities:
            activity_id = activity.get("id")
            if activity_id and activity_id not in self.processed_activities:
                self.processed_activities.add(activity_id)
                formatted_text = self.format_activity(activity)
                self.send_telegram_message(formatted_text)

    def run(self):
        self.send_telegram_message("Harness started. Creating Jules session...")
        session_id = self.create_session("Start working on the repo.")
        if not session_id:
            self.send_telegram_message("Failed to create Jules session. Exiting.")
            return

        self.send_telegram_message(f"Jules session created: {session_id}")

        while True:
            try:
                # 1. Check for Telegram messages and send to Jules
                messages = self.poll_telegram_messages()
                for msg in messages:
                    self.send_message_to_jules(msg)
                    self.send_telegram_message(f"Sent to Jules: {msg}")

                # 2. Check for Jules activities and send to Telegram
                self.poll_jules_activities()

                time.sleep(5)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error in run loop: {e}")
                time.sleep(10)
