import json
import urllib.request
from datetime import datetime
from .logging_setup import get_logger
from .config import Config

logger = get_logger()

class DiscordNotifier:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_notification(self, version, kr_version):
        if not self.webhook_url:
            logger.warning("Discord Webhook URL is not configured. Notification skipped.")
            return


        description = "```yaml\n"
        description += f"업데이트 버전 : {version}\n"
        description += f"게임 버전 : {kr_version}\n"
        description += "```"

        embed = {
            "title": "데이터 업데이트 완료",
            "description": description,
            "color": 0x1F8B4C
        }

        payload = {
            "embeds": [embed]
        }

        try:
            req = urllib.request.Request(
                self.webhook_url,
                data=json.dumps(payload).encode('utf-8'),
                headers={'Content-Type': 'application/json', 'User-Agent': 'FFXIV-Extractor-Notifier'}
            )
            with urllib.request.urlopen(req) as response:
                if 200 <= response.status < 300:
                    logger.info("Discord notification sent successfully.")
                else:
                    logger.error(f"Failed to send Discord notification: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")
