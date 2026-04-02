"""
通知モジュール（Discord Webhook / LINE Notify）
.env に設定してください:
    DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
    LINE_NOTIFY_TOKEN=xxxxx
"""

import os
import requests
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
LINE_NOTIFY_TOKEN = os.getenv("LINE_NOTIFY_TOKEN")


def send_discord(message: str):
    if not DISCORD_WEBHOOK_URL:
        logger.debug("DISCORD_WEBHOOK_URL not set, skipping")
        return
    try:
        resp = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("Discord notification sent")
    except Exception as e:
        logger.error(f"Discord notification failed: {e}")


def send_line(message: str):
    if not LINE_NOTIFY_TOKEN:
        logger.debug("LINE_NOTIFY_TOKEN not set, skipping")
        return
    try:
        resp = requests.post(
            "https://notify-api.line.me/api/notify",
            headers={"Authorization": f"Bearer {LINE_NOTIFY_TOKEN}"},
            data={"message": message},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("LINE notification sent")
    except Exception as e:
        logger.error(f"LINE notification failed: {e}")


def notify(message: str):
    """Discord と LINE 両方に通知（設定されているものだけ）"""
    send_discord(message)
    send_line(message)


def notify_daily_summary(total_bet: int, total_payout: int, hit_count: int, bet_count: int):
    recovery = total_payout / total_bet * 100 if total_bet > 0 else 0
    msg = (
        f"【本日の収益サマリー】\n"
        f"投資: ¥{total_bet:,}\n"
        f"払戻: ¥{total_payout:,}\n"
        f"収支: ¥{total_payout - total_bet:+,}\n"
        f"回収率: {recovery:.1f}%\n"
        f"的中: {hit_count}/{bet_count}回"
    )
    notify(msg)


def notify_high_ev_race(stadium_name: str, race_number: int, combinations: list[str], evs: list[float]):
    combo_str = "\n".join(f"  {c}: EV={e:.2f}" for c, e in zip(combinations, evs))
    msg = (
        f"【高期待値レース検出】\n"
        f"{stadium_name} R{race_number}\n"
        f"{combo_str}"
    )
    notify(msg)
