"""Builds and sends the newsletter email over SMTP, and keeps track of the
newsletters already sent."""

import logging
import smtplib
from datetime import date, datetime, timedelta
from email.message import EmailMessage

from airflow.sdk import BaseHook, Variable

from notifications import config

logger = logging.getLogger(__name__)


def build_message(
    subject: str,
    html: str,
    text: str,
    sender: str,
    recipients: list[str],
    message_id: str = None,
) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    if message_id:
        message["Message-ID"] = message_id
    message.set_content(text)
    message.add_alternative(html, subtype="html")
    return message


class SmtpMailer:
    def __init__(self, conn_id: str = "newsletter_smtp"):
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson
        self.host = conn.host
        self.port = conn.port or 587
        self.login = conn.login
        self.password = conn.password
        self.starttls = extra.get("starttls", True)
        self.sender = extra.get("from") or conn.login
        self.recipients = [
            r.strip() for r in extra.get("to", "").split(",") if r.strip()
        ]
        if not self.recipients:
            raise ValueError(f"Set the recipients in the 'to' extra of '{conn_id}'.")

    def send(self, message: EmailMessage) -> None:
        logger.info("Sending '%s' to %s", message["Subject"], message["To"])
        sent = False
        try:
            with smtplib.SMTP(self.host, self.port, timeout=60) as smtp:
                if self.starttls:
                    smtp.starttls()
                if self.login:
                    smtp.login(self.login, self.password or "")
                smtp.send_message(message)
                sent = True
        except smtplib.SMTPException:
            if not sent:
                raise
            # Failing here would retry the task and send the email again.
            logger.warning("Email sent, but the SMTP connection did not close cleanly.")
        logger.info("Email sent.")


def message_id(week_start: date, sender: str, resend_at: datetime = None) -> str:
    domain = sender.rpartition("@")[2]
    resend = f".resend-{resend_at:%Y%m%dT%H%M%S}" if resend_at else ""
    return f"<library-newsletter-{week_start.isoformat()}{resend}@{domain}>"


def load_history() -> dict:
    return Variable.get(config.SENT_HISTORY_VARIABLE, default={}, deserialize_json=True)


def save_history(history: dict) -> None:
    Variable.set(config.SENT_HISTORY_VARIABLE, history, serialize_json=True)


def add_to_history(
    history: dict,
    message_id: str,
    sent_at: datetime,
    keep_days: int = config.SENT_HISTORY_DAYS,
) -> dict:
    oldest = sent_at - timedelta(days=keep_days)
    kept = {
        sent_id: when
        for sent_id, when in history.items()
        if datetime.fromisoformat(when) >= oldest
    }
    kept[message_id] = sent_at.isoformat(timespec="seconds")
    return kept
