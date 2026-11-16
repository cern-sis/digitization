"""Builds and sends the newsletter email over SMTP."""

import logging
import smtplib
from email.message import EmailMessage

from airflow.sdk import BaseHook

logger = logging.getLogger(__name__)


def build_message(
    subject: str, html: str, text: str, sender: str, recipients: list[str]
) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
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
        logger.info(f"Sending '{message['Subject']}' to {message['To']}")
        with smtplib.SMTP(self.host, self.port, timeout=60) as smtp:
            if self.starttls:
                smtp.starttls()
            if self.login:
                smtp.login(self.login, self.password or "")
            smtp.send_message(message)
        logger.info("Email sent.")
