"""
Sends email via Gmail SMTP. Single reusable function for the whole project.
"""
import smtplib
import sys
import os
from email.mime.text import MIMEText

sys.path.insert(0, os.path.dirname(__file__))

from config import GMAIL_APP_PASSWORD, GMAIL_SENDER, ALERT_EMAIL

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


def send_email(subject: str, body: str, to: str = ALERT_EMAIL) -> bool:
    """
    Send a plain-text email via Gmail SMTP.
    Returns True on success, False on failure (logs error, never raises).
    """
    if not GMAIL_APP_PASSWORD:
        print("ERROR: GMAIL_APP_PASSWORD not set — cannot send email")
        return False

    if not to:
        print("ERROR: recipient email is empty — cannot send email")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = GMAIL_SENDER
    msg["To"] = to

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_SENDER, [to], msg.as_string())
        print(f"Email sent: '{subject}' → {to}")
        return True
    except Exception as e:
        print(f"Email failed: {type(e).__name__}: {e}")
        return False
