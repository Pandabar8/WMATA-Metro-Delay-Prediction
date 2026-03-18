"""
Sends email via SendGrid. Single reusable function for the whole project.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from config import SENDGRID_API_KEY, ALERT_EMAIL


def send_email(subject: str, body: str, to: str = ALERT_EMAIL) -> bool:
    """
    Send a plain-text email via SendGrid.
    Returns True on success, False on failure (logs error, never raises).
    """
    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not set — cannot send email")
        return False

    message = Mail(
        from_email=ALERT_EMAIL,
        to_emails=to,
        subject=subject,
        plain_text_content=body,
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"Email sent: '{subject}' → {to} (status {response.status_code})")
        return True
    except Exception as e:
        print(f"Email failed: {e}")
        return False
