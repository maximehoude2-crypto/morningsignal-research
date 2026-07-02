"""
Failure alerting via Resend. No-ops (with a console note) when
RESEND_API_KEY or ALERT_EMAIL is not configured, so the pipeline never
fails because alerting is unavailable.
"""

import os

from dotenv import load_dotenv

load_dotenv()


def send_failure_alert(subject: str, body: str) -> bool:
    """Email a pipeline failure notice. Returns True if a send was attempted
    and accepted by Resend."""
    api_key = os.getenv("RESEND_API_KEY")
    to_addr = os.getenv("ALERT_EMAIL") or os.getenv("NEWSLETTER_FROM")
    if not api_key or not to_addr:
        print(f"  [alerts] Not configured (RESEND_API_KEY/ALERT_EMAIL); skipped alert: {subject}")
        return False
    try:
        import resend

        resend.api_key = api_key
        resend.Emails.send(
            {
                "from": os.getenv("NEWSLETTER_FROM", to_addr),
                "to": [to_addr],
                "subject": f"[MorningSignal] {subject}",
                "text": body,
            }
        )
        print(f"  [alerts] Sent failure alert: {subject}")
        return True
    except Exception as exc:
        print(f"  [alerts] Failed to send alert ({subject}): {exc}")
        return False
