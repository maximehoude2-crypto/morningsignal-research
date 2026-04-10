"""
Weekly digest assembler + Resend sender.
Reads the last 5 trading days of state files and sends an HTML email.
"""

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
CONFIG_DIR = BASE_DIR / "config"


def load_week_data(n_days: int = 7) -> dict:
    """Load breakout and market brief data from the past n trading days."""
    days_found = []
    check_date = date.today()
    attempts = 0
    while len(days_found) < 5 and attempts < n_days + 5:
        d = check_date - timedelta(days=attempts)
        if d.weekday() < 5:  # weekday only
            bk = STATE_DIR / f"breakouts_{d.isoformat()}.json"
            mb = STATE_DIR / f"market_brief_{d.isoformat()}.json"
            if bk.exists() and mb.exists():
                days_found.append({
                    "date": d.isoformat(),
                    "breakouts": json.loads(bk.read_text()),
                    "brief": json.loads(mb.read_text()),
                })
        attempts += 1
    return days_found


def best_of_week(days_data: list) -> list:
    """Find top 5 stocks by average composite score across the week."""
    scores: dict[str, list] = {}
    info: dict[str, dict] = {}
    for day in days_data:
        for b in day["breakouts"]:
            t = b["ticker"]
            scores.setdefault(t, []).append(b["score"])
            info[t] = b  # keep latest metadata
    ranked = [
        {**info[t], "avg_score": round(sum(v) / len(v), 1), "days_appeared": len(v)}
        for t, v in scores.items()
    ]
    ranked.sort(key=lambda x: x["avg_score"], reverse=True)
    return ranked[:5]


def week_market_summary(days_data: list) -> dict:
    """Summarize the week's market performance."""
    if not days_data:
        return {}
    # Use first (most recent) and last (oldest) days
    latest = days_data[0]["brief"]
    oldest = days_data[-1]["brief"]

    def idx_chg(sym):
        try:
            return latest["indices"][sym]["day_change"]
        except (KeyError, TypeError):
            return 0

    return {
        "spy_week": sum(
            day["brief"]["indices"].get("SPY", {}).get("day_change", 0)
            for day in days_data
        ),
        "qqq_week": sum(
            day["brief"]["indices"].get("QQQ", {}).get("day_change", 0)
            for day in days_data
        ),
        "vix_end": latest["macro"]["vix"]["level"],
        "days": len(days_data),
        "total_breakouts_seen": sum(len(d["breakouts"]) for d in days_data),
    }


def build_html_email(week_summary: dict, top5: list, days_data: list) -> str:
    today = date.today().strftime("%B %d, %Y")
    week_start = days_data[-1]["date"] if days_data else "N/A"
    week_end = days_data[0]["date"] if days_data else "N/A"

    # Top 5 rows
    top5_rows = ""
    for i, b in enumerate(top5, 1):
        score_color = "#00c48c" if b["avg_score"] >= 80 else ("#f5a623" if b["avg_score"] >= 60 else "#8a9bbf")
        top5_rows += f"""
        <tr>
          <td style="padding:10px 12px;color:#8a9bbf;font-size:13px">{i}</td>
          <td style="padding:10px 12px;font-weight:700;font-family:monospace;color:#3b82f6">{b['ticker']}</td>
          <td style="padding:10px 12px;color:#8a9bbf;font-size:13px">{b.get('name', b['ticker'])}</td>
          <td style="padding:10px 12px;color:#8a9bbf;font-size:13px">{b.get('sector','')}</td>
          <td style="padding:10px 12px;font-weight:700;font-family:monospace;color:{score_color}">{b['avg_score']}</td>
          <td style="padding:10px 12px;color:#8a9bbf;font-size:13px;text-align:center">{b['days_appeared']}d</td>
          <td style="padding:10px 12px;font-family:monospace;color:#e8edf5">${b['price']:.2f}</td>
        </tr>
        """

    spy_color = "#00c48c" if week_summary.get("spy_week", 0) >= 0 else "#ff4d5a"
    qqq_color = "#00c48c" if week_summary.get("qqq_week", 0) >= 0 else "#ff4d5a"

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0a1628;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">

<div style="max-width:640px;margin:0 auto;padding:24px 16px">

  <!-- Header -->
  <div style="background:#0f2040;border:1px solid #1e3357;border-radius:12px;padding:24px;margin-bottom:20px">
    <div style="font-size:22px;font-weight:800;color:#ffffff;margin-bottom:4px">
      Morning<span style="color:#00c48c">Signal</span> Weekly Digest
    </div>
    <div style="color:#8a9bbf;font-size:14px">{week_start} – {week_end}</div>
  </div>

  <!-- Week Summary -->
  <div style="background:#111e36;border:1px solid #1e3357;border-radius:10px;padding:20px;margin-bottom:20px">
    <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.1em;color:#8a9bbf;margin-bottom:14px">
      Week in Markets
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px">
      <div>
        <div style="color:#8a9bbf;font-size:11px;margin-bottom:4px">S&amp;P 500 (week)</div>
        <div style="font-size:20px;font-weight:700;font-family:monospace;color:{spy_color}">
          {week_summary.get('spy_week', 0):+.2f}%
        </div>
      </div>
      <div>
        <div style="color:#8a9bbf;font-size:11px;margin-bottom:4px">Nasdaq 100 (week)</div>
        <div style="font-size:20px;font-weight:700;font-family:monospace;color:{qqq_color}">
          {week_summary.get('qqq_week', 0):+.2f}%
        </div>
      </div>
      <div>
        <div style="color:#8a9bbf;font-size:11px;margin-bottom:4px">VIX Close</div>
        <div style="font-size:20px;font-weight:700;font-family:monospace;color:#e8edf5">
          {week_summary.get('vix_end', 0):.2f}
        </div>
      </div>
    </div>
    <div style="margin-top:14px;font-size:13px;color:#8a9bbf">
      {week_summary.get('total_breakouts_seen', 0)} total breakout setups identified across {week_summary.get('days', 0)} trading sessions.
    </div>
  </div>

  <!-- Top 5 of the Week -->
  <div style="background:#111e36;border:1px solid #1e3357;border-radius:10px;margin-bottom:20px;overflow:hidden">
    <div style="padding:16px 20px;border-bottom:1px solid #1e3357">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.1em;color:#8a9bbf">
        Top 5 Breakout Setups — Week of {week_end}
      </div>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="background:#162b50">
          <th style="padding:8px 12px;text-align:left;font-size:10px;color:#8a9bbf;text-transform:uppercase;letter-spacing:0.07em">#</th>
          <th style="padding:8px 12px;text-align:left;font-size:10px;color:#8a9bbf;text-transform:uppercase;letter-spacing:0.07em">Ticker</th>
          <th style="padding:8px 12px;text-align:left;font-size:10px;color:#8a9bbf;text-transform:uppercase;letter-spacing:0.07em">Name</th>
          <th style="padding:8px 12px;text-align:left;font-size:10px;color:#8a9bbf;text-transform:uppercase;letter-spacing:0.07em">Sector</th>
          <th style="padding:8px 12px;text-align:left;font-size:10px;color:#8a9bbf;text-transform:uppercase;letter-spacing:0.07em">Avg Score</th>
          <th style="padding:8px 12px;text-align:center;font-size:10px;color:#8a9bbf;text-transform:uppercase;letter-spacing:0.07em">Days</th>
          <th style="padding:8px 12px;text-align:left;font-size:10px;color:#8a9bbf;text-transform:uppercase;letter-spacing:0.07em">Price</th>
        </tr>
      </thead>
      <tbody>
        {top5_rows if top5_rows else '<tr><td colspan="7" style="padding:20px;text-align:center;color:#8a9bbf">No data available for this week.</td></tr>'}
      </tbody>
    </table>
  </div>

  <!-- What to Watch -->
  <div style="background:#111e36;border:1px solid #1e3357;border-radius:10px;padding:20px;margin-bottom:20px">
    <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.1em;color:#8a9bbf;margin-bottom:12px">
      What to Watch Next Week
    </div>
    <ul style="color:#e8edf5;font-size:14px;line-height:1.8;padding-left:20px">
      <li>Monitor this week's top breakout setups for follow-through on volume</li>
      <li>Any stocks with 3+ consecutive days in the scan are auto-flagged for deep dive</li>
      <li>Watch VIX: levels above 20 suggest defensive positioning</li>
      <li>Check earnings calendar — avoid holding through reports unless intentional</li>
    </ul>
  </div>

  <!-- Footer -->
  <div style="text-align:center;color:#8a9bbf;font-size:12px;padding:16px">
    MorningSignal Research · {today}<br>
    Not financial advice. For research purposes only.<br>
    <a href="https://research.morningsignal.xyz" style="color:#3b82f6">research.morningsignal.xyz</a>
  </div>

</div>
</body>
</html>"""


def send_digest(html: str, subject: str, recipients: list[str], dry_run: bool = False) -> bool:
    if dry_run:
        preview_path = BASE_DIR / "state" / "weekly_digest_preview.html"
        preview_path.write_text(html)
        print(f"[DRY RUN] Email preview saved → {preview_path}")
        print(f"  Would send to: {recipients}")
        return True

    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key:
        print("ERROR: RESEND_API_KEY not set in environment")
        return False

    try:
        import resend
        resend.api_key = api_key
        resp = resend.Emails.send({
            "from": "MorningSignal Research <research@morningsignal.xyz>",
            "to": recipients,
            "subject": subject,
            "html": html,
        })
        print(f"Email sent: {resp}")
        return True
    except ImportError:
        print("ERROR: resend package not installed. Run: pip install resend")
        return False
    except Exception as e:
        print(f"ERROR sending email: {e}")
        return False


def run_weekly_digest(dry_run: bool = False) -> bool:
    print("Building weekly digest...")
    days_data = load_week_data()
    if not days_data:
        print("No state data found for this week.")
        return False

    week_summary = week_market_summary(days_data)
    top5 = best_of_week(days_data)
    html = build_html_email(week_summary, top5, days_data)

    # Load recipients
    advisors_path = CONFIG_DIR / "advisors.json"
    recipients = []
    if advisors_path.exists():
        recipients = json.loads(advisors_path.read_text())
    if not recipients:
        print("WARNING: No recipients in config/advisors.json")
        recipients = []

    week_str = days_data[-1]["date"] if days_data else date.today().isoformat()
    subject = f"MorningSignal Weekly Digest — Week of {week_str}"

    return send_digest(html, subject, recipients, dry_run=dry_run)


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    ok = run_weekly_digest(dry_run=dry)
    sys.exit(0 if ok else 1)
