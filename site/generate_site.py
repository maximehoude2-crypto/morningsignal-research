"""
Site generator — reads state/ JSON files and assembles static HTML into docs/.
"""

import json
import sys
from datetime import date, datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
TEMPLATES_DIR = Path(__file__).parent / "templates"
DOCS_DIR = BASE_DIR / "docs"
ASSETS_SRC = Path(__file__).parent / "assets"

# Ensure output directories exist
(DOCS_DIR / "daily").mkdir(parents=True, exist_ok=True)
(DOCS_DIR / "deep-dives").mkdir(parents=True, exist_ok=True)

# Copy assets into docs/assets/
import shutil
assets_dst = DOCS_DIR / "assets"
if ASSETS_SRC.exists():
    shutil.copytree(ASSETS_SRC, assets_dst, dirs_exist_ok=True)


def load_json(path: Path) -> dict | list | None:
    if path.exists():
        return json.loads(path.read_text())
    return None


def get_today_data(today: str) -> tuple[dict | None, list | None]:
    brief = load_json(STATE_DIR / f"market_brief_{today}.json")
    breakouts = load_json(STATE_DIR / f"breakouts_{today}.json")
    return brief, breakouts


def find_latest_date() -> str | None:
    """Find the most recent date that has both a brief and breakouts file."""
    files = sorted(STATE_DIR.glob("breakouts_*.json"), reverse=True)
    for f in files:
        d = f.stem.replace("breakouts_", "")
        if (STATE_DIR / f"market_brief_{d}.json").exists():
            return d
    return None


def build_archive() -> list[dict]:
    """Build archive list from all past daily reports."""
    entries = []
    for bf in sorted(STATE_DIR.glob("market_brief_*.json"), reverse=True):
        d = bf.stem.replace("market_brief_", "")
        bk = STATE_DIR / f"breakouts_{d}.json"
        if bk.exists():
            breakouts = json.loads(bk.read_text())
            entries.append({
                "date": d,
                "breakout_count": len(breakouts),
                "top_ticker": breakouts[0]["ticker"] if breakouts else "—",
                "top_score": breakouts[0]["score"] if breakouts else 0,
            })
    return entries


def get_deep_dive_history(ticker: str) -> list[dict]:
    """Get all dates + scores where this ticker appeared in the top 15."""
    history = []
    for bf in sorted(STATE_DIR.glob("breakouts_*.json"), reverse=True):
        d = bf.stem.replace("breakouts_", "")
        breakouts = json.loads(bf.read_text())
        for b in breakouts:
            if b["ticker"] == ticker:
                history.append({"date": d, "rank": b["rank"], "score": b["score"]})
                break
    return history


def render(env: Environment, template_name: str, output_path: Path, **ctx):
    tmpl = env.get_template(template_name)
    html = tmpl.render(**ctx)
    output_path.write_text(html, encoding="utf-8")
    print(f"  Generated → {output_path.relative_to(BASE_DIR)}")


def generate_site(today: str | None = None) -> bool:
    today = today or date.today().isoformat()
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html"]),
    )

    # Load today's data; fall back to latest available
    brief, breakouts = get_today_data(today)
    if brief is None or breakouts is None:
        latest = find_latest_date()
        if latest:
            print(f"  No data for {today}, using latest available: {latest}")
            brief, breakouts = get_today_data(latest)
            today = latest
        else:
            print("  ERROR: No state data found. Run the scanner first.")
            return False

    archive = build_archive()

    # ── 1. Index page ──────────────────────────────────────────────────────
    render(env, "index.html.j2", DOCS_DIR / "index.html",
           today=today,
           generated_at=generated_at,
           brief=brief,
           breakouts=breakouts)

    # ── 2. Daily report ────────────────────────────────────────────────────
    render(env, "daily.html.j2", DOCS_DIR / "daily" / f"{today}.html",
           report_date=today,
           generated_at=generated_at,
           brief=brief,
           breakouts=breakouts)

    # ── 3. Archive page ────────────────────────────────────────────────────
    _generate_archive(env, DOCS_DIR / "archive.html", archive, generated_at)

    # ── 4. Deep dive pages ─────────────────────────────────────────────────
    queue = load_json(STATE_DIR / "deep_dive_queue.json") or []
    for entry in queue:
        ticker = entry["ticker"]
        # Find latest breakout data for this ticker
        ticker_data = next((b for b in breakouts if b["ticker"] == ticker), None)
        if ticker_data is None:
            # Try to find in any breakouts file
            for bf in sorted(STATE_DIR.glob("breakouts_*.json"), reverse=True):
                all_bk = json.loads(bf.read_text())
                ticker_data = next((b for b in all_bk if b["ticker"] == ticker), None)
                if ticker_data:
                    break
        if ticker_data is None:
            continue
        history = get_deep_dive_history(ticker)
        render(env, "deep_dive.html.j2",
               DOCS_DIR / "deep-dives" / f"{ticker}.html",
               ticker=ticker,
               name=ticker_data.get("name", ticker),
               sector=ticker_data.get("sector", "Unknown"),
               score=ticker_data["score"],
               rs=ticker_data["rs"],
               base=ticker_data["base"],
               trend=ticker_data["trend"],
               stage2=ticker_data.get("stage2", True),
               price=ticker_data["price"],
               high_52w=ticker_data["high_52w"],
               pct_from_high=ticker_data["pct_from_high"],
               avg_volume=ticker_data["avg_volume"],
               vol_ratio=ticker_data["vol_ratio"],
               streak_days=ticker_data.get("streak_days", entry.get("streak_days", 1)),
               flagged_date=entry["flagged_date"],
               history=history)

    print(f"Site generated: {len(archive)} archived reports, {len(queue)} deep dives")
    return True


def _generate_archive(env: Environment, out_path: Path, archive: list, generated_at: str):
    """Generate the archive page inline (no separate template needed)."""
    rows = ""
    for entry in archive:
        rows += f"""
        <a href="daily/{entry['date']}.html" class="archive-row" style="text-decoration:none">
          <div class="archive-date">{entry['date']}</div>
          <div class="archive-title">Daily Report — {entry['breakout_count']} breakout setups</div>
          <div class="archive-meta">
            Top: <strong style="color:var(--text)">{entry['top_ticker']}</strong>
            ({entry['top_score']:.1f})
          </div>
        </a>
        """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MorningSignal — Archive</title>
  <link rel="stylesheet" href="assets/style.css">
  <link rel="icon" href="assets/logo.svg" type="image/svg+xml">
</head>
<body>
<header>
  <div class="header-inner">
    <a href="index.html" class="logo">
      <img src="assets/logo.svg" width="32" height="32" alt="MorningSignal">
      <div>
        <div class="logo-text">Morning<span>Signal</span></div>
        <div class="logo-sub">Research Platform</div>
      </div>
    </a>
    <nav>
      <a href="index.html">Home</a>
      <a href="archive.html" class="active">Archive</a>
    </nav>
    <div class="header-date">{generated_at}</div>
  </div>
</header>
<div class="hero-strip">
  <div class="page-wrapper">
    <div class="hero-title">Report Archive</div>
    <div class="hero-subtitle">{len(archive)} daily reports</div>
  </div>
</div>
<div class="page-wrapper">
  <div class="section">
    <div class="archive-list">
      {rows if rows else '<div style="color:var(--text-dim);padding:20px">No archived reports yet.</div>'}
    </div>
  </div>
</div>
<footer>
  <div>MorningSignal Research · Generated {generated_at} · Not financial advice.</div>
</footer>
</body>
</html>"""
    out_path.write_text(html, encoding="utf-8")
    print(f"  Generated → {out_path.relative_to(out_path.parent.parent)}")


if __name__ == "__main__":
    today_arg = sys.argv[1] if len(sys.argv) > 1 else None
    ok = generate_site(today_arg)
    sys.exit(0 if ok else 1)
