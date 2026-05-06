"""
Site generator — reads state/ JSON files and assembles static HTML into docs/.
"""

import json
import sys
from datetime import date, datetime
from html import escape
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from scanner.market_brief import fallback_narrative, narrative_has_content

STATE_DIR = BASE_DIR / "state"
TEMPLATES_DIR = Path(__file__).parent / "templates"
DOCS_DIR = BASE_DIR / "docs"
ASSETS_SRC = Path(__file__).parent / "assets"

# Ensure output directories exist
(DOCS_DIR / "daily").mkdir(parents=True, exist_ok=True)
(DOCS_DIR / "deep-dives").mkdir(parents=True, exist_ok=True)
(DOCS_DIR / "weekly").mkdir(parents=True, exist_ok=True)

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


def get_today_industries(today: str) -> dict | None:
    """Load industry-scan output for `today`. Falls back to the most recent file."""
    p = STATE_DIR / f"industries_{today}.json"
    if p.exists():
        return load_json(p)
    files = sorted(STATE_DIR.glob("industries_*.json"), reverse=True)
    if files:
        return load_json(files[0])
    return None


def get_today_dashboard(today: str) -> dict | None:
    """Load dashboard data file for `today`. Falls back to the most recent."""
    p = STATE_DIR / f"dashboard_{today}.json"
    if p.exists():
        return load_json(p)
    files = sorted(STATE_DIR.glob("dashboard_*.json"), reverse=True)
    if files:
        return load_json(files[0])
    return None


def get_investordebate_index() -> dict | None:
    p = STATE_DIR / "investordebate_index.json"
    if p.exists():
        return load_json(p)
    return None


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


def _normalize_brief_for_render(brief: dict | None) -> dict | None:
    if brief is None:
        return None

    narrative = brief.get("narrative")
    if not narrative_has_content(narrative):
        brief = dict(brief)
        brief["narrative"] = fallback_narrative(brief)

    return brief


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

    brief = _normalize_brief_for_render(brief)
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

    # ── 2b. Industries page (sub-sector intelligence) ─────────────────────
    industries_data = get_today_industries(today)
    if industries_data:
        # Top-level page lives at /industries.html (matches nav prefix='')
        render(env, "industries.html.j2", DOCS_DIR / "industries.html",
               report_date=industries_data.get("date", today),
               generated_at=generated_at,
               data=industries_data,
               prefix="")
        # Mirror at /industries/<date>.html for archival deep-link
        ind_dir = DOCS_DIR / "industries"
        ind_dir.mkdir(parents=True, exist_ok=True)
        render(env, "industries.html.j2", ind_dir / f"{industries_data.get('date', today)}.html",
               report_date=industries_data.get("date", today),
               generated_at=generated_at,
               data=industries_data,
               prefix="../")
    else:
        print("  Note: no industry scan output found — skipping industries page.")

    # ── 2c. Dashboard page (Bridgewater + Citadel + Koyfin hybrid) ─────────
    dashboard_data = get_today_dashboard(today)
    if dashboard_data and industries_data:
        # The RRG chart data is serialised into the page as JSON for Chart.js
        rrg_payload = []
        for rec in industries_data.get("industries", []):
            if rec.get("rrg") and rec["rrg"].get("tail"):
                rrg_payload.append({
                    "industry": rec["industry"],
                    "sector": rec["sector"],
                    "quadrant": rec["rrg"]["quadrant"],
                    "tail": rec["rrg"]["tail"],
                })
        render(env, "dashboard.html.j2", DOCS_DIR / "dashboard.html",
               report_date=dashboard_data.get("date", today),
               generated_at=generated_at,
               brief=brief,
               industries=industries_data,
               dashboard=dashboard_data,
               rrg_json=json.dumps(rrg_payload))
    else:
        print("  Note: no dashboard data (or industries data) — skipping dashboard page.")

    # ── 2d. InvestorDebate index + per-sector pages ───────────────────────
    id_index = get_investordebate_index()
    if id_index is not None:
        # Always render the index so the section appears (even with no data).
        render(env, "investordebate_index.html.j2", DOCS_DIR / "investordebate.html",
               index_data=id_index,
               generated_at=generated_at)

        # Per-sector pages — only render where we have actual data
        id_dir = DOCS_DIR / "investordebate"
        id_dir.mkdir(parents=True, exist_ok=True)
        for sector in id_index.get("sectors", []):
            if not sector.get("has_data"):
                continue
            # latest sector page at /investordebate/{slug}.html
            render(env, "investordebate_sector.html.j2",
                   id_dir / f"{sector['sector_slug']}.html",
                   sector=sector,
                   generated_at=generated_at)
            # Archive each historical report at /investordebate/{slug}-{date}.html
            for h in sector.get("history", []):
                archive_path = id_dir / f"{sector['sector_slug']}-{h['date']}.html"
                # Skip if it's the latest one (already rendered)
                if h["date"] == sector["latest"]["date"] and archive_path.name == f"{sector['sector_slug']}-{h['date']}.html":
                    pass  # we still create the archive entry below for direct linking
        print(f"  Generated InvestorDebate index ({id_index['sectors_with_data']}/11 sectors with data)")

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

    # ── 5. Weekly summary pages ─────────────────────────────────────────────
    weekly_files = sorted(STATE_DIR.glob("weekly_summary_*.json"), reverse=True)
    latest_weekly = None
    for wf in weekly_files:
        week_data = load_json(wf)
        if week_data:
            week_date = wf.stem.replace("weekly_summary_", "")
            render(env, "weekly.html.j2",
                   DOCS_DIR / "weekly" / f"{week_date}.html",
                   week=week_data,
                   prefix="../")
            if latest_weekly is None:
                latest_weekly = week_data
                # Also render as the main weekly.html
                render(env, "weekly.html.j2",
                       DOCS_DIR / "weekly.html",
                       week=week_data,
                       prefix="")

    # ── 6. Podcast pages ────────────────────────────────────────────────────
    _generate_podcast_pages(DOCS_DIR, generated_at)

    # ── 7. Earnings pages ─────────────────────────────────────────────────
    _generate_earnings_pages(DOCS_DIR, generated_at)

    print(f"Site generated: {len(archive)} archived reports, {len(queue)} deep dives, {len(weekly_files)} weekly summaries")
    return True


def _nav_html(active: str, generated_at: str, prefix: str = "") -> str:
    """Generate consistent nav HTML. prefix is '../' for subpages."""
    links = [
        ("index.html", "Home"), ("dashboard.html", "Dashboard"),
        ("industries.html", "Industries"),
        ("investordebate.html", "Debate"),
        ("weekly.html", "Weekly"),
        ("podcast.html", "Podcast"), ("earnings.html", "Earnings"),
        ("archive.html", "Archive"),
    ]
    nav_links = " ".join(
        f'<a href="{prefix}{href}" {"class=active" if name == active else ""}>{name}</a>'
        for href, name in links
    )
    return f"""<header>
  <div class="header-inner">
    <a href="{prefix}index.html" class="logo">
      <img src="{prefix}assets/logo.svg" width="32" height="32" alt="MorningSignal">
      <div><div class="logo-text">Morning<span>Signal</span></div><div class="logo-sub">Research Platform</div></div>
    </a>
    <nav>{nav_links}</nav>
    <div class="header-date">{generated_at}</div>
  </div>
</header>"""


def _page_shell(title: str, active: str, generated_at: str, body: str, prefix: str = "", extra_css: str = "") -> str:
    """Wrap body content in full HTML page with consistent nav."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MorningSignal — {title}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600;9..144,700&family=IBM+Plex+Mono:wght@400;500;600&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="{prefix}assets/style.css">
  <link rel="icon" href="{prefix}assets/logo.svg" type="image/svg+xml">
  {f'<style>{extra_css}</style>' if extra_css else ''}
</head>
<body>
{_nav_html(active, generated_at, prefix)}
{body}
<footer>
  <div>MorningSignal Research · Generated {generated_at} · Not financial advice.</div>
</footer>
</body>
</html>"""


def _generate_podcast_pages(docs_dir: Path, generated_at: str):
    """Generate podcast index + individual episode pages with full transcripts."""
    podcast_dir = Path("/Users/max/morning-briefing/output")
    podcastv2_dir = Path("/Users/max/podcastbrief-v2/outputs")
    ep_dir = docs_dir / "podcast"
    ep_dir.mkdir(parents=True, exist_ok=True)

    episodes = []
    for mp3 in sorted(podcast_dir.glob("morning_signal_*.mp3"), reverse=True):
        d = mp3.stem.replace("morning_signal_", "")
        script = podcastv2_dir / f"PodcastBrief_{d}_script.txt"
        briefing = podcastv2_dir / f"PodcastBrief_{d}.md"

        transcript = ""
        if script.exists():
            transcript = script.read_text(encoding="utf-8", errors="replace")
        elif briefing.exists():
            transcript = briefing.read_text(encoding="utf-8", errors="replace")

        episodes.append({
            "date": d,
            "audio_url": f"https://podcast.morningsignal.xyz/morning_signal_{d}.mp3",
            "transcript": transcript,
            "preview": transcript[:200].replace("<", "&lt;").replace(">", "&gt;") if transcript else "",
            "size_mb": round(mp3.stat().st_size / (1024 * 1024), 1),
        })

    # Generate individual episode pages
    for ep in episodes:
        transcript_html = ep["transcript"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br>\n")
        body = f"""
<div class="hero-strip">
  <div class="page-wrapper">
    <div class="hero-title">Morning Signal — {ep['date']}</div>
    <div class="hero-subtitle">{ep['size_mb']}MB · <a href="{ep['audio_url']}" style="color:var(--blue)">Download MP3</a></div>
  </div>
</div>
<div class="page-wrapper" style="max-width:900px">
  <div class="section">
    <div class="section-title">Listen</div>
    <div class="card">
      <audio controls style="width:100%" preload="none">
        <source src="{ep['audio_url']}" type="audio/mpeg">
      </audio>
    </div>
  </div>
  <div class="section">
    <div class="section-title">Full Transcript</div>
    <div class="card" style="font-size:0.84rem;line-height:1.8;color:var(--text);max-height:none">
      {transcript_html if transcript_html else '<span style="color:var(--text-dim)">No transcript available</span>'}
    </div>
  </div>
</div>"""
        html = _page_shell(f"Podcast — {ep['date']}", "Podcast", generated_at, body, prefix="../")
        (ep_dir / f"{ep['date']}.html").write_text(html, encoding="utf-8")

    # Generate index page
    rows = ""
    for ep in episodes:
        rows += f"""
        <a href="podcast/{ep['date']}.html" class="archive-row" style="text-decoration:none">
          <div class="archive-date">{ep['date']}</div>
          <div class="archive-title" style="flex:1">Morning Signal — {ep['date']}</div>
          <div class="archive-meta">{ep['size_mb']}MB</div>
          <span class="badge badge-green" style="font-size:0.7rem">Play</span>
        </a>"""

    body = f"""
<div class="hero-strip">
  <div class="page-wrapper">
    <div class="hero-title">Morning Signal Podcast</div>
    <div class="hero-subtitle">Daily OpenAI-generated audio briefing · Subscribe: <a href="https://podcast.morningsignal.xyz/feed" style="color:var(--blue)">RSS Feed</a></div>
  </div>
</div>
<div class="page-wrapper" style="max-width:900px">
  <div class="section">
    <div class="section-title">Episodes ({len(episodes)} available)</div>
    <div class="archive-list">
      {rows if rows else '<div style="color:var(--text-dim);padding:20px">No episodes available yet.</div>'}
    </div>
  </div>
</div>"""
    html = _page_shell("Podcast", "Podcast", generated_at, body)
    (docs_dir / "podcast.html").write_text(html, encoding="utf-8")
    print(f"  Generated → docs/podcast.html ({len(episodes)} episodes)")


def _simple_markdown_to_html(content: str) -> str:
    """Very small fallback renderer when python-markdown isn't available."""
    blocks = []
    lines = content.splitlines()
    in_list = False

    def close_list():
        nonlocal in_list
        if in_list:
            blocks.append("</ul>")
            in_list = False

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()

        if not stripped:
            close_list()
            continue

        if stripped == "---":
            close_list()
            blocks.append("<hr />")
            continue

        if stripped.startswith("#"):
            close_list()
            level = min(len(stripped) - len(stripped.lstrip("#")), 4)
            text = escape(stripped[level:].strip())
            blocks.append(f"<h{level}>{text}</h{level}>")
            continue

        if stripped.startswith(("- ", "* ")):
            if not in_list:
                blocks.append("<ul>")
                in_list = True
            blocks.append(f"<li>{escape(stripped[2:].strip())}</li>")
            continue

        close_list()
        blocks.append(f"<p>{escape(stripped)}</p>")

    close_list()
    return "\n".join(blocks)


def _generate_earnings_pages(docs_dir: Path, generated_at: str):
    """Generate earnings index + individual brief pages."""
    try:
        import markdown as _md
    except ModuleNotFoundError:
        _md = None
    earnings_dir = Path(__file__).parent.parent / "state" / "earnings"
    ep_dir = docs_dir / "earnings"
    ep_dir.mkdir(parents=True, exist_ok=True)

    earnings_css = """
    .earnings-content h1 { font-size:1.1rem; font-weight:700; margin:16px 0 8px; }
    .earnings-content h2 { font-size:0.95rem; font-weight:700; margin:14px 0 6px; }
    .earnings-content h3 { font-size:0.88rem; font-weight:600; margin:12px 0 4px; }
    .earnings-content h4 { font-size:0.84rem; font-weight:600; margin:10px 0 4px; color:var(--text-dim); }
    .earnings-content p { margin:6px 0; }
    .earnings-content ul, .earnings-content ol { margin:6px 0; padding-left:20px; }
    .earnings-content li { margin:3px 0; }
    .earnings-content strong { color:var(--text); }
    .earnings-content table { border-collapse:collapse; width:100%; margin:10px 0; font-size:0.8rem; font-family:var(--font-mono); }
    .earnings-content th { text-align:left; padding:6px 10px; border-bottom:2px solid var(--border); font-weight:600; color:var(--text-dim); }
    .earnings-content td { padding:5px 10px; border-bottom:1px solid var(--border); }
    .earnings-content hr { border:none; border-top:1px solid var(--border); margin:16px 0; }
    .earnings-content code { font-family:var(--font-mono); font-size:0.82em; background:rgba(0,0,0,0.04); padding:1px 4px; border-radius:3px; }
    """

    briefs = []
    for md_file in sorted(earnings_dir.glob("earnings_*.md"), reverse=True):
        parts = md_file.stem.split("_")
        if len(parts) >= 3:
            briefs.append({
                "date": parts[1],
                "session": parts[2],
                "content": md_file.read_text(encoding="utf-8", errors="replace"),
                "slug": f"{parts[1]}_{parts[2]}",
            })

    # Generate individual brief pages
    for b in briefs:
        session_label = "Pre-Market Brief" if b["session"] == "AM" else "Post-Close Brief"
        if _md is not None:
            html_content = _md.markdown(b["content"], extensions=["tables", "fenced_code"])
        else:
            html_content = _simple_markdown_to_html(b["content"])
        body = f"""
<div class="hero-strip">
  <div class="page-wrapper">
    <div class="hero-title">{session_label} — {b['date']}</div>
    <div class="hero-subtitle"><a href="../earnings.html" style="color:var(--blue)">← All Briefs</a></div>
  </div>
</div>
<div class="page-wrapper" style="max-width:900px">
  <div class="section">
    <div class="earnings-content" style="font-size:0.84rem;line-height:1.7;color:var(--text)">
      {html_content}
    </div>
  </div>
</div>"""
        html = _page_shell(f"Earnings — {b['date']} {b['session']}", "Earnings", generated_at, body, prefix="../", extra_css=earnings_css)
        (ep_dir / f"{b['slug']}.html").write_text(html, encoding="utf-8")

    # Generate index page
    rows = ""
    for b in briefs:
        session_label = "Pre-Market" if b["session"] == "AM" else "Post-Close"
        session_color = "badge-green" if b["session"] == "AM" else "badge-yellow"
        # Extract first heading as title
        first_line = ""
        for line in b["content"].splitlines():
            if line.startswith("#"):
                first_line = line.lstrip("# ").strip()[:80]
                break
        rows += f"""
        <a href="earnings/{b['slug']}.html" class="archive-row" style="text-decoration:none">
          <div class="archive-date">{b['date']}</div>
          <div class="archive-title" style="flex:1">{first_line or f'Earnings Brief — {b["date"]}'}</div>
          <span class="badge {session_color}" style="font-size:0.65rem">{session_label}</span>
        </a>"""

    body = f"""
<div class="hero-strip">
  <div class="page-wrapper">
    <div class="hero-title">Earnings Intelligence Briefs</div>
    <div class="hero-subtitle">{len(briefs)} briefs · Pre-market & post-close analysis</div>
  </div>
</div>
<div class="page-wrapper" style="max-width:900px">
  <div class="section">
    <div class="section-title">All Briefs</div>
    <div class="archive-list">
      {rows if rows else '<div style="color:var(--text-dim);padding:20px">No earnings briefs available yet.</div>'}
    </div>
  </div>
</div>"""
    html = _page_shell("Earnings Briefs", "Earnings", generated_at, body)
    (docs_dir / "earnings.html").write_text(html, encoding="utf-8")
    print(f"  Generated → docs/earnings.html ({len(briefs)} briefs)")


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

    body = f"""<div class="hero-strip">
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
 </div>"""
    html = _page_shell("Archive", "Archive", generated_at, body)
    out_path.write_text(html, encoding="utf-8")
    print(f"  Generated → {out_path.relative_to(out_path.parent.parent)}")


if __name__ == "__main__":
    today_arg = sys.argv[1] if len(sys.argv) > 1 else None
    ok = generate_site(today_arg)
    sys.exit(0 if ok else 1)
