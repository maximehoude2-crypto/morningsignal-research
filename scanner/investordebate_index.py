"""
InvestorDebate Index & Diff Engine.

Walks state/investordebate/ for files named:
    investordebate-{sector-slug}-{YYYY-MM-DD}.md

Parses each report to extract:
    • sector name (canonical) & date
    • Executive Summary (Macro Context, Top 5 ranked stocks, Key Sector Call,
      Biggest Disagreement, Where We Differ From Consensus, What We're Probably
      Wrong About)
    • Detailed stock analyses (rank, ticker, rating, conviction, composite,
      bull case, bear case, key debate point, catalysts)

Groups all reports by sector, sorts each group by date desc, and computes a
per-sector diff between the latest report and the previous report:
    • rank changes      (e.g. NVDA #2 → #1)
    • rating changes    (e.g. AAPL BUY → HOLD)
    • new entrants      (top-N stocks that weren't in the prior top-N)
    • dropouts          (stocks that fell out of the top-N)
    • conviction deltas (1-5 score changes per stock)

Saves the consolidated index to state/investordebate_index.json.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
REPORTS_DIR = STATE_DIR / "investordebate"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = STATE_DIR / "investordebate_index.json"


# 11 GICS sectors with their canonical name + slug
SECTOR_CANONICAL: dict[str, dict] = {
    "information-technology": {"name": "Information Technology", "color": "#315b96"},
    "health-care":            {"name": "Health Care",            "color": "#0f7b54"},
    "financials":             {"name": "Financials",             "color": "#9d6a2e"},
    "consumer-discretionary": {"name": "Consumer Discretionary", "color": "#a97d22"},
    "communication-services": {"name": "Communication Services", "color": "#5e6878"},
    "industrials":            {"name": "Industrials",            "color": "#26354d"},
    "consumer-staples":       {"name": "Consumer Staples",       "color": "#7c8594"},
    "energy":                 {"name": "Energy",                 "color": "#c45650"},
    "materials":              {"name": "Materials",              "color": "#192232"},
    "real-estate":            {"name": "Real Estate",            "color": "#a97d22"},
    "utilities":              {"name": "Utilities",              "color": "#0f7b54"},
}

# ---------------------------------------------------------------------------
# Filename / metadata extraction
# ---------------------------------------------------------------------------

REPORT_FILE_RE = re.compile(
    r"^investordebate-(?P<slug>[a-z0-9-]+)-(?P<date>\d{4}-\d{2}-\d{2})\.md$"
)


def _meta_from_filename(p: Path) -> dict | None:
    m = REPORT_FILE_RE.match(p.name)
    if not m:
        return None
    slug = m.group("slug")
    canonical = SECTOR_CANONICAL.get(slug, {"name": slug.replace("-", " ").title(), "color": "#5e6878"})
    return {
        "path": str(p),
        "filename": p.name,
        "sector_slug": slug,
        "sector_name": canonical["name"],
        "sector_color": canonical["color"],
        "date": m.group("date"),
    }


# ---------------------------------------------------------------------------
# Markdown parsing
# ---------------------------------------------------------------------------

RATING_PATTERNS = ["STRONG BUY", "STRONG SELL", "BUY", "SELL", "HOLD"]


def _section(md: str, header_re: str, max_lines: int = 60) -> str:
    """Extract the prose under a markdown header."""
    pat = re.compile(rf"^{header_re}\s*$", re.IGNORECASE | re.MULTILINE)
    m = pat.search(md)
    if not m:
        return ""
    rest = md[m.end():]
    lines: list[str] = []
    for line in rest.splitlines():
        if line.startswith("#") or line.startswith("---"):
            break
        lines.append(line)
        if len(lines) >= max_lines:
            break
    return "\n".join(lines).strip()


def _parse_top5_table(md: str) -> list[dict]:
    """
    Look for the Top 5 markdown table:
        | Rank | Ticker | Rating | Conviction (1-5) | Composite Score | One-Line Thesis |
    """
    # Find a markdown table that contains 'Rank', 'Ticker', 'Rating', 'Conviction'
    table_re = re.compile(
        r"\|\s*Rank\s*\|.*?Ticker.*?\|.*?Rating.*?\|.*?Conviction.*?\|(.*?)(?=\n\n|\n#|\Z)",
        re.IGNORECASE | re.DOTALL,
    )
    m = table_re.search(md)
    if not m:
        return []
    body = m.group(0)
    rows = []
    for line in body.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 5:
            continue
        # Skip header / separator rows
        if cells[0].lower() in ("rank", "----", "----:", "---"):
            continue
        if not re.match(r"^\d+$", cells[0]):
            continue
        rank = int(cells[0])
        ticker = cells[1].upper().strip()
        rating = cells[2].upper()
        # Normalize rating
        for pat in RATING_PATTERNS:
            if pat in rating:
                rating = pat; break
        conviction_raw = cells[3]
        conviction = None
        m2 = re.search(r"(\d+(\.\d+)?)", conviction_raw)
        if m2:
            try: conviction = float(m2.group(1))
            except ValueError: conviction = None
        composite_raw = cells[4]
        composite = None
        m3 = re.search(r"(\d+(\.\d+)?)", composite_raw)
        if m3:
            try: composite = float(m3.group(1))
            except ValueError: composite = None
        thesis = cells[5] if len(cells) > 5 else ""
        rows.append({
            "rank": rank,
            "ticker": ticker,
            "rating": rating,
            "conviction": conviction,
            "composite": composite,
            "thesis": thesis,
        })
    return rows


def _parse_detailed_stocks(md: str) -> list[dict]:
    """
    Find sections like:
        ### 1. NVDA — NVIDIA Corp
            **Current Price:** $XXX | **Market Cap:** $XXXB | **YTD:** +/-XX%
            **Committee Rating:** Strong Buy
            **Conviction Score:** 5/5
            **Composite Score:** X.X/10
            **Bull Case (3-4 sentences):** ...
            **Bear Case (3-4 sentences):** ...
            **Key Debate Point:** ...
    """
    out = []
    pattern = re.compile(
        r"^###\s*(\d+)\.\s+([A-Z][A-Z0-9\.-]{0,5})\s*[—–-]\s*(.+?)$",
        re.MULTILINE,
    )
    matches = list(pattern.finditer(md))
    for idx, m in enumerate(matches):
        rank = int(m.group(1))
        ticker = m.group(2).upper().strip()
        company = m.group(3).strip()
        start = m.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(md)
        body = md[start:end]

        def _grab(label: str) -> str:
            r = re.search(rf"\*\*{label}.*?\*\*\s*(.+?)(?=\n\*\*|\n###|\n##|\n---|$)",
                          body, re.IGNORECASE | re.DOTALL)
            return r.group(1).strip() if r else ""

        rating = ""
        m_rate = re.search(r"\*\*Committee Rating:\*\*\s*\[?([\w\s]+?)\]?\s*(?:\n|$)", body, re.IGNORECASE)
        if m_rate:
            rating = m_rate.group(1).strip().upper()
            for pat in RATING_PATTERNS:
                if pat in rating:
                    rating = pat; break

        conviction = None
        m_c = re.search(r"\*\*Conviction (?:Score)?:\*\*\s*(\d+(?:\.\d+)?)\s*/\s*5", body, re.IGNORECASE)
        if m_c:
            try: conviction = float(m_c.group(1))
            except ValueError: pass

        composite = None
        m_co = re.search(r"\*\*Composite Score:\*\*\s*(\d+(?:\.\d+)?)\s*/\s*10", body, re.IGNORECASE)
        if m_co:
            try: composite = float(m_co.group(1))
            except ValueError: pass

        out.append({
            "rank": rank,
            "ticker": ticker,
            "company": company,
            "rating": rating,
            "conviction": conviction,
            "composite": composite,
            "bull_case": _grab(r"Bull Case[^*]*"),
            "bear_case": _grab(r"Bear Case[^*]*"),
            "key_debate": _grab(r"Key Debate Point[^*]*"),
            "body": body.strip(),
        })
    return out


def parse_report(path: Path) -> dict:
    """Parse a single InvestorDebate markdown report."""
    meta = _meta_from_filename(path)
    if not meta:
        raise ValueError(f"Filename does not match pattern: {path.name}")
    md = path.read_text(encoding="utf-8", errors="replace")

    # Universe count from header
    universe = None
    m_univ = re.search(r"\*\*Universe:\*\*\s*(\d+)", md)
    if m_univ:
        universe = int(m_univ.group(1))

    macro = _section(md, r"###\s*Macro Context", 30)
    sector_call = _section(md, r"###\s*Key Sector Call", 30)
    disagreement = _section(md, r"###\s*Biggest Disagreement", 30)
    contrarian = _section(md, r"###\s*Where We Differ From Consensus", 30)
    wrong_about = _section(md, r"###\s*What We[''`]re Probably Wrong About", 20)

    top5 = _parse_top5_table(md)
    detailed = _parse_detailed_stocks(md)

    return {
        **meta,
        "universe_size": universe,
        "macro_context": macro,
        "sector_call": sector_call,
        "biggest_disagreement": disagreement,
        "contrarian_call": contrarian,
        "wrong_about": wrong_about,
        "top5": top5,
        "detailed_stocks": detailed,
        "raw_markdown": md,
    }


# ---------------------------------------------------------------------------
# Diff engine
# ---------------------------------------------------------------------------

def compute_diff(current: dict, prior: dict | None) -> dict:
    """
    Diff current vs prior report (same sector). Returns dict with:
      rank_changes, rating_changes, new_entrants, dropouts, conviction_deltas
    """
    if prior is None:
        return {"new_sector": True}

    cur_top = {row["ticker"]: row for row in current.get("top5", [])}
    prev_top = {row["ticker"]: row for row in prior.get("top5", [])}

    rank_changes = []
    rating_changes = []
    conviction_deltas = []
    for tkr, row in cur_top.items():
        pr = prev_top.get(tkr)
        if pr is None:
            continue
        if row.get("rank") != pr.get("rank"):
            rank_changes.append({
                "ticker": tkr,
                "prior_rank": pr.get("rank"),
                "current_rank": row.get("rank"),
                "delta": (pr.get("rank") or 0) - (row.get("rank") or 0),
            })
        if (row.get("rating") or "") != (pr.get("rating") or ""):
            rating_changes.append({
                "ticker": tkr,
                "prior_rating": pr.get("rating"),
                "current_rating": row.get("rating"),
            })
        c1, c0 = row.get("conviction"), pr.get("conviction")
        if c1 is not None and c0 is not None and c1 != c0:
            conviction_deltas.append({
                "ticker": tkr,
                "prior": c0, "current": c1, "delta": round(c1 - c0, 2),
            })

    new_entrants = [
        {"ticker": tkr, "rank": cur_top[tkr]["rank"],
         "rating": cur_top[tkr]["rating"], "thesis": cur_top[tkr].get("thesis", "")}
        for tkr in cur_top if tkr not in prev_top
    ]
    dropouts = [
        {"ticker": tkr, "prior_rank": prev_top[tkr]["rank"],
         "prior_rating": prev_top[tkr]["rating"]}
        for tkr in prev_top if tkr not in cur_top
    ]

    return {
        "new_sector": False,
        "prior_date": prior.get("date"),
        "rank_changes": sorted(rank_changes, key=lambda x: -abs(x["delta"])),
        "rating_changes": rating_changes,
        "new_entrants": sorted(new_entrants, key=lambda x: x["rank"]),
        "dropouts": sorted(dropouts, key=lambda x: x["prior_rank"]),
        "conviction_deltas": sorted(conviction_deltas, key=lambda x: -abs(x["delta"])),
    }


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def run_investordebate_index(dry_run: bool = False) -> dict:
    """Parse every report file, group by sector, compute diffs, save index."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    all_files = sorted(REPORTS_DIR.glob("investordebate-*.md"))
    print(f"Found {len(all_files)} InvestorDebate report file(s) in {REPORTS_DIR.relative_to(BASE_DIR)}")

    # Parse all
    parsed: list[dict] = []
    for p in all_files:
        try:
            parsed.append(parse_report(p))
        except Exception as exc:  # noqa: BLE001
            print(f"  ! could not parse {p.name}: {exc}")

    # Group by sector_slug
    by_sector: dict[str, list[dict]] = {}
    for r in parsed:
        by_sector.setdefault(r["sector_slug"], []).append(r)
    for slug, lst in by_sector.items():
        lst.sort(key=lambda x: x["date"], reverse=True)

    # Build per-sector index entries (latest + diff vs prior)
    sectors_out: list[dict] = []
    for slug, canonical in SECTOR_CANONICAL.items():
        history = by_sector.get(slug, [])
        if not history:
            sectors_out.append({
                "sector_slug": slug,
                "sector_name": canonical["name"],
                "sector_color": canonical["color"],
                "has_data": False,
                "history": [],
            })
            continue

        latest = history[0]
        prior = history[1] if len(history) > 1 else None
        diff = compute_diff(latest, prior)

        # Strip the heavy `raw_markdown` and `body` fields from history records
        # for the index file (we keep them only for `latest` + serve raw for sector pages).
        thin_history = [
            {
                "date": h["date"],
                "filename": h["filename"],
                "top5": h.get("top5", []),
                "universe_size": h.get("universe_size"),
            }
            for h in history
        ]

        # Latest payload — keep the heavy stuff for the sector page renderer
        sectors_out.append({
            "sector_slug": slug,
            "sector_name": canonical["name"],
            "sector_color": canonical["color"],
            "has_data": True,
            "latest": latest,
            "diff": diff,
            "history": thin_history,
            "report_count": len(history),
        })

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_reports": len(parsed),
        "sectors_with_data": sum(1 for s in sectors_out if s["has_data"]),
        "sectors": sectors_out,
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Saved investordebate index → {OUT_PATH.relative_to(BASE_DIR)}")
    print(f"  Sectors with reports: {payload['sectors_with_data']}/11 · "
          f"Total reports: {payload['total_reports']}")
    return payload


if __name__ == "__main__":
    run_investordebate_index()
