from __future__ import annotations

"""
Generate and manage earnings briefs in state/earnings/ using OpenAI.
"""

import json
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import requests
import yfinance as yf

from scanner.market_brief import fallback_narrative
from scanner.openai_client import complete_text, openai_enabled
from scanner.thematic_scanner import _scrape_catalyst_calendar

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state" / "earnings"
STATE_DIR.mkdir(parents=True, exist_ok=True)
CALENDAR_STATE_DIR = BASE_DIR / "state"

MIN_EARNINGS_MARKET_CAP = 2_000_000_000
MAX_SNAPSHOTS = 35
MAX_EVIDENCE_COMPANIES = 30
MAX_EVIDENCE_CHARS = 4500


def _is_weekday(d: date) -> bool:
    return d.isoweekday() <= 5


def _next_weekday(d: date) -> date:
    probe = d + timedelta(days=1)
    while not _is_weekday(probe):
        probe += timedelta(days=1)
    return probe


def _format_short(d: date) -> str:
    return d.strftime("%b %d")


def _session_matches(time_text: str, session: str) -> bool:
    lowered = (time_text or "").lower()
    if session == "AM":
        return any(token in lowered for token in ("before", "pre", "am", "open", "time-pre-market"))
    return any(token in lowered for token in ("after", "post", "pm", "close", "time-after-hours"))


def _parse_market_cap(value: str | int | float | None) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    raw = str(value).strip()
    if not raw or raw.upper() in {"N/A", "NA", "--"}:
        return 0
    cleaned = re.sub(r"[^0-9.\-KMBTkmbt]", "", raw)
    if not cleaned:
        return 0
    multiplier = 1
    suffix = cleaned[-1].upper()
    if suffix in {"K", "M", "B", "T"}:
        cleaned = cleaned[:-1]
        multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "T": 1_000_000_000_000}[suffix]
    try:
        return int(float(cleaned.replace(",", "")) * multiplier)
    except ValueError:
        return 0


def _session_label_from_time(time_text: str) -> str:
    lowered = (time_text or "").lower()
    if _session_matches(lowered, "AM"):
        return "AM"
    if _session_matches(lowered, "PM"):
        return "PM"
    return "UNSPECIFIED"


def _fetch_nasdaq_earnings_calendar(target_date: str) -> list[dict]:
    """Fetch Nasdaq earnings calendar rows for a date."""
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={target_date}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/market-activity/earnings",
    }
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    payload = response.json()
    rows = ((payload.get("data") or {}).get("rows") or [])
    out: list[dict] = []
    for row in rows:
        ticker = str(row.get("symbol") or "").upper().strip()
        if not ticker:
            continue
        market_cap = _parse_market_cap(row.get("marketCap"))
        out.append({
            "ticker": ticker,
            "name": str(row.get("name") or ticker).strip(),
            "date": target_date,
            "time": str(row.get("time") or "").strip(),
            "session": _session_label_from_time(str(row.get("time") or "")),
            "market_cap": market_cap,
            "market_cap_text": row.get("marketCap") or "",
            "fiscal_quarter": row.get("fiscalQuarterEnding") or "",
            "eps_forecast": row.get("epsForecast") or "",
            "num_estimates": row.get("noOfEsts") or "",
            "last_year_report_date": row.get("lastYearRptDt") or "",
            "last_year_eps": row.get("lastYearEPS") or "",
            "source": "Nasdaq earnings calendar",
        })
    return out


def _fallback_earnings_calendar(target_date: str) -> list[dict]:
    """Fallback to the existing catalyst scraper if Nasdaq is unavailable."""
    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    target_label = _format_short(target_dt)
    out = []
    for event in _scrape_catalyst_calendar():
        if event.get("type") != "earnings" or event.get("date") != target_label:
            continue
        ticker = str(event.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        out.append({
            "ticker": ticker,
            "name": ticker,
            "date": target_date,
            "time": event.get("time", ""),
            "session": _session_label_from_time(event.get("time", "")),
            "market_cap": 0,
            "market_cap_text": "",
            "fiscal_quarter": "",
            "eps_forecast": "",
            "num_estimates": "",
            "last_year_report_date": "",
            "last_year_eps": "",
            "source": "fallback catalyst calendar",
        })
    return out


def _load_earnings_calendar(target_date: str) -> list[dict]:
    try:
        rows = _fetch_nasdaq_earnings_calendar(target_date)
    except Exception as exc:
        print(f"  Warning: Nasdaq earnings calendar failed ({exc}); using fallback calendar.")
        rows = _fallback_earnings_calendar(target_date)

    cache_path = CALENDAR_STATE_DIR / f"earnings_calendar_{target_date}.json"
    cache_path.write_text(json.dumps({
        "date": target_date,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(rows),
        "rows": rows,
    }, indent=2), encoding="utf-8")
    return rows


def _market_context(target_date: str) -> dict:
    path = BASE_DIR / "state" / f"market_brief_{target_date}.json"
    if not path.exists():
        return {}
    try:
        brief = json.loads(path.read_text())
    except Exception:
        return {}

    narrative = brief.get("narrative") or {}
    summary = ""
    if isinstance(narrative, dict):
        summary = narrative.get("summary", "")
    elif isinstance(narrative, str):
        summary = narrative

    if not summary:
        summary = fallback_narrative(brief).get("summary", "")

    return {
        "summary": summary,
        "indices": {
            sym: {
                "name": idx.get("name", sym),
                "day_change": idx.get("day_change", 0),
            }
            for sym, idx in (brief.get("indices") or {}).items()
        },
    }


def _ticker_snapshot(ticker: str, calendar_row: dict | None = None) -> dict:
    stock = yf.Ticker(ticker)
    calendar_row = calendar_row or {}
    meta = {}
    history = None
    news_items = []

    try:
        meta = stock.info or {}
    except Exception:
        meta = {}

    try:
        history = stock.history(period="3mo", auto_adjust=False)
    except Exception:
        history = None

    try:
        news_items = stock.news or []
    except Exception:
        news_items = []

    close = 0.0
    one_day = 0.0
    five_day = 0.0
    if history is not None and not history.empty:
        close = float(history["Close"].iloc[-1])
        if len(history) >= 2:
            prev = float(history["Close"].iloc[-2])
            if prev:
                one_day = round((close / prev - 1) * 100, 2)
        if len(history) >= 6:
            prev5 = float(history["Close"].iloc[-6])
            if prev5:
                five_day = round((close / prev5 - 1) * 100, 2)

    headlines = []
    for item in news_items[:4]:
        title = item.get("title", "").strip()
        publisher = item.get("publisher", "").strip()
        if title:
            headlines.append(f"{publisher}: {title}" if publisher else title)

    if not headlines:
        headlines = _fetch_yahoo_rss_headlines(ticker)

    market_cap = calendar_row.get("market_cap") or meta.get("marketCap")
    if not market_cap:
        try:
            market_cap = getattr(stock.fast_info, "market_cap", None)
        except Exception:
            market_cap = None

    return {
        "ticker": ticker,
        "name": calendar_row.get("name") or meta.get("shortName") or meta.get("longName") or ticker,
        "sector": meta.get("sector") or "Unknown",
        "market_cap": market_cap or 0,
        "market_cap_text": calendar_row.get("market_cap_text", ""),
        "price": round(close, 2) if close else 0.0,
        "day_change": one_day,
        "five_day_change": five_day,
        "fiscal_quarter": calendar_row.get("fiscal_quarter", ""),
        "eps_forecast": calendar_row.get("eps_forecast", ""),
        "num_estimates": calendar_row.get("num_estimates", ""),
        "last_year_eps": calendar_row.get("last_year_eps", ""),
        "headlines": headlines,
    }


def _money(value: int | float | None) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    abs_num = abs(number)
    if abs_num >= 1_000_000_000:
        return f"${number / 1_000_000_000:.2f}B"
    if abs_num >= 1_000_000:
        return f"${number / 1_000_000:.1f}M"
    return f"${number:,.0f}"


def _safe_float(value) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _parse_calendar_quarter(value: str) -> pd.Timestamp | None:
    raw = (value or "").strip()
    if not raw:
        return None
    for fmt in ("%b/%Y", "%B/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return pd.Timestamp(dt).to_period("Q").end_time.normalize()
        except ValueError:
            continue
    return None


def _latest_actuals_from_yfinance(ticker: str, calendar_row: dict | None = None) -> dict:
    """Best-effort actual EPS/revenue evidence from yfinance structured fields."""
    calendar_row = calendar_row or {}
    stock = yf.Ticker(ticker)
    target_quarter = _parse_calendar_quarter(calendar_row.get("fiscal_quarter", ""))
    actuals: dict = {"source": "yfinance"}

    try:
        hist = stock.earnings_history
    except Exception:
        hist = None

    if hist is not None and not hist.empty:
        selected = None
        selected_key = None
        try:
            for idx, row in hist.sort_index(ascending=False).iterrows():
                idx_ts = pd.Timestamp(idx).to_period("Q").end_time.normalize()
                if target_quarter is None or idx_ts == target_quarter:
                    selected = row
                    selected_key = idx_ts
                    break
        except Exception:
            selected = None

        if selected is not None:
            eps_actual = _safe_float(selected.get("epsActual"))
            eps_estimate = _safe_float(selected.get("epsEstimate"))
            surprise = _safe_float(selected.get("surprisePercent"))
            actuals.update({
                "fiscal_period": selected_key.strftime("%Y-Q%q") if selected_key is not None else "",
                "eps_actual": eps_actual,
                "eps_estimate": eps_estimate,
                "eps_surprise_pct": round(surprise * 100, 2) if surprise is not None and abs(surprise) < 5 else surprise,
                "has_actual_eps": eps_actual is not None,
            })

    try:
        income = stock.quarterly_income_stmt
    except Exception:
        income = None

    if income is not None and not income.empty:
        col = None
        for candidate in income.columns:
            try:
                cand_ts = pd.Timestamp(candidate).to_period("Q").end_time.normalize()
                if target_quarter is None or cand_ts == target_quarter:
                    col = candidate
                    break
            except Exception:
                continue
        if col is None:
            col = income.columns[0]

        def _line(*names: str) -> float | None:
            for name in names:
                if name in income.index:
                    return _safe_float(income.loc[name, col])
            return None

        revenue = _line("Total Revenue", "Operating Revenue")
        gross_profit = _line("Gross Profit")
        operating_income = _line("Operating Income", "Operating Income Loss")
        net_income = _line("Net Income", "Net Income Common Stockholders")
        if revenue is not None:
            actuals.update({
                "revenue_actual": revenue,
                "revenue_actual_text": _money(revenue),
                "gross_profit": gross_profit,
                "gross_margin_pct": round(gross_profit / revenue * 100, 2) if gross_profit and revenue else None,
                "operating_income": operating_income,
                "operating_margin_pct": round(operating_income / revenue * 100, 2) if operating_income and revenue else None,
                "net_income": net_income,
                "has_actual_revenue": True,
            })

    try:
        yf_calendar = stock.calendar or {}
    except Exception:
        yf_calendar = {}
    if isinstance(yf_calendar, dict):
        revenue_avg = yf_calendar.get("Revenue Average")
        eps_avg = yf_calendar.get("Earnings Average")
        if revenue_avg:
            actuals["revenue_estimate"] = revenue_avg
            actuals["revenue_estimate_text"] = _money(revenue_avg)
        if eps_avg:
            actuals["calendar_eps_estimate"] = eps_avg

    actuals["has_actuals"] = bool(actuals.get("has_actual_eps") or actuals.get("has_actual_revenue"))
    return actuals


def _fetch_yahoo_rss_headlines(ticker: str, limit: int = 5) -> list[str]:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        response.raise_for_status()
        root = ET.fromstring(response.text)
    except Exception:
        return []

    headlines = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        publisher = (item.findtext("source") or "").strip()
        if title:
            headlines.append(f"{publisher}: {title}" if publisher else title)
        if len(headlines) >= limit:
            break
    return headlines


def _fetch_yahoo_rss_items(ticker: str, limit: int = 8) -> list[dict]:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        response.raise_for_status()
        root = ET.fromstring(response.text)
    except Exception:
        return []

    items: list[dict] = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        publisher = (item.findtext("source") or "").strip()
        if not title:
            continue
        items.append({"title": title, "url": link, "source": publisher or "Yahoo Finance RSS"})
        if len(items) >= limit:
            break
    return items


def _extract_plaintext(html: str, limit: int = MAX_EVIDENCE_CHARS) -> str:
    html = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", html)
    text = unquote(text)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def _fetch_url_text(url: str, limit: int = MAX_EVIDENCE_CHARS) -> str:
    if not url:
        return ""
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 MorningSignal/1.0"},
            timeout=15,
        )
        response.raise_for_status()
    except Exception:
        return ""
    return _extract_plaintext(response.text, limit=limit)


def _fetch_company_evidence(ticker: str, calendar_row: dict | None = None) -> dict:
    """Collect actual result, press-release, and call/transcript evidence."""
    rss_items = _fetch_yahoo_rss_items(ticker)
    evidence_items = []
    for item in rss_items:
        title_l = item["title"].lower()
        is_relevant = any(
            token in title_l
            for token in ("earnings", "results", "revenue", "eps", "guidance", "transcript", "conference call")
        )
        if not is_relevant:
            continue
        item = dict(item)
        if any(token in title_l for token in ("transcript", "conference call", "earnings call")):
            item["kind"] = "conference_call"
            item["text"] = _fetch_url_text(item.get("url", ""), limit=MAX_EVIDENCE_CHARS)
        elif any(token in title_l for token in ("reports", "announces", "results", "earnings")):
            item["kind"] = "press_or_news"
            item["text"] = _fetch_url_text(item.get("url", ""), limit=2500)
        else:
            item["kind"] = "headline"
            item["text"] = ""
        evidence_items.append(item)
        if len(evidence_items) >= 5:
            break

    actuals = _latest_actuals_from_yfinance(ticker, calendar_row)
    has_call = any(item.get("kind") == "conference_call" and item.get("text") for item in evidence_items)
    has_release_or_news = any(item.get("kind") == "press_or_news" for item in evidence_items)
    return {
        "actuals": actuals,
        "evidence_items": evidence_items,
        "has_actual_result": bool(actuals.get("has_actuals") or has_release_or_news),
        "has_conference_call": has_call,
    }


def _build_company_set(target_date: str, session: str) -> tuple[list[dict], list[dict]]:
    calendar = _load_earnings_calendar(target_date)

    primary_events = []
    seen: set[str] = set()
    for event in calendar:
        ticker = event.get("ticker", "").upper()
        if not ticker or ticker in seen:
            continue
        if event.get("market_cap", 0) < MIN_EARNINGS_MARKET_CAP:
            continue
        if event.get("session") != session:
            continue
        seen.add(ticker)
        primary_events.append(event)

    primary_events.sort(key=lambda item: item.get("market_cap", 0), reverse=True)

    snapshots = []
    for idx, event in enumerate(primary_events):
        ticker = event.get("ticker", "").upper()
        if idx < MAX_SNAPSHOTS:
            snap = _ticker_snapshot(ticker, event)
        else:
            snap = {
                "ticker": ticker,
                "name": event.get("name", ticker),
                "sector": "Unknown",
                "market_cap": event.get("market_cap", 0),
                "market_cap_text": event.get("market_cap_text", ""),
                "price": 0.0,
                "day_change": 0.0,
                "five_day_change": 0.0,
                "fiscal_quarter": event.get("fiscal_quarter", ""),
                "eps_forecast": event.get("eps_forecast", ""),
                "num_estimates": event.get("num_estimates", ""),
                "last_year_eps": event.get("last_year_eps", ""),
                "headlines": [],
            }
        snap["calendar"] = {
            "date": target_date,
            "time": event.get("time", ""),
            "session": event.get("session", ""),
            "source": event.get("source", ""),
        }
        if idx < MAX_EVIDENCE_COMPANIES:
            snap["earnings_evidence"] = _fetch_company_evidence(ticker, event)
        else:
            snap["earnings_evidence"] = {
                "actuals": {},
                "evidence_items": [],
                "has_actual_result": False,
                "has_conference_call": False,
                "deferred_reason": "Evidence collection limited to top market-cap reporters for runtime.",
            }
        snap["importance_rank"] = idx + 1
        snapshots.append(snap)

    watchlist = []
    seen_watch: set[str] = set()
    for event in calendar:
        ticker = event.get("ticker", "").upper()
        if not ticker or ticker in seen or ticker in seen_watch:
            continue
        if event.get("market_cap", 0) < MIN_EARNINGS_MARKET_CAP:
            continue
        seen_watch.add(ticker)
        watchlist.append({
            "ticker": ticker,
            "name": event.get("name", ticker),
            "date": target_date,
            "time": event.get("time", ""),
            "session": event.get("session", ""),
            "market_cap": event.get("market_cap", 0),
            "eps_forecast": event.get("eps_forecast", ""),
            "source": event.get("source", ""),
        })

    watchlist.sort(key=lambda item: item.get("market_cap", 0), reverse=True)
    return snapshots, watchlist[:25]


def generate_earnings_brief(target_date: str | None = None, session: str = "AM") -> Path:
    target_date = target_date or date.today().isoformat()
    companies, watchlist = _build_company_set(target_date, session)
    market_context = _market_context(target_date)
    session_label = "Pre-Market" if session == "AM" else "Post-Close"

    if not companies:
        raise RuntimeError(
            f"No >$2B {session_label.lower()} earnings companies found for {target_date}; refusing to generate empty brief."
        )

    if not openai_enabled():
        raise RuntimeError("OPENAI_API_KEY is not configured for earnings brief generation.")

    evidence_count = sum(1 for c in companies if (c.get("earnings_evidence") or {}).get("has_actual_result"))
    call_count = sum(1 for c in companies if (c.get("earnings_evidence") or {}).get("has_conference_call"))

    prompt = f"""You are Morning Signal's earnings strategist. Write a deep, evidence-first markdown earnings analysis.

Today: {target_date}
Session: {session_label}

Use only the provided context. Do not invent reported numbers, management quotes, conference-call commentary, or price reactions that are not present in the inputs.
If a company lacks actual result or conference-call evidence, label it "Awaiting reliable post-call source" and do not write a fake take.
This is not a calendar preview. It is a post-result/call earnings analysis where evidence exists.

MARKET CONTEXT
{json.dumps(market_context, indent=2)}

PRIMARY COMPANIES (all US-listed companies in this session with market cap >= $2B, sorted by market cap)
{json.dumps(companies, indent=2)}

OTHER >$2B REPORTERS TODAY / UNSPECIFIED SESSION WATCH LIST
{json.dumps(watchlist, indent=2)}

Evidence summary: {evidence_count} companies have actual-result evidence; {call_count} companies have conference-call/transcript evidence.

Return markdown with this structure:

# Earnings Deep Dive — {target_date} {session_label}

## Executive Summary
4-6 paragraphs. Start with the correct market tape from MARKET CONTEXT indices/sectors. Then synthesize what the actual earnings evidence says: beats/misses, revenue quality, margins, guidance, AI/data-center read-throughs, and after-hours reactions only where provided.

## Highest-Conviction Takeaways
5-8 numbered calls. Each call must name tickers, the specific evidence, and the second-order read-through.

## Company-by-Company Analysis
Add a section for every PRIMARY COMPANY that has actual-result evidence or conference-call evidence. Prioritize the top market-cap names first:
### TICKER — Company Name
- Reported result: EPS/revenue versus estimate where available.
- Quality of print: revenue mix, margins, cash flow, guidance, and balance-sheet signals.
- Conference call / management take: only if call evidence exists; otherwise say "Call evidence not yet available."
- Market reaction and interpretation: only if provided in evidence.
- Read-throughs: who else is impacted and why.
- Bottom line: one sentence on whether the print improves, confirms, or weakens the thesis.

## Awaiting Reliable Post-Call Source
Compact table for PRIMARY COMPANIES with no actual-result evidence yet. Columns: ticker, company, market cap, EPS forecast, fiscal quarter, status.

## Full Reporter Tape
A compact table listing every PRIMARY COMPANY ticker, company name, market cap, EPS forecast, fiscal quarter, actual EPS if available, actual revenue if available, and evidence status.

## Watch List For Tomorrow
Bullet list of follow-up items: transcript availability, guidance clarifications, read-through groups, analyst estimate revisions, and after-hours movers.

## Source Notes
Short note describing that this brief was generated from Nasdaq calendar data, yfinance actuals/market snapshots, RSS/news evidence, and available transcript/press-release evidence.

Every company in PRIMARY COMPANIES must appear at least once, either in Company-by-Company Analysis, Awaiting Reliable Post-Call Source, or Full Reporter Tape. Keep it analytical and publication-ready. Return markdown only."""

    print(f"  Generating {session_label.lower()} earnings brief via OpenAI GPT-5.4...")
    text = complete_text(prompt, max_output_tokens=16000)

    out_path = STATE_DIR / f"earnings_{target_date}_{session}.md"
    out_path.write_text(text.strip() + "\n", encoding="utf-8")
    print(f"  Saved earnings brief → {out_path}")
    return out_path


def get_earnings_list() -> list[dict]:
    briefs = []
    for md_file in sorted(STATE_DIR.glob("earnings_*.md"), reverse=True):
        parts = md_file.stem.split("_")
        if len(parts) >= 3:
            briefs.append({
                "date": parts[1],
                "session": parts[2],
                "filename": md_file.name,
                "size": md_file.stat().st_size,
            })
    return briefs


def sync_earnings(
    target_date: str | None = None,
    *,
    sessions: tuple[str, ...] = ("AM", "PM"),
    dry_run: bool = False,
    generate_missing: bool = True,
    regenerate: bool = False,
) -> list[dict]:
    target_date = target_date or date.today().isoformat()

    for session in sessions:
        out_path = STATE_DIR / f"earnings_{target_date}_{session}.md"
        if out_path.exists() and not regenerate:
            continue

        if not out_path.exists() and not generate_missing:
            continue

        if dry_run:
            action = "refresh" if out_path.exists() and regenerate else "generate"
            print(f"  [dry-run] Would {action} earnings brief {out_path.name}")
            continue

        if not openai_enabled():
            print("  OPENAI_API_KEY not set, skipping new earnings brief generation")
            break

        try:
            generate_earnings_brief(target_date, session)
        except Exception as exc:  # pragma: no cover - provider/network dependent
            print(f"  Warning: failed to generate {out_path.name}: {exc}")

    briefs = get_earnings_list()
    print(f"  Earnings library ready: {len(briefs)} briefs")
    return briefs


if __name__ == "__main__":
    sync_earnings()
