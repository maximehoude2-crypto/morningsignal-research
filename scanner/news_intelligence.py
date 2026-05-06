"""
News Intelligence — pull, classify, and prioritise market-moving headlines.

Sources (all run on the user's Mac via the existing requests-based scrape):
  • CNBC market-insider + markets + earnings
  • Yahoo Finance topic feeds (stock-market, earnings, energy, tech, healthcare)
  • Reuters Business RSS
  • MarketWatch RSS (markets, top stories)
  • Investing.com RSS (markets, stock-market-news)
  • Seeking Alpha RSS (market currents)
  • AP / Reuters geopolitics RSS (limited list)

Each headline is tagged with:
  • themes:    ["Earnings", "AI/Tech", "Fed/Macro", "Geopolitics", "Energy/Oil",
                "China/Trade", "Healthcare", "Crypto", "M&A", "Regulation"]
  • sectors:   subset of {Technology, Financials, Energy, Health Care, Industrials,
                Materials, Real Estate, Utilities, Cons. Discretionary, Cons. Staples,
                Communication Svcs}
  • urgency:   1-5 (5 = market-moving event today)

Saves to state/news_YYYY-MM-DD.json.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).parent.parent
STATE_DIR = BASE_DIR / "state"
STATE_DIR.mkdir(exist_ok=True)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xml,application/rss+xml"}


# ---------------------------------------------------------------------------
# Theme + sector keyword taxonomies
# ---------------------------------------------------------------------------

THEME_KEYWORDS: dict[str, list[str]] = {
    "Earnings": [
        "earnings", "revenue", "eps", "guidance", "beat estimates", "missed estimates",
        "quarterly results", "reports earnings", "raised guidance", "lowered guidance",
        "outlook", "forecast", "beats", "misses", "Q1", "Q2", "Q3", "Q4",
    ],
    "AI/Tech": [
        "AI ", "artificial intelligence", "openai", "anthropic", "nvidia", "amd ",
        "gpu", "chip", "semiconductor", "chatgpt", "claude", "copilot", "gemini",
        "model", "llm", "data center", "cloud", "AWS", "azure", "GCP", "TSMC",
        "ASML", "broadcom", "marvell", "tesla autonomy", "humanoid", "robotaxi",
        "quantum", "agentic",
    ],
    "Fed/Macro": [
        "federal reserve", "fed ", "powell", "fomc", "rate cut", "rate hike",
        "rate decision", "inflation", "cpi", "ppi", "pce", "jobs report", "payrolls",
        "unemployment", "gdp", "recession", "soft landing", "yield", "treasury",
        "bond market", "bond yield", "ECB", "BOJ", "BOE", "lagarde", "ueda",
        "ism", "pmi", "retail sales", "consumer confidence", "economist",
    ],
    "Geopolitics": [
        "iran", "iranian", "israel", "gaza", "hamas", "hezbollah", "lebanon",
        "russia", "putin", "ukraine", "kyiv", "moscow",
        "north korea", "kim jong",
        "venezuela", "maduro",
        "sanctions", "tariff", "trade war",
        "houthi", "red sea", "saudi",
        "missile", "strike", "war", "ceasefire", "peace talks",
    ],
    "Energy/Oil": [
        "oil price", "wti", "brent", "crude", "opec", "natural gas", "lng",
        "saudi aramco", "exxon", "chevron", "refinery", "pipeline", "energy crisis",
        "drilling", "shale",
    ],
    "China/Trade": [
        "china", "beijing", "xi jinping", "taiwan", "hong kong", "shenzhen",
        "yuan", "renminbi", "tariff", "export controls", "chips act",
        "alibaba", "tencent", "byd", "tsmc",
    ],
    "Healthcare": [
        "fda", "drug approval", "biotech", "clinical trial", "pfizer", "merck",
        "eli lilly", "novo nordisk", "weight loss", "glp-1", "ozempic", "wegovy",
        "moderna", "vaccine", "medicare", "medicaid", "managed care", "unitedhealth",
        "cigna", "humana",
    ],
    "Crypto": [
        "bitcoin", "btc", "ethereum", "eth", "crypto", "stablecoin",
        "coinbase", "microstrategy", "blockchain", "spot etf",
    ],
    "M&A": [
        "merger", "acquires", "acquisition", "buyout", "takeover", "deal worth",
        "spinoff", "spin-off", "private equity", "leveraged buyout",
    ],
    "Regulation": [
        "doj", "antitrust", "ftc", "sec ", "lawsuit", "regulation", "regulator",
        "investigation", "settle", "settled", "fine ", "fined",
        "supreme court", "congress", "house bill", "senate",
    ],
    "Layoffs/Labor": [
        "layoff", "layoffs", "headcount", "job cuts", "fired", "strike", "union",
        "labor", "workforce reduction",
    ],
}


SECTOR_KEYWORDS: dict[str, list[str]] = {
    "Technology": [
        "nvidia", "amd", "intel", "tsmc", "asml", "broadcom", "marvell",
        "apple", "microsoft", "alphabet", "google", "meta", "amazon",
        "snowflake", "salesforce", "oracle", "ibm",
        "palantir", "crowdstrike", "zscaler", "datadog", "mongodb",
        "AI ", "semiconductor", "chip", "software", "saas", "cloud",
    ],
    "Financials": [
        "jpmorgan", "bank of america", "wells fargo", "citigroup", "goldman",
        "morgan stanley", "blackrock", "kkr", "blackstone", "apollo",
        "bank ", "credit card", "visa", "mastercard", "american express",
        "regional bank", "fintech", "interest rate",
    ],
    "Energy": [
        "exxon", "chevron", "occidental", "conocophillips", "shell", "bp ",
        "schlumberger", "halliburton", "oil ", "gas", "opec", "wti", "brent",
        "refinery", "pipeline", "lng",
    ],
    "Health Care": [
        "pfizer", "merck", "johnson & johnson", "eli lilly", "novo nordisk",
        "unitedhealth", "humana", "cigna", "elevance", "cvs",
        "biotech", "fda", "clinical trial", "drug", "pharma",
    ],
    "Industrials": [
        "boeing", "airbus", "lockheed", "raytheon", "general dynamics",
        "caterpillar", "deere", "ge ", "general electric", "honeywell",
        "fedex", "ups ", "union pacific",
        "defense", "aerospace", "machinery",
    ],
    "Materials": [
        "freeport", "newmont", "barrick", "alcoa", "nucor", "steel",
        "copper", "iron ore", "aluminum", "cement", "fertilizer",
    ],
    "Real Estate": [
        "real estate", "reit", "housing", "homebuilder", "lennar", "pulte",
        "office", "data center reit", "mall", "self storage",
    ],
    "Utilities": [
        "utility", "utilities", "nextera", "southern company", "duke energy",
        "electric grid", "power generation",
    ],
    "Cons. Discretionary": [
        "tesla", "amazon", "home depot", "lowe", "starbucks", "mcdonald",
        "nike", "lululemon", "booking", "marriott", "hilton",
        "auto", "automaker", "ford", "gm ", "rivian", "lucid",
    ],
    "Cons. Staples": [
        "walmart", "costco", "target", "kroger", "procter", "coca-cola", "pepsi",
        "general mills", "kellogg", "philip morris", "altria",
    ],
    "Communication Svcs": [
        "meta", "alphabet", "google", "netflix", "disney", "warner bros",
        "comcast", "at&t", "verizon", "t-mobile", "spotify",
        "advertising", "streaming",
    ],
}


# Urgency keywords boost a headline's score
URGENCY_BOOSTS: dict[str, int] = {
    "breaking": 5, "live": 4, "exclusive": 3,
    "halt": 4, "halted": 4, "plunge": 3, "soar": 3, "surge": 3,
    "crash": 4, "rally": 2, "sells off": 3, "spike": 3,
    "downgraded": 3, "upgraded": 3, "cut to": 3, "raised to": 3,
    "guidance": 3, "preannounced": 4, "warning": 3,
    "ceo": 2, "cfo": 2, "resigns": 4, "fired": 4,
    "investigation": 3, "subpoena": 4, "doj ": 4, "ftc": 3, "sec ": 3,
    "approved": 3, "rejected": 4,
    "opec": 4, "fomc": 5, "fed ": 3,
}


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

HTML_SOURCES = [
    {"label": "CNBC",        "url": "https://www.cnbc.com/market-insider/",   "selectors": [".Card-titleAndFooter a", ".Card-title a", "a.Card-title", "a"]},
    {"label": "CNBC",        "url": "https://www.cnbc.com/markets/",          "selectors": [".Card-titleAndFooter a", ".Card-title a", "a"]},
    {"label": "CNBC",        "url": "https://www.cnbc.com/earnings/",         "selectors": [".Card-titleAndFooter a", ".Card-title a", "a"]},
    {"label": "Yahoo",       "url": "https://finance.yahoo.com/topic/stock-market-news/", "selectors": ["h3 a", "a"]},
    {"label": "Yahoo",       "url": "https://finance.yahoo.com/topic/earnings/",          "selectors": ["h3 a", "a"]},
    {"label": "Yahoo",       "url": "https://finance.yahoo.com/topic/economic-news/",     "selectors": ["h3 a", "a"]},
    {"label": "MarketWatch", "url": "https://www.marketwatch.com/markets",                "selectors": ["h3 a", "a.link"]},
    {"label": "MarketWatch", "url": "https://www.marketwatch.com/economy-politics",       "selectors": ["h3 a", "a.link"]},
]

RSS_SOURCES = [
    {"label": "Reuters Business",       "url": "https://feeds.reuters.com/reuters/businessNews"},
    {"label": "Reuters Markets",        "url": "https://feeds.reuters.com/reuters/USmarketsNews"},
    {"label": "MarketWatch Top",        "url": "https://feeds.marketwatch.com/marketwatch/topstories/"},
    {"label": "Seeking Alpha Markets",  "url": "https://seekingalpha.com/market_currents.xml"},
    {"label": "Investing.com Markets",  "url": "https://www.investing.com/rss/news_25.rss"},
    {"label": "Investing.com Stocks",   "url": "https://www.investing.com/rss/news_301.rss"},
    {"label": "AP Top",                 "url": "https://feeds.apnews.com/rss/apf-topnews"},
    {"label": "AP Business",            "url": "https://feeds.apnews.com/rss/apf-business"},
]


# ---------------------------------------------------------------------------
# Scraping helpers
# ---------------------------------------------------------------------------

def _safe_get(url: str, timeout: int = 8) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


def _scrape_html(source: dict) -> list[dict]:
    text = _safe_get(source["url"])
    if not text:
        return []
    soup = BeautifulSoup(text, "html.parser")
    candidates = []
    for sel in source["selectors"]:
        for a in soup.select(sel):
            t = (a.get_text() or "").strip()
            href = a.get("href", "")
            if not t or len(t) < 25 or len(t) > 250:
                continue
            if href and href.startswith("/"):
                base = "/".join(source["url"].split("/")[:3])
                href = base + href
            elif href and not href.startswith("http"):
                continue
            candidates.append({"title": t, "url": href, "source": source["label"]})
    return candidates


def _scrape_rss(source: dict) -> list[dict]:
    text = _safe_get(source["url"])
    if not text:
        return []
    out = []
    try:
        root = ET.fromstring(text)
        # Navigate either RSS (channel/item) or Atom (entry)
        for item in root.iter():
            tag = item.tag.lower().split("}")[-1]
            if tag not in ("item", "entry"):
                continue
            title = ""
            link = ""
            pub = ""
            for child in item:
                ctag = child.tag.lower().split("}")[-1]
                if ctag == "title":
                    title = (child.text or "").strip()
                elif ctag == "link":
                    link = (child.attrib.get("href") or child.text or "").strip()
                elif ctag in ("pubdate", "published", "updated"):
                    pub = (child.text or "").strip()
            if title and 25 <= len(title) <= 280:
                out.append({
                    "title": title,
                    "url": link,
                    "source": source["label"],
                    "published": pub,
                })
    except ET.ParseError:
        pass
    return out


def _all_headlines() -> list[dict]:
    all_items: list[dict] = []
    for s in HTML_SOURCES:
        try:
            items = _scrape_html(s)
            all_items.extend(items)
        except Exception:
            pass
    for s in RSS_SOURCES:
        try:
            items = _scrape_rss(s)
            all_items.extend(items)
        except Exception:
            pass
    # Dedupe by lowercased first 70 chars
    seen = set()
    unique = []
    for it in all_items:
        key = it["title"].lower()[:70]
        if key in seen:
            continue
        seen.add(key)
        unique.append(it)
    return unique


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

def _match_keywords(text: str, keywords: list[str]) -> list[str]:
    t = text.lower()
    return [k for k in keywords if k.lower() in t]


def _tag(headline: dict) -> dict:
    title = headline["title"]
    themes = []
    for theme, kws in THEME_KEYWORDS.items():
        if _match_keywords(title, kws):
            themes.append(theme)
    sectors = []
    for sec, kws in SECTOR_KEYWORDS.items():
        if _match_keywords(title, kws):
            sectors.append(sec)
    urgency = 1
    t_low = title.lower()
    for kw, boost in URGENCY_BOOSTS.items():
        if kw in t_low:
            urgency = max(urgency, boost)
    headline["themes"] = themes
    headline["sectors"] = sectors
    headline["urgency"] = urgency
    return headline


# ---------------------------------------------------------------------------
# Earnings recap (uses yfinance)
# ---------------------------------------------------------------------------

EARNINGS_WATCHLIST = [
    # mega caps & key reporters — extend over time
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA",
    "AVGO", "ORCL", "ADBE", "CRM", "NOW", "INTU", "AMD", "TSM",
    "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK",
    "UNH", "LLY", "JNJ", "PFE", "MRK", "ABBV",
    "WMT", "COST", "PG", "KO", "PEP", "MCD", "NKE",
    "XOM", "CVX", "COP", "SLB",
    "BA", "CAT", "HON", "GE",
    "DIS", "NFLX", "CMCSA", "VZ", "T",
    "HD", "LOW", "TGT", "BKNG",
]


def _earnings_today(target_dt: date, dry_run: bool = False) -> list[dict]:
    if dry_run:
        return []
    out = []
    try:
        import yfinance as yf
    except ImportError:
        return []
    yesterday = target_dt - timedelta(days=1)
    for ticker in EARNINGS_WATCHLIST:
        try:
            tk = yf.Ticker(ticker)
            cal = tk.calendar
            if cal is None:
                continue
            # Calendar can be DataFrame or dict depending on yfinance version
            ed = None
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date") or cal.get("earningsDate")
            else:
                try:
                    ed = cal.loc["Earnings Date"].iloc[0] if "Earnings Date" in cal.index else None
                except Exception:
                    ed = None
            if not ed:
                continue
            if isinstance(ed, list):
                ed = ed[0] if ed else None
            if hasattr(ed, "date"):
                ed = ed.date()
            if ed not in (target_dt, yesterday):
                continue
            info = tk.info
            row = {
                "ticker": ticker,
                "name": info.get("shortName", ticker),
                "sector": info.get("sector", "Unknown"),
                "earnings_date": ed.isoformat() if hasattr(ed, "isoformat") else str(ed),
            }
            # Add EPS surprise if available
            try:
                hist = tk.earnings_history
                if hist is not None and not hist.empty:
                    row["surprise_pct"] = float(hist.iloc[-1].get("Surprise(%)", 0) or 0)
            except Exception:
                pass
            out.append(row)
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _bucket_by_theme(items: list[dict]) -> dict[str, list[dict]]:
    buckets: dict[str, list[dict]] = {}
    untagged: list[dict] = []
    for it in items:
        if not it["themes"]:
            untagged.append(it)
            continue
        for th in it["themes"]:
            buckets.setdefault(th, []).append(it)
    if untagged:
        buckets["General"] = untagged
    # Sort each bucket by urgency desc
    for k in buckets:
        buckets[k].sort(key=lambda x: x["urgency"], reverse=True)
    return buckets


def _bucket_by_sector(items: list[dict]) -> dict[str, list[dict]]:
    buckets: dict[str, list[dict]] = {}
    for it in items:
        for sec in it["sectors"]:
            buckets.setdefault(sec, []).append(it)
    for k in buckets:
        buckets[k].sort(key=lambda x: x["urgency"], reverse=True)
    return buckets


def _resolve_target_date(target_date: str | date | None) -> date:
    if target_date is None:
        return date.today()
    if isinstance(target_date, date):
        return target_date
    return datetime.strptime(target_date, "%Y-%m-%d").date()


def run_news_intelligence(
    dry_run: bool = False,
    target_date: str | date | None = None,
) -> dict:
    target_dt = _resolve_target_date(target_date)
    today = target_dt.isoformat()
    out_path = STATE_DIR / f"news_{today}.json"

    if dry_run:
        payload = {"date": today, "generated_at": datetime.now().isoformat(timespec="seconds"),
                   "headlines": [], "by_theme": {}, "by_sector": {}, "earnings": [],
                   "total": 0, "sources": []}
        out_path = STATE_DIR / "dry_run" / f"news_{today}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2, default=str))
        return payload

    print("Pulling headlines from financial news sources...")
    raw = _all_headlines()
    print(f"  {len(raw)} unique headlines retrieved.")

    print("Tagging headlines with themes and sectors...")
    tagged = [_tag(h) for h in raw]

    # Filter out junk: must have at least one theme OR a sector OR urgency >= 2
    relevant = [h for h in tagged if h["themes"] or h["sectors"] or h["urgency"] >= 3]
    relevant.sort(key=lambda x: x["urgency"], reverse=True)

    by_theme = _bucket_by_theme(relevant)
    by_sector = _bucket_by_sector(relevant)

    print(f"  {len(relevant)} relevant headlines kept; "
          f"{sum(len(v) for v in by_theme.values())} theme-tagged.")

    print("Pulling today's earnings reporters from watchlist...")
    earnings = _earnings_today(target_dt, dry_run=dry_run)
    print(f"  {len(earnings)} watchlist names report today/yesterday.")

    payload = {
        "date": today,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total": len(relevant),
        "sources": sorted({h["source"] for h in relevant}),
        "headlines": relevant[:100],            # cap to top 100 by urgency
        "by_theme":  {k: v[:12] for k, v in by_theme.items()},   # top 12 per theme
        "by_sector": {k: v[:8]  for k, v in by_sector.items()},  # top 8 per sector
        "earnings": earnings,
    }
    out_path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Saved news intelligence → {out_path.relative_to(BASE_DIR)}")
    print(f"  Themes covered: {', '.join(sorted(by_theme.keys()))}")
    return payload


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    run_news_intelligence(dry_run=dry)
