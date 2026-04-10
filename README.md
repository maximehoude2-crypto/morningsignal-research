# MorningSignal Research Platform

Fully automated daily market intelligence system. Scans the S&P 1500 universe for high-conviction breakout setups using a composite scoring model (IBD Relative Strength + Weinstein Stage Analysis + Minervini SEPA + trend structure), generates a static HTML website, and publishes it to `research.morningsignal.xyz` via Cloudflare Pages + GitHub.

## Project Structure

```
morningsignal-research/
├── scanner/
│   ├── indicators.py        # RS, base, trend, stage-2 scoring
│   ├── breakout_scanner.py  # Main scanner (S&P 1500 universe)
│   └── market_brief.py      # Index / sector / macro snapshot
├── site/
│   ├── generate_site.py     # Jinja2 → docs/ HTML builder
│   ├── templates/
│   │   ├── index.html.j2    # Homepage
│   │   ├── daily.html.j2    # Per-day report
│   │   └── deep_dive.html.j2
│   └── assets/
│       ├── style.css
│       └── logo.svg
├── newsletter/
│   └── weekly_digest.py     # Weekly email digest via Resend
├── deploy/
│   └── push_to_github.py    # Git commit + push to gh-pages
├── config/
│   └── advisors.json        # Advisor email list for newsletter
├── state/                   # Auto-created; holds daily JSON output
├── docs/                    # Auto-created; static site output
├── run_daily.py             # Daily orchestrator (steps 1-4)
├── run_weekly.py            # Weekly digest orchestrator
├── requirements.txt
├── .env.example
└── README.md
```

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/morningsignal-research
cd morningsignal-research
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env and add your RESEND_API_KEY
```

## GitHub repository

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/morningsignal-research.git
git push -u origin main
```

## Cloudflare Pages

1. **Cloudflare Dashboard → Pages → Create project → Connect to Git**
2. Select `morningsignal-research`
3. Build settings: leave build command blank, set output to `docs`
4. Deploy, then add custom domain `research.morningsignal.xyz`
5. DNS: CNAME `research → <project>.pages.dev`

Every `git push origin main` auto-triggers a deploy in ~30s.

## First run

```bash
# Dry run — mock data, real HTML output, no git push
python3 run_daily.py --dry-run

# Preview generated site
open docs/index.html

# Live run (after market close)
python3 run_daily.py
```

## Daily Pipeline (`run_daily.py`)

| Step | Module | Output |
|------|--------|--------|
| 1 | `scanner.market_brief` | `state/market_brief_YYYY-MM-DD.json` |
| 2 | `scanner.breakout_scanner` | `state/breakouts_YYYY-MM-DD.json` |
| 3 | `site.generate_site` | `docs/index.html`, `docs/daily/YYYY-MM-DD.html` |
| 4 | `deploy.push_to_github` | Pushes `docs/` to GitHub (skipped in dry-run) |

## Scoring Model

Each ticker is scored 0–100 across three dimensions:

- **Relative Strength (RS)** — price performance vs. SPY over 3m/6m/12m
- **Base Quality** — consolidation tightness, low volatility, proximity to 52w high
- **Trend** — 50/150/200-day MA alignment (Stage 2 filter)

Tickers must score ≥ 60 overall **and** pass the Stage 2 filter to appear.

## Scheduling with launchd (macOS)

**`~/Library/LaunchAgents/xyz.morningsignal.daily.plist`** — runs at 4:30 PM Mon–Fri:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>xyz.morningsignal.daily</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/max/morningsignal-research/.venv/bin/python3</string>
    <string>/Users/max/morningsignal-research/run_daily.py</string>
  </array>
  <key>StartCalendarInterval</key>
  <array>
    <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>16</integer><key>Minute</key><integer>30</integer></dict>
    <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>16</integer><key>Minute</key><integer>30</integer></dict>
    <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>16</integer><key>Minute</key><integer>30</integer></dict>
    <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>16</integer><key>Minute</key><integer>30</integer></dict>
    <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>16</integer><key>Minute</key><integer>30</integer></dict>
  </array>
  <key>StandardOutPath</key><string>/Users/max/morningsignal-research/logs/daily.log</string>
  <key>StandardErrorPath</key><string>/Users/max/morningsignal-research/logs/daily.err</string>
</dict>
</plist>
```

**`~/Library/LaunchAgents/xyz.morningsignal.weekly.plist`** — runs at 6 PM Fridays:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>xyz.morningsignal.weekly</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/max/morningsignal-research/.venv/bin/python3</string>
    <string>/Users/max/morningsignal-research/run_weekly.py</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Weekday</key><integer>5</integer>
    <key>Hour</key><integer>18</integer>
    <key>Minute</key><integer>0</integer>
  </dict>
  <key>StandardOutPath</key><string>/Users/max/morningsignal-research/logs/weekly.log</string>
  <key>StandardErrorPath</key><string>/Users/max/morningsignal-research/logs/weekly.err</string>
</dict>
</plist>
```

```bash
mkdir -p logs
launchctl load ~/Library/LaunchAgents/xyz.morningsignal.daily.plist
launchctl load ~/Library/LaunchAgents/xyz.morningsignal.weekly.plist
```

## Environment Variables (`.env`)

| Variable | Purpose |
|----------|---------|
| `GITHUB_TOKEN` | Push access to `docs/` branch |
| `GITHUB_REPO` | `owner/repo` slug |
| `RESEND_API_KEY` | Weekly newsletter emails |
| `NEWSLETTER_FROM` | Sender address |

## Output

- **`docs/index.html`** — today's homepage with market brief + top 15 breakouts
- **`docs/daily/YYYY-MM-DD.html`** — archived daily report
- **`docs/archive.html`** — index of all past reports
- **`docs/deep-dives/TICKER.html`** — deep-dive for 3+ consecutive-day appearances

## Not Financial Advice

This tool is for research and educational purposes only. Nothing here constitutes investment advice.
