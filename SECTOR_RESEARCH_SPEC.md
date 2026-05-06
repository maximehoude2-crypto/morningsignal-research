# Sector Research System — Phase 1 Build Spec
## For Claude Code — Autonomous Build Instructions

---

## OBJECTIVE

Build a fully automated daily sector stock-picking research system at `/Users/max/sector-research/`. The system identifies 5–10 US equities (market cap >$2B) per GICS sector with the highest probability of outperforming their sector ETF over the following 9 months. It outputs a hedge-fund-grade deep dive report per stock and integrates into the MorningSignal website at `/Users/max/morningsignal-research/`.

This is NOT a screener. It is an autonomous research agent that reads, thinks, weighs evidence, and produces institutional-quality investment theses with specific price targets and expected returns.

---

## PROJECT STRUCTURE

```
/Users/max/sector-research/
├── run_sector.py              # Main orchestrator
├── config.py                  # Sector rotation, ETF mappings, constants
├── research/
│   ├── universe.py            # Stock universe discovery per sector
│   ├── screener.py            # Quantitative pre-screening (financials + momentum)
│   ├── analyst.py             # Deep dive research agent (Gemini 2.5 Flash)
│   ├── valuation.py           # DCF + relative valuation engine
│   ├── momentum.py            # Technical/momentum scoring
│   └── sector_playbooks.py    # Sector-specific KPI frameworks
├── portfolio/
│   ├── constructor.py         # Selects 5-10 best names, builds portfolio
│   └── scorer.py              # Cross-stock ranking and conviction scoring
├── output/
│   ├── report_generator.py    # Generates markdown deep dive reports
│   └── site_generator.py      # HTML for MorningSignal integration
├── state/
│   ├── picks_YYYY-MM-DD_SECTOR.json    # Daily picks with entry prices
│   └── reports/                         # Generated report markdown files
├── logs/
│   └── sector_research.log
├── requirements.txt
├── .env
└── README.md
```

---

## SECTOR ROTATION LOGIC

### The 10 Sectors (GICS, REITs excluded)

```python
SECTORS = [
    {"name": "Information Technology", "etf": "XLK",  "day_mod": 0},
    {"name": "Financials",             "etf": "XLF",  "day_mod": 1},
    {"name": "Health Care",            "etf": "XLV",  "day_mod": 2},
    {"name": "Consumer Discretionary", "etf": "XLY",  "day_mod": 3},
    {"name": "Consumer Staples",       "etf": "XLP",  "day_mod": 4},
    {"name": "Industrials",            "etf": "XLI",  "day_mod": 5},
    {"name": "Materials",              "etf": "XLB",  "day_mod": 6},
    {"name": "Energy",                 "etf": "XLE",  "day_mod": 7},
    {"name": "Communication Services", "etf": "XLC",  "day_mod": 8},
    {"name": "Utilities",              "etf": "XLU",  "day_mod": 9},
]
```

`today_sector = SECTORS[date.today().toordinal() % 10]`

Each sector is analyzed once every 10 calendar days. The report replaces the prior report for that sector.

---

## STEP 1 — STOCK UNIVERSE DISCOVERY

**File:** `research/universe.py`

Use the existing MorningSignal breakout scanner's Wikipedia fetch as a starting point, but filter by GICS sector using yfinance.

```python
def get_sector_universe(sector_name: str, min_market_cap_b: float = 2.0) -> list[dict]:
    """
    Returns list of {ticker, name, market_cap, sector} dicts for the given sector.
    Min market cap in billions.
    """
```

**Implementation:**
1. Fetch S&P 500 + S&P 400 tickers from Wikipedia (using User-Agent headers as in breakout_scanner.py)
2. For each ticker, use `yf.Ticker(t).info` to get `sector`, `marketCap`, `longName`
3. Filter: sector matches AND marketCap >= min_market_cap_b * 1e9
4. Cache results in `state/universe_{sector}_{date}.json` (refresh daily)
5. Target: 30–80 candidates per sector

**Fallback:** If Wikipedia fails, use a hardcoded seed list of the top 20 names per sector by market cap (embed this in config.py).

---

## STEP 2 — QUANTITATIVE PRE-SCREENING

**File:** `research/screener.py`

Narrow the universe to the top 20 candidates before doing expensive LLM research. This is pure quant — no LLM involved.

### Metrics to fetch via yfinance for each ticker:

```python
QUANT_FIELDS = [
    # Valuation
    "trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailingTwelveMonths",
    "enterpriseToEbitda", "enterpriseToRevenue",
    # Growth
    "revenueGrowth", "earningsGrowth", "earningsQuarterlyGrowth",
    # Quality
    "returnOnEquity", "returnOnAssets", "grossMargins", "operatingMargins", "profitMargins",
    # Balance sheet
    "debtToEquity", "currentRatio", "quickRatio", "totalCash", "totalDebt",
    # Momentum (compute from price history)
    # "momentum_3m", "momentum_6m", "momentum_12m", "rs_score" — computed separately
    # Analyst
    "recommendationMean", "numberOfAnalystOpinions", "targetMeanPrice", "currentPrice",
]
```

### Momentum score (compute via price history):
```python
def momentum_score(ticker: str) -> float:
    hist = yf.Ticker(ticker).history(period="1y")["Close"]
    m3  = (hist.iloc[-1] / hist.iloc[-63] - 1)   # 3-month
    m6  = (hist.iloc[-1] / hist.iloc[-126] - 1)  # 6-month
    m12 = (hist.iloc[-1] / hist.iloc[-252] - 1)  # 12-month (skip last month)
    return m3 * 0.4 + m6 * 0.35 + (m12 - (hist.iloc[-1]/hist.iloc[-21]-1)) * 0.25
```

### Composite pre-screen score (sector-agnostic):
Weight: 40% momentum, 30% quality (ROE, margins), 20% growth, 10% analyst consensus

Select top 20 by composite score. These 20 go to the deep dive phase.

---

## STEP 3 — SECTOR PLAYBOOKS

**File:** `research/sector_playbooks.py`

Each sector has a specific research framework. These are embedded as structured prompts that tell the analyst agent what to look for, what metrics matter most, and what the key risk factors are. Build from first principles — what do the best sector specialists actually look at?

```python
SECTOR_PLAYBOOKS = {

    "Information Technology": {
        "primary_metrics": [
            "Revenue growth rate (YoY and QoQ acceleration)",
            "Rule of 40 score (revenue growth % + FCF margin %)",
            "Gross margin trend and expansion",
            "FCF margin and conversion rate",
            "R&D as % of revenue (investment intensity)",
            "NRR/NTM ARR for SaaS names",
            "TAM size and penetration rate",
        ],
        "valuation_framework": "EV/NTM Revenue primary, EV/NTM FCF secondary. Compare to growth-adjusted peers (PEG on FCF). DCF with 3-stage model: high growth (yr 1-3), deceleration (yr 4-7), terminal.",
        "key_risks": ["Multiple compression on rate cycle", "Competition from hyperscalers", "AI disruption of legacy software", "Customer concentration"],
        "outperformance_drivers": ["Earnings beat + raise cadence", "AI product monetization", "Platform expansion TAM", "Margin inflection story"],
        "avoid": ["Negative FCF with decelerating growth", "Single-product companies losing pricing power"],
    },

    "Financials": {
        "primary_metrics": [
            "Net Interest Margin (NIM) trend and rate sensitivity",
            "Return on Equity (ROE) vs cost of equity",
            "Return on Assets (ROA)",
            "Efficiency ratio (lower is better)",
            "Loan growth rate and mix",
            "Net charge-off rate and coverage ratio",
            "CET1 capital ratio and buyback capacity",
            "Book value per share growth",
        ],
        "valuation_framework": "P/TBV primary for banks (compare to ROE), P/E secondary. For insurers: P/BV, combined ratio. For asset managers: P/AUM, fee rate trends.",
        "key_risks": ["Credit cycle turning", "Yield curve shape", "Regulatory capital requirements", "Loan loss provisions"],
        "outperformance_drivers": ["NIM expansion in rising rate environment", "Reserve release", "M&A accretion", "Capital return above peers"],
        "avoid": ["High commercial real estate exposure", "Negative operating leverage", "Regulatory overhang"],
    },

    "Health Care": {
        "primary_metrics": [
            "Pipeline value (number of Phase 2/3 assets, addressable patient population)",
            "Patent cliff exposure (% of revenue at risk in next 5 years)",
            "FDA catalyst calendar (upcoming PDUFA dates)",
            "Reimbursement risk (Medicare/Medicaid exposure)",
            "R&D productivity (NMEs per R&D dollar)",
            "Same-facility volume growth for services",
            "Managed care medical loss ratio",
        ],
        "valuation_framework": "rNPV for pipeline companies. P/E and EV/EBITDA for large cap pharma/devices. Sum-of-parts for diversified healthcare. DCF with probability-weighted pipeline.",
        "key_risks": ["Drug pricing legislation", "Clinical trial failure", "Biosimilar competition", "Reimbursement cuts"],
        "outperformance_drivers": ["Positive Phase 3 readout", "FDA approval", "Beat-and-raise on earnings", "Acquisition premium"],
        "avoid": ["Single-asset biotechs below $2B", "Companies with >30% revenue at patent cliff within 3 years without pipeline offset"],
    },

    "Consumer Discretionary": {
        "primary_metrics": [
            "Same-store sales growth (SSS) — volume vs price mix",
            "Gross margin trend (input cost, freight, promotions)",
            "Inventory turnover and days inventory outstanding",
            "Digital penetration and e-commerce growth",
            "Customer acquisition cost vs lifetime value",
            "Unit economics for new store/concept openings",
            "Consumer confidence sensitivity (beta to sentiment)",
        ],
        "valuation_framework": "EV/EBITDA primary, P/E secondary. Brand-owning companies at premium. Retailers at discount to asset-light peers. FCF yield for mature businesses.",
        "key_risks": ["Consumer spending slowdown", "Input cost inflation", "Inventory glut", "Amazon/DTC disruption"],
        "outperformance_drivers": ["SSS reacceleration", "Margin recovery from input cost normalization", "International expansion", "Share gains from weak competitors"],
        "avoid": ["Negative SSS trends with inventory build", "Highly levered balance sheets in a softening consumer environment"],
    },

    "Consumer Staples": {
        "primary_metrics": [
            "Organic revenue growth (price + volume decomposition)",
            "Pricing power evidence (ability to pass through costs)",
            "Volume trends (negative volume with price = demand destruction)",
            "Gross margin recovery post-inflation",
            "Emerging market revenue exposure and growth",
            "Private label penetration risk",
            "Free cash flow conversion and dividend coverage",
        ],
        "valuation_framework": "P/E and EV/EBITDA vs 5-year historical average. Dividend yield vs 10Y treasury spread. DCF with steady-state growth assumptions (2-4% organic).",
        "key_risks": ["Volume destruction from pricing", "Private label share gains", "FX headwinds on EM exposure", "Input cost re-inflation"],
        "outperformance_drivers": ["Volume recovery + margin expansion simultaneously", "Portfolio pruning/premiumization", "EM market share gains", "Cost savings programs"],
        "avoid": ["Continued negative volume alongside elevated valuation", "Dividend payout ratio >85% with debt"],
    },

    "Industrials": {
        "primary_metrics": [
            "Order backlog level and book-to-bill ratio",
            "Organic revenue growth (ex-FX, ex-M&A)",
            "EBITDA margin trajectory and incremental margins",
            "Free cash flow conversion (FCF/net income)",
            "Capital allocation: M&A vs buyback vs dividend",
            "End-market exposure (aerospace, defense, auto, construction)",
            "Pricing vs cost spread",
        ],
        "valuation_framework": "EV/EBITDA primary with cycle-adjusted normalization. P/E secondary. FCF yield. Defense contractors at premium for revenue visibility.",
        "key_risks": ["Capex cycle slowdown", "Supply chain disruption", "Labor cost inflation", "End-market destocking"],
        "outperformance_drivers": ["Backlog conversion acceleration", "Margin inflection from restructuring", "Defense spending increase", "Reshoring/nearshoring beneficiary"],
        "avoid": ["Companies with declining book-to-bill and inventory build", "High auto/housing cyclical exposure in a slowdown"],
    },

    "Materials": {
        "primary_metrics": [
            "Commodity price cycle position and exposure",
            "Capacity utilization rates (industry and company)",
            "Cost curve position (are they low-cost producer?)",
            "Inventory levels vs demand",
            "Capital return yield (FCF yield + dividend)",
            "M&A consolidation activity in sector",
            "China demand sensitivity",
        ],
        "valuation_framework": "EV/EBITDA at mid-cycle commodity prices. P/NAV for miners. Normalized FCF yield. Replacement cost for asset-intensive businesses.",
        "key_risks": ["Commodity price collapse", "China demand disappointment", "Cost inflation (energy, labor)", "New supply coming online"],
        "outperformance_drivers": ["Commodity price spike", "Supply discipline in the industry", "M&A premium", "Cost reduction program"],
        "avoid": ["Companies priced for peak commodity cycle", "High-cost producers with leverage"],
    },

    "Energy": {
        "primary_metrics": [
            "Free cash flow yield at $65/70/75 WTI (stress test)",
            "Reserve replacement ratio (>100% = growing reserves)",
            "Production growth rate (organic)",
            "Breakeven oil price (lower is better)",
            "Dividend + buyback yield (total capital return)",
            "Debt/EBITDA at current prices",
            "Leverage to commodity price (beta analysis)",
        ],
        "valuation_framework": "EV/EBITDA at strip prices and at $65 WTI (downside case). FCF yield primary. NAV for E&P companies. Sum-of-parts for integrated majors.",
        "key_risks": ["Oil price collapse", "OPEC+ production increase", "Energy transition / stranded assets", "Geopolitical supply disruption (two-way risk)"],
        "outperformance_drivers": ["Supply disruption spike", "Buyback acceleration", "Reserve upgrade", "M&A consolidation premium"],
        "avoid": ["High breakeven companies (>$65 WTI) with leverage", "Companies with reserve replacement <80%"],
    },

    "Communication Services": {
        "primary_metrics": [
            "ARPU trend (average revenue per user)",
            "Subscriber/user growth and churn rate",
            "Advertising revenue growth (digital ad market share)",
            "Content investment ROI (streaming services)",
            "Broadband/wireless market share trajectory",
            "FCF after content spend",
            "Net debt/EBITDA and deleveraging path",
        ],
        "valuation_framework": "EV/EBITDA for telcos and cable. EV/EBITDA + P/E for media. Revenue multiple for high-growth digital platforms. FCF yield for mature names.",
        "key_risks": ["Streaming content cost inflation", "Digital ad market slowdown", "Cord-cutting acceleration", "Competition from tech giants in advertising"],
        "outperformance_drivers": ["Streaming profitability inflection", "Digital ad market share gain", "Broadband subscriber acceleration", "Debt paydown re-rates stock"],
        "avoid": ["Linear TV overexposure with declining subscribers", "Streaming with no path to profitability"],
    },

    "Utilities": {
        "primary_metrics": [
            "Rate base growth (higher = more regulated earnings power)",
            "Earned ROE vs allowed ROE (efficiency of regulatory compact)",
            "Renewable energy capex plan (growth driver)",
            "Dividend yield and payout sustainability",
            "Balance sheet: FFO/debt ratio (target >15%)",
            "Regulatory jurisdiction quality (constructive vs adversarial)",
            "Data center power demand exposure (new growth driver)",
        ],
        "valuation_framework": "P/E and EV/EBITDA. Dividend yield vs 10Y treasury spread (historically 150-200bps premium). Rate base growth drives long-term NAV.",
        "key_risks": ["Rising interest rates compress valuations", "Regulatory rate case denial", "Wildfire liability (for western utilities)", "Capex cost overruns"],
        "outperformance_drivers": ["Data center power contract wins", "Rate case approval above expectations", "Dividend increase", "Interest rate decline"],
        "avoid": ["Utilities in adversarial regulatory states with rising debt", "Names trading at <100bps spread to treasury with no growth"],
    },
}
```

---

## STEP 4 — DEEP DIVE RESEARCH AGENT

**File:** `research/analyst.py`

This is the core of the system. For each of the top 20 screened stocks, run a structured research agent using **Gemini 2.5 Flash** (free tier, key in `.env` as `GEMINI_API_KEY`).

### Research Agent Prompt Structure

```python
DEEP_DIVE_PROMPT = """
You are a senior equity analyst at a top-tier hedge fund. You are conducting a deep dive on {ticker} ({company_name}) in the {sector} sector.

Today's date: {today}
Current stock price: ${current_price}
Market cap: ${market_cap_b:.1f}B
Sector ETF: {sector_etf} (current price: ${etf_price})

## SECTOR FRAMEWORK FOR {sector}

Primary metrics to evaluate:
{sector_primary_metrics}

Valuation approach: {sector_valuation_framework}

Key outperformance drivers: {sector_outperformance_drivers}

Key risks: {sector_key_risks}

## QUANTITATIVE DATA (from yfinance)

{quant_data_table}

## YOUR TASK

Conduct a complete institutional-grade equity analysis. You must produce ALL of the following sections:

### 1. INVESTMENT THESIS (3-5 sentences)
The single most important reason to own this stock over the next 9 months. Be specific — reference recent earnings, product cycles, management actions, or macro tailwinds. Do NOT write generic statements.

### 2. BUSINESS OVERVIEW (2-3 paragraphs)
What does this company actually do? What is the competitive moat? What is the market structure? Be specific about the revenue model and what drives unit economics.

### 3. SECTOR-SPECIFIC KPI ANALYSIS
Evaluate each of the following sector-specific KPIs based on the quantitative data provided and your knowledge of recent developments:
{sector_primary_metrics_list}

For each KPI: current reading, trend (improving/deteriorating/stable), and verdict (positive/negative/neutral for the thesis).

### 4. FINANCIAL DEEP DIVE

#### Revenue Analysis
- TTM revenue and YoY growth rate
- Revenue trajectory: accelerating, decelerating, or stable?
- Revenue quality: recurring vs one-time, geographic mix, customer concentration

#### Margin Analysis
- Gross margin: current level, trend, peers comparison
- EBITDA margin: current level, direction, drivers
- FCF margin and conversion rate (FCF/net income)

#### Balance Sheet
- Net debt position and debt/EBITDA
- Interest coverage ratio
- Capital allocation priorities (capex, buybacks, dividends, M&A)

### 5. DCF VALUATION

Use a 3-stage DCF model:
- **Stage 1 (Years 1-3):** Base case revenue growth rate, margin assumptions
- **Stage 2 (Years 4-7):** Deceleration toward sector average growth
- **Stage 3 (Terminal):** 2.5% terminal growth rate
- **WACC:** Calculate appropriate WACC based on company's beta and capital structure (use 9-11% range for most equities)

Provide THREE scenarios:
| Scenario | Revenue CAGR (Y1-3) | EBITDA Margin | Fair Value | Upside/Downside |
|----------|---------------------|---------------|------------|-----------------|
| Bull     |                     |               |            |                 |
| Base     |                     |               |            |                 |
| Bear     |                     |               |            |                 |

### 6. RELATIVE VALUATION

Compare current valuation multiples to:
a) 3-year historical average for this stock
b) Top 5 sector peers (name them)
c) Sector ETF implied blended multiple

Conclude: Is the stock cheap, fair, or expensive relative to history and peers? By how much?

### 7. MOMENTUM & TECHNICAL ASSESSMENT

Evaluate the following:
- Price momentum: 3-month, 6-month, 12-month returns vs sector ETF
- Trend structure: Is the stock in a Weinstein Stage 2 uptrend (price above rising 30-week MA)?
- Volume pattern: Is accumulation or distribution visible?
- Upcoming catalysts: Earnings date, product launches, analyst days, FDA dates, etc.

### 8. RISK ASSESSMENT

List the top 3 risks to the thesis. For each risk:
- What is it?
- How likely is it?
- What would the downside be if it materializes?
- What would confirm or deny this risk?

### 9. EXPECTED RETURN ANALYSIS

**Over 9 months:**
- Base case absolute return: ___% 
- Base case alpha vs {sector_etf}: ___% (this is the primary metric)
- Bull case alpha: ___%
- Bear case alpha: ___%

Explain your reasoning for the 9-month expected alpha clearly and specifically.

### 10. CONVICTION RATING

Rate: HIGH / MEDIUM / LOW

Criteria:
- HIGH: Strong thesis, multiple catalysts, valuation support, positive momentum. Expected alpha >10% in base case.
- MEDIUM: Good thesis but 1-2 meaningful risks, or valuation less compelling. Expected alpha 5-10%.
- LOW: Speculative thesis or significant execution risk. Expected alpha <5% or binary.

### 11. POSITION SIZING RECOMMENDATION

For a 10-stock portfolio, recommend:
- Suggested portfolio weight: ___%
- Rationale for sizing

---

Write in the voice of a senior hedge fund analyst: direct, quantitative, specific. Use actual numbers throughout. Avoid generic language like "the company is well-positioned." Reference specific recent events (last 2 quarters of earnings, recent news). Challenge your own thesis.
"""
```

### Implementation notes:
- Run this prompt for all 20 screened candidates
- Use `google.generativeai` with model `gemini-2.5-flash`
- Set temperature to 0.3 for consistency
- Parse the output sections programmatically using regex/string matching
- Cache results in `state/deep_dives/{ticker}_{date}.json`
- If Gemini quota is hit, fall back to top 10 candidates only

---

## STEP 5 — PORTFOLIO CONSTRUCTION

**File:** `portfolio/constructor.py`

After the 20 deep dives, select the best 5-10 names.

### Scoring rubric (0-100 total):

```python
def portfolio_score(deep_dive: dict) -> float:
    score = 0
    # Expected alpha vs ETF (base case) — primary metric — 40 pts max
    alpha = deep_dive["expected_alpha_base"]
    score += min(40, alpha * 2)  # 20% alpha = 40 pts
    
    # Conviction rating — 25 pts
    conviction_map = {"HIGH": 25, "MEDIUM": 15, "LOW": 5}
    score += conviction_map[deep_dive["conviction"]]
    
    # Valuation (cheap vs peers/history) — 20 pts
    valuation_map = {"cheap": 20, "fair": 10, "expensive": 0}
    score += valuation_map[deep_dive["relative_valuation"]]
    
    # Momentum (positive vs sector) — 15 pts
    momentum = deep_dive["momentum_score"]  # -1 to 1
    score += max(0, momentum * 15)
    
    return score
```

### Portfolio construction rules:
1. Rank all 20 by portfolio_score
2. Select top N where N is between 5 and 10
3. Enforce sector diversification within the picks (no more than 3 from same sub-sector)
4. Size positions by conviction: HIGH = 15% weight, MEDIUM = 10%, LOW = 7%
5. Normalize weights to sum to 100%

---

## STEP 6 — REPORT GENERATION

**File:** `output/report_generator.py`

Generate two outputs per sector run:

### A. Full Research Report (Markdown)

**File:** `state/reports/sector_{sector}_{date}.md`

Structure:
```
# {Sector} Sector Deep Dive — {Date}
## Portfolio Summary (5-10 picks with one-line thesis and expected alpha)
## Sector Macro Backdrop (2-3 paragraphs on sector-level tailwinds/headwinds)
## Individual Stock Deep Dives (full analysis for each pick)
## Discarded Names (top 3 screened-out candidates with one-line reason)
## Risk Scenario Analysis
```

### B. MorningSignal HTML Section

**File:** `/Users/max/morningsignal-research/docs/sectors/{sector_slug}.html`

Template (Jinja2, matching the existing MorningSignal dark navy/green style):

```html
<!-- Sector Picks Card -->
<div class="sector-card">
  <div class="sector-header">
    <h2>{sector_name}</h2>
    <span class="sector-etf">Benchmark: {etf} ({etf_return_ytd}% YTD)</span>
    <span class="update-date">Updated {date}</span>
  </div>
  
  <!-- Portfolio summary table -->
  <table class="picks-table">
    <thead><tr>
      <th>Ticker</th><th>Name</th><th>Conviction</th>
      <th>Weight</th><th>Entry Price</th><th>Base Alpha</th>
      <th>Bull Alpha</th><th>Bear Alpha</th>
    </tr></thead>
    <tbody>
      {% for pick in picks %}
      <tr>
        <td class="ticker">{{ pick.ticker }}</td>
        ...
      </tr>
      {% endfor %}
    </tbody>
  </table>
  
  <!-- One-line thesis per pick -->
  {% for pick in picks %}
  <div class="pick-thesis">
    <strong>{{ pick.ticker }}</strong>: {{ pick.one_line_thesis }}
  </div>
  {% endfor %}
</div>
```

Also update `/Users/max/morningsignal-research/docs/index.html` to include a "Sector Portfolios" section linking to each sector page.

Update the existing Jinja2 templates in `/Users/max/morningsignal-research/site/templates/` to include the sector picks section.

---

## STEP 7 — STATE PERSISTENCE

**File:** Each sector run saves to `state/picks_{sector}_{date}.json`:

```json
{
  "sector": "Information Technology",
  "etf": "XLK",
  "run_date": "2026-04-10",
  "etf_price_at_entry": 215.42,
  "picks": [
    {
      "ticker": "NVDA",
      "entry_price": 850.00,
      "conviction": "HIGH",
      "weight": 0.15,
      "expected_alpha_9m_base": 0.18,
      "expected_alpha_9m_bull": 0.35,
      "expected_alpha_9m_bear": -0.05,
      "one_line_thesis": "AI accelerator monopoly with pricing power expanding into data center networking",
      "deep_dive_path": "state/deep_dives/NVDA_2026-04-10.json"
    }
  ]
}
```

This file is the seed for Phase 2's performance tracker.

---

## MAIN ORCHESTRATOR

**File:** `run_sector.py`

```python
def main():
    today = date.today()
    sector = get_today_sector()
    
    print(f"=== Sector Research: {sector['name']} ({sector['etf']}) ===")
    
    # Step 1: Universe
    universe = get_sector_universe(sector["name"])
    print(f"  Universe: {len(universe)} stocks")
    
    # Step 2: Quant screen → top 20
    screened = run_quant_screen(universe, sector["name"])
    print(f"  After screening: {len(screened)} candidates")
    
    # Step 3: Deep dives (Gemini)
    deep_dives = run_deep_dives(screened, sector)
    print(f"  Deep dives complete: {len(deep_dives)}")
    
    # Step 4: Portfolio construction
    portfolio = construct_portfolio(deep_dives, sector)
    print(f"  Portfolio: {len(portfolio)} picks")
    
    # Step 5: Reports
    save_report(portfolio, sector, today)
    update_morningsignal_site(portfolio, sector, today)
    
    # Step 6: Push to GitHub
    push_to_github(f"Sector picks: {sector['name']} {today}")
    
    print(f"=== Complete. Site live at research.morningsignal.xyz ===")
```

---

## DEPENDENCIES

`requirements.txt`:
```
yfinance>=1.2.0
pandas>=2.0
numpy>=1.24
requests>=2.31
google-generativeai>=0.8.0
jinja2>=3.1
python-dotenv>=1.0
```

`.env`:
```
GEMINI_API_KEY=AIzaSyB1fw-fwwOzzBg7xnyyPu72P_7PXj4lerk
```

---

## SUCCESS CRITERIA

Run `python3 run_sector.py` and verify:

1. ✓ Identifies today's sector by rotation
2. ✓ Fetches 30+ stock universe for that sector
3. ✓ Screens to top 20 quantitatively
4. ✓ Produces deep dives for all 20 (or 10 if quota limited)
5. ✓ Selects 5-10 portfolio picks with scores and rationale
6. ✓ Generates sector_SECTOR_DATE.md report
7. ✓ Generates and pushes HTML to MorningSignal
8. ✓ Saves picks JSON to state/ for Phase 2 tracking

**Estimated runtime:** 5-15 minutes per sector (dominated by Gemini API calls).

---

## CRITICAL IMPLEMENTATION NOTES

1. **Do not use mocked data.** Every number in the reports must come from real yfinance data or real Gemini analysis. No placeholders.

2. **Handle yfinance failures gracefully.** Many fields in `.info` return None. Always use `.get("field", None)` and handle missing data in the report as "N/A" rather than crashing.

3. **Rate limiting.** yfinance will throttle on 1300+ tickers. Batch downloads using `yf.download(batch_of_20)` for price history. Use `.info` individually only for the top 20 screened candidates.

4. **Gemini context window.** Each deep dive prompt is large. Keep to one stock at a time. Gemini 2.5 Flash handles this well.

5. **The sector playbook is baked into the system prompt.** Don't retrieve it dynamically — embed it per sector call.

6. **MorningSignal integration:** Do NOT break the existing `docs/index.html` or `docs/daily/` structure. Add sector picks as a NEW section. The existing breakout scanner and market brief remain unchanged.

---

*Build this system completely and test with a live run on today's sector before considering it complete.*
