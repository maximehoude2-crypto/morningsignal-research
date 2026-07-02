# MorningSignal — Full Codebase Review & Improvement Plan

Reviewed: 2026-07-02. Scope: all scanner modules, site generation + templates, newsletter, deploy, and the three orchestrators (~7,000 lines). Findings verified against the code and against actual published output in `docs/`.

Overall: the system is impressively complete — a real end-to-end daily research product with scanning, LLM narrative, static site, and newsletter. The architecture (state JSON → Jinja2 → git push → Cloudflare Pages) is sound. The problems fall into four buckets: **bugs that silently corrupt today's published output**, **ops fragility** (single laptop, no alerting, ungated deploys), **LLM cost waste**, and **accumulated duplication**.

---

## 1. Critical — silently wrong output, fix first

### 1.1 Jinja2 autoescaping is OFF for every template (stored XSS)
`site/generate_site.py:141` uses `select_autoescape(["html"])`, but all templates are named `*.html.j2` — the suffix match fails, so autoescape resolves to `False` on every page (verified empirically: `env.autoescape('index.html.j2') → False`). Every `{{ ... }}` is injected raw, including **scraped third-party headlines** (`daily.html.j2:242`, including `href="{{ h.url }}"`) and all LLM output. Confirmed in the wild: raw `S&P` entity issues in `docs/daily/2026-07-01.html:431`.
**Fix:** `autoescape=select_autoescape(enabled_extensions=("html","htm","xml","j2"))` (or `True`), then harden the two `|safe` sites: `weekly.html.j2:84` (escape before paragraph-splitting) and `dashboard.html.j2:513` (escape `</` in embedded JSON).

### 1.2 The primary earnings-calendar scraper has never worked
`scanner/thematic_scanner.py:376–378`:
```python
ticker_match = re.search(r'>([A-Z]{1,5})<', ticker_html) if 'import re' or True else None
import re as _re
```
`re` is never imported at module level and `'import re' or True` is truthy, so this raises `NameError` on the first row of every response — swallowed by `except Exception: pass` at line 395. The Zacks catalyst calendar has silently returned nothing since it was written, and it degrades `earnings_sync._fallback_earnings_calendar` too. Also `_scrape_catalyst_calendar` uses `date.today()` instead of the pipeline's target date (line 357).

### 1.3 Ticker cleaning is backwards — class shares excluded from the universe
`scanner/breakout_scanner.py:48` and `scanner/industry_scanner.py:116`:
`str(t).split(".")[0].strip().replace("-", ".")` turns Wikipedia's `BRK.B`/`BF.B` into `BRK`/`BF` (wrong symbols), and the `replace("-", ".")` direction is the opposite of yfinance's dash convention. Correct transform: `.replace(".", "-")`. These names have been permanently missing from every scan.

### 1.4 The "2s10s" spread is not 2s10s
`scanner/market_brief.py:41–45, 1268–1270`: `^IRX` is the **13-week T-bill**, not the 2-year. The flagship macro stat is actually the 3m/10y spread, mislabeled in the JSON, the LLM prompt, and the fallback note. Source a real 2Y (FRED DGS2 or `2YY=F`) or relabel everywhere.

### 1.5 RS scoring saturates exactly where it matters
`scanner/indicators.py:126`: `rs_score = min(100, max(0, (rs + 0.3) / 0.6 * 100))` — a linear clamp on a raw return-spread, not the claimed "IBD RS Rating (0–99)". Every stock beating SPY by >30 weighted points pins at 100, destroying discrimination among the leaders the scanner exists to rank. Since ~1,500 names are already scanned, compute raw RS for all and **percentile-rank across the universe** (true IBD style). Related: the quarter weighting uses disjoint segments instead of IBD's today-anchored legs, and missing history scores 0 instead of renormalizing (penalizes recent IPOs).

### 1.6 Base-quality score penalizes the breakout itself
`scanner/indicators.py:71–77`: `base_window = prices.tail(75)` includes the breakout move, so a stock that just broke out +20% from a tight base measures ~30% "depth" → tightness ≈ 0. Measure the base over `prices.iloc[-75:-5]` (or find the pivot). Also smooth the volume-score cliff at line 83 (ratio 1.39 → 0 pts, 1.41 → 20.5 pts) and exclude the last 5 days from the volume average.

### 1.7 Missing market data publishes as 0.00
`market_brief.py:48–55, 1260–1266`: `_pct_change` returns 0.0 for empty series and macro levels default to 0. A failed `^VIX` fetch puts "VIX: 0.00" into the LLM prompt and the published report; a total yfinance outage yields a fully-formed all-zeros brief. Distinguish missing from flat, and refuse to write/publish a brief whose indices/sectors are empty.

### 1.8 Corrupt numbers fed to the earnings LLM
`scanner/earnings_sync.py:340`: `strftime("%Y-Q%q")` — `%q` is not a strftime directive; produces garbage fiscal-period labels. Use `pd.Period` quarter formatting. `earnings_sync.py:343`: the magnitude heuristic multiplies already-percent surprises by 100 (a real +4.0% becomes 400%) and hands it to the model as evidence.

### 1.9 Age-0 truthiness bugs bury today's MA crosses
`scanner/industry_scanner.py:611, 618`: `events.get(...) or fallback` and `x.get("age") or 99` — a golden/death cross that happened **today** (age 0) is falsy, so it gets the wrong age and sorts last as if oldest. Use `is not None`.

### 1.10 Positional (not date) alignment in the scanner
`breakout_scanner.py:316–322`: Close, Volume, and SPY are truncated by row count after independent `dropna()`s, so indices can refer to different dates for gappy series — RS computed against a misdated benchmark. Inner-join on the DatetimeIndex instead. Related: insufficient-history guards in `indicators.py` are off (Stage-2 needs 159+ rows, trend score needs 219+; guards check 150/200), so newly-listed names silently fail checks due to NaN comparisons.

---

## 2. Ops & reliability — the "site quietly stops updating" class

### 2.1 Deploy is ungated and failures are invisible
`run_daily.py` continues through every step failure and runs Site Generation + Deploy unconditionally: a failed scanner republishes stale data with today's timestamp; a half-written `docs/` gets committed. And failures only reach a log file on one Mac.
**Fix (highest-leverage ops change):** gate `deploy()` on site-generation success, build `docs/` to a temp dir and atomically swap, and send a failure email via the already-wired Resend on any step failure.

### 2.2 push_to_github: blind lock deletion, no divergence handling
`deploy/push_to_github.py:39–44` unconditionally deletes `.git/HEAD.lock`/`index.lock` — if any git process is mid-operation this invites index corruption. And there is no fetch/rebase or retry before `push origin main`: one commit made on GitHub (e.g., editing README in the web UI) makes every subsequent daily push fail forever, silently. Add a pipeline-level lockfile (flock/pidfile in run_daily/run_morning/run_weekly — nothing currently stops the 8:30 and 18:30 runs overlapping), remove the blind lock deletion, and add `fetch` + `pull --rebase` + one retry + alert-on-failure.

### 2.3 `state/` is an unbacked single point of truth
Everything — archive, deep-dive history, streaks, weekly digest inputs — is rebuilt from gitignored JSON on one laptop. Sync it nightly (private repo/branch or cloud storage), and decouple `archive.html` from state by enumerating `docs/daily/*.html` so lost state doesn't orphan published pages.

### 2.4 Silent network degradation everywhere
~15 `except Exception: pass` blocks across the scrapers. Concretely dead today: `feeds.reuters.com` and the AP RSS feeds (`news_intelligence.py:205–212`) were decommissioned years ago — half the news sources contribute nothing, invisibly. Barchart scraping (`thematic_scanner.py:471`) targets a JS-rendered page and yields nothing. yfinance calls have no retries and near-zero throttling (`BATCH_SLEEP = 0.1`s; the top-15 `.info` enrichment loop hammers the most rate-limited endpoint). Add per-source item-count logging, retries with backoff, a minimum-universe sanity check (abort if < ~1,200 tickers), and prune the parquet cache (~1,500 files/day, never cleaned). Also: the same-day parquet cache freezes partial intraday bars if any run happens before the close (`breakout_scanner.py:95, 141`).

### 2.5 State writes are non-atomic and reads unguarded
Every module hand-rolls `write_text(json.dumps(...))` and bare `json.loads(read_text())`. One interrupted run corrupts a state file that then crashes `dashboard_data`/`industry_scanner` next run. One shared `save_state`/`load_state` helper with tmp-file + `os.replace` and guarded reads fixes the whole class.

### 2.6 Scheduling gaps
- The weekly digest is documented to run Friday 18:00, but the daily pipeline that writes Friday's data runs at 18:30 — the digest never includes Friday and silently backfills the previous week. Move it after the daily run (or Saturday AM).
- No `xyz.morningsignal.weekly.plist` exists in the repo despite the README documenting it.
- `StartCalendarInterval` skips runs entirely if the Mac is off; the Friday-gated weekly summary then never generates and nothing backfills.
- Streak state isn't idempotent (`breakout_scanner.py:166–177`): re-running the same day double-increments streaks and can falsely trip 3-day deep-dive flags. Store last-counted date.

---

## 3. LLM cost & quality

### 3.1 Three narrative generations per day, two thrown away
`run_market_brief` generates a narrative before thematic/industry/news data exists (maximal hallucination pressure — the prompt's rules demand citing sections that are empty), then `run_daily.py` regenerates it twice more after merges (the two near-identical regen blocks at `run_daily.py:124–153` and `176–199` are also copy-paste). **Defer to a single narrative call in the orchestrator after full enrichment** — cuts this cost ~3× and removes the duplicated code.

### 3.2 The earnings prompt is unbounded
`earnings_sync.py:615–670`: up to 30 companies × 5 evidence items × 4,500 chars, serialized with `indent=2` (≈2× token bloat) — worst case ~170K input tokens, twice a day, ×3 on retry. Output capped at 16K tokens with a "every company must appear" instruction — busy days truncate mid-table with no detection. Add a hard prompt-size budget, drop `indent=2`, and check for truncation before publishing.

### 3.3 Retry and timeout hygiene
`openai_client.py:72`: retries all exceptions including auth/400/context-overflow (deterministic failures, retried anyway), no client `timeout=` (SDK default 600s × its own 2 internal retries × your 3 attempts ≈ a pipeline hung for 30+ min), and `max_output_tokens=5000` is shared with reasoning tokens under `reasoning effort: medium` — heavy reasoning can starve the actual text and trigger the fallback. Retry only 429/5xx/timeouts, set a timeout, `max_retries=0` on the SDK, raise the output budget.

### 3.4 Prompt hallucination anchors
`weekly_summary.py:210, 254`: the schema examples and RULE 2 hardcode specific past events ("Anthropic Mythos Model Launch", "TSMC Q1 Revenue Beat") and tell the model to reference them — actively inviting echoes of stale events. Mark examples synthetic; don't enumerate events. Also: Friday reruns regenerate (and re-bill) the weekly summary and can overwrite a good file with a failure stub (`weekly_summary.py:283–332`); and `run_daily.py:204` doesn't pass the target date, so backfill runs summarize the wrong week.

---

## 4. Product & frontend

- **Broken deep-dive links:** every breakout ticker is hyperlinked to `deep-dives/{ticker}.html`, but pages only exist for 3-day-streak tickers — 8 of 15 links on `docs/daily/2026-07-01.html` are 404s. Only link tickers with pages (or generate a page per ranked ticker).
- **SEO baseline missing:** no `og:*`/canonical/robots.txt/sitemap.xml, one meta description on the homepage only, and no real `h1`–`h6` outline (headings are styled divs). Cheap, high-value for a published research site.
- **Accessibility:** three different navs across the site, empty-href active links, color-only pos/neg semantics, no skip link, Chart.js canvas without a text alternative.
- **Newsletter compliance & privacy:** all recipients in the `to:` header (everyone sees everyone's address — use bcc/per-recipient), no unsubscribe footer (CAN-SPAM/CASL), and `config/advisors.json` commits subscriber PII to a public repo. Weekly index math **sums** daily percent changes instead of compounding (`weekly_digest.py:68–75`).
- **Page bloat:** the same headline renders once per theme tag (duplicated content blocks), heavy inline styles, three blocking Google Fonts, Chart.js CDN without SRI. Every page embeds `generated_at`, so nearly every file changes every run → two commits/day of near-identical HTML, compounding git history growth. Move the timestamp to one include and only rewrite changed pages.
- **Machine-specific paths:** `generate_site.py:363–364` hardcodes `/Users/max/...`; on any other machine the podcast section silently regenerates as "No episodes available yet." and orphans episode pages. Never overwrite an index page with an empty one when the source dir is missing.

---

## 5. Code health / duplication (factor-out list)

1. `_compute_rrg` — verbatim copy in `industry_scanner.py:326` and `thematic_scanner.py:135` (with a shared RRG tail-alignment bug: ratio and momentum series of different lengths indexed by the same end-offset ≈ 10 trading days apart).
2. `_resolve_target_date` — identical in four modules.
3. Multi-timeframe returns — `industry_scanner._multi_timeframe` vs `thematic_scanner._compute_returns`, same math, different empty-value conventions.
4. State JSON IO (see 2.5) and dry-run scaffolding — every module re-implements both.
5. `step()` duplicated between `run_daily.py` and `run_morning.py`; narrative-regen block duplicated within `run_daily.py`.
6. The same Chrome UA string pasted in 4+ files; two near-identical Yahoo RSS helpers in `earnings_sync.py`; three independent earnings-calendar mechanisms across three modules.
7. **Rename the `site/` package** (e.g. `sitegen/`) — it shadows stdlib `site`, forcing the `sys.modules.pop("site")` + importlib gymnastics in both orchestrators.
8. Dead code: `_mock_ticker` and duplicate `_theme_local` in market_brief; `anchor_today` in industry_scanner; `_next_weekday` in earnings_sync; `industry_map` in dashboard_data; unused `hashlib`/`re`/`os`/`numpy` imports; the always-`pass` investordebate archive loop in `generate_site.py:236–241`; hardcoded "GPT-5.4" in log strings; stale hand-maintained `SP100` list (contains delisted `PXD`).
9. Every magic tunable (score weights 40/35/25, RS normalization 0.3/0.6, volume gate 1.4, MIN_SCORE, TOP_N, cap thresholds) belongs in one config block.
10. Zero tests. The indicator math (section 1.5/1.6/1.10) is exactly the kind of pure-function code that's trivial to unit-test with synthetic price series — start there.

---

## Suggested order of attack

| Phase | Items | Effort |
|-------|-------|--------|
| 1. One-line critical fixes | 1.1 autoescape, 1.2 `re` import, 1.3 ticker transform, 1.4 relabel 2s10s, 1.9 age-0 | ~1 hour |
| 2. Publish-safety | 2.1 gate deploy + Resend failure alerts, 2.2 lockfile + rebase-push, 1.7 refuse empty briefs, minimum-universe check | ~1 day |
| 3. Scoring quality | 1.5 percentile RS, 1.6 base window, 1.10 date-join, history guards + unit tests | ~1–2 days |
| 4. Cost | 3.1 single deferred narrative, 3.2 earnings prompt budget, 3.3 client hygiene | ~1 day |
| 5. Product polish | broken links, SEO baseline, newsletter bcc/unsubscribe/PII, Friday digest timing | ~1 day |
| 6. Refactor | shared state IO, analytics module, `sitegen` rename, config block, dead-code sweep | ongoing |
