# Zefix Analyzer

Internal leads dashboard for Swiss registered companies. Bulk-imports the entire Zefix commercial register, runs Google Search to find each company's website, and provides a GUI to review, score, and track outreach.

* **Zefix API** – bulk-import all ~700k companies from the official Swiss commercial register ([zefix.admin.ch](https://www.zefix.admin.ch/ZefixREST/swagger-ui.html)), canton by canton with resume support
* **Serper.dev** – automatically find and score each company's website (0–100 match score)
* **Zefix priority score** – score every company from Zefix data alone (legal form, capital, purpose, industry, proximity) so high-value companies are Google-searched first
* **Keyword scoring** – configurable target and excluded keywords matched against `purpose` + `industry` text; boosts or penalises the Zefix score; fully tunable from Settings
* **Configurable scoring** – tune Zefix scoring weights/penalties in the Settings UI without code changes
* **Score explainability** – per-company Zefix score breakdown (component contributions + forced-zero reason)
* **Industry taxonomy** – configurable category→keyword mapping stored in Settings; best-match derivation (highest keyword hit count wins) replaces first-match; re-derivable in bulk via Settings
* **TF-IDF clustering** – unsupervised topic clustering of `purpose` text (K-Means + scikit-learn, no API); groups companies into N thematic clusters; stores a `tfidf_cluster` label (top-3 terms, e.g. `software · entwicklung · digital`) per company; run periodically from Settings
* **Claude classification** – sends each company's purpose text to Claude Haiku; returns a `claude_score` (0–100) and `claude_category` label; separate from Zefix/Google scores; configurable system prompt; ~$0.25/1 000 companies; run in bulk from Settings
* **Offline geocoding** – building-level precision (<10 m) via the swisstopo Amtliches Gebäudeadressverzeichnis (~4 M addresses, downloaded once, no API key); falls back to GeoNames PLZ centroid (~2 km) if no match; proximity to Muri bei Bern factored into the score
* **Interactive map** – `/ui/map` plots all geocoded companies on a Leaflet.js map, coloured by Google score (green/yellow/red/grey); filterable by canton, review status, score thresholds
* **Persistent background jobs** – DB-backed queue for bulk/batch/detail/initial/scoring/classification jobs; survives closing/reopening the UI
* **Jobs dashboard** – `/ui/jobs` shows queued/running/paused/completed/failed/cancelled jobs with progress and timestamps
* **Job pause + resume** – pause a running job at the next checkpoint, start another, then resume from where it left off
* **Job cancellation + event stream** – cancel queued/running/paused jobs and inspect per-job event logs
* **Leads dashboard** – filter/sort/paginate companies by Zefix, Google, or Claude score; toggle column visibility; bulk-update review and proposal status; shows a live banner when jobs are running
* **Company detail** – view enriched data including TF-IDF cluster and Claude score/category; pick best website from search results, add contact info and notes; "Refresh from Zefix" button re-fetches and geocodes on demand
* **CSV export** – export any filtered view to CSV
* **HTTPS** – Nginx reverse proxy with self-signed certificate (or swap in a CA-signed cert); HTTP auto-redirects to HTTPS
* **PostgreSQL** – all data persisted in Postgres; DB indexes on all filter columns
* **FastAPI + Jinja2** – server-rendered UI, no JS framework required

---

## Quick start (Docker Compose)

```bash
cp .env.example .env
# Edit .env: set SERPER_API_KEY and database credentials

# Generate a self-signed TLS certificate (once)
bash scripts/gen-certs.sh

docker compose up --build
```

GUI: <https://localhost/ui>
Health check: <https://localhost/health>

> **HTTP is redirected to HTTPS automatically.** Browsers will show a self-signed certificate warning — add an exception or replace `certs/cert.pem` / `certs/key.pem` with a CA-signed certificate.

---

## Local development

### Prerequisites

* Python 3.12+
* PostgreSQL 14+

### Setup

```bash
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env         # fill in your values
alembic upgrade head
uvicorn app.main:app --reload
```

---

## Configuration

All settings are read from environment variables (or a `.env` file):

| Variable | Description | Default |
|---|---|---|
| `POSTGRES_HOST` | PostgreSQL server host or IP | `localhost` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_USER` | PostgreSQL username | `zefix` |
| `POSTGRES_PASSWORD` | PostgreSQL password | *(required)* |
| `POSTGRES_DB` | Database name | `zefix_analyzer` |
| `DATABASE_URL` | Full connection URL — overrides the individual fields above if set | *(optional)* |
| `ZEFIX_API_BASE_URL` | Zefix REST API base URL | `https://www.zefix.admin.ch/ZefixREST/api/v1` |
| `ZEFIX_API_USERNAME` | HTTP Basic Auth username (optional) | *(empty)* |
| `ZEFIX_API_PASSWORD` | HTTP Basic Auth password (optional) | *(empty)* |
| `GOOGLE_SEARCH_ENABLED` | Enable/disable website search (also settable via the UI) | `true` |
| `SERPER_API_KEY` | Serper.dev API key | *(required for website search)* |
| `GOOGLE_DAILY_QUOTA` | Daily search quota (also settable via the UI) | `83` |
| `ANTHROPIC_API_KEY` | Anthropic API key for Claude classification jobs (also settable via Settings UI) | *(empty)* |

---

## GUI Workflow

1. **Bulk import** all companies from Zefix (see below) — one-time, ~hours
2. **Detail fetch** from Collection — populates address, purpose, and geocodes lat/lon
3. **Batch enrich** with Google Search to find websites — runs daily against free 100-query quota
4. **Dashboard** at `/ui` — filter by canton, industry, tags, review/proposal status, Zefix/Google/Claude score; toggle column visibility; sort; paginate; bulk-update
5. **Map** at `/ui/map` — geographic overview of geocoded companies, coloured by Google score
6. **Company detail** — pick best website from Google results, set contact info, add research notes; shows TF-IDF cluster and Claude score/category if populated
7. **Jobs** at `/ui/jobs` — monitor queue/runs, pause/resume, view event stream, cancel jobs
8. **Export CSV** — download any filtered view
9. **Settings** at `/ui/settings` — configure scoring, industry taxonomy, API keys; run classification batch jobs (re-derive industry, TF-IDF clustering, Claude classification)

### Status fields

| Field | Values |
|---|---|
| Review status | `pending` (default) · `confirmed` · `interesting` · `rejected` |
| Proposal status | `not sent` (default) · `sent` · `responded` · `converted` · `rejected` |
| Website match score | 0–100 (auto-scored: name overlap, location, purpose keywords, legal form) |

---

## Data Collection (run_collector.py)

Three modes — run locally or via Docker:

```bash
python -m app.run_collector <mode> [flags]
# or via Docker:
docker compose --profile collector run --rm collector python -m app.run_collector <mode> [flags]
```

### `bulk` — mass-import all companies from Zefix

Iterates every canton with pagination. No Google Search — fast, low API load. Run once to seed the DB.

```bash
python -m app.run_collector bulk
python -m app.run_collector bulk --canton ZH --canton BE   # specific cantons only
python -m app.run_collector bulk --resume                  # resume after interruption
```

Flags:
* `--canton XX` — limit to specific canton(s), repeatable (default: all 26)
* `--page-size 200` — companies per API request (Zefix max ~500)
* `--delay 0.5` — seconds between API calls
* `--include-inactive` — include inactive register entries
* `--resume` — continue from last checkpoint (survives crashes/network errors)

### `batch` — recurring Google Search enrichment

Processes companies already in the DB, runs Google Search to find websites.
Respects the 100 free queries/day limit — the dashboard shows today's count.

```bash
python -m app.run_collector batch --limit 100
python -m app.run_collector batch --limit 100 --refresh-zefix   # also re-fetch Zefix details
```

Flags:
* `--limit 100` — max companies to process (default: 100)
* `--all-companies` — process all companies, not only those missing a website
* `--refresh-zefix` — re-fetch full Zefix details (purpose, address) before Google step
* `--skip-google` — skip Google Search (useful with `--refresh-zefix` for data refresh only)

### `initial` — one-time import from UIDs or name searches

Useful for targeted imports before or instead of a full bulk run.

```bash
python -m app.run_collector initial --name "Muster AG" --uid CHE-123.456.789
python -m app.run_collector initial --names-file names.txt --uids-file uids.txt
```

Flags:
* `--name` / `--names-file` — search terms (repeatable / one per line)
* `--uid` / `--uids-file` — direct Zefix UIDs (repeatable / one per line)
* `--import-limit-per-name 10` — how many results to import per search term
* `--search-max-results 25` — Zefix search breadth
* `--include-inactive` — include inactive companies
* `--skip-google` — import from Zefix only

### Scheduling recurring batch runs (cron)

```bash
# Every day at 02:30 — process up to 100 companies
30 2 * * * cd /opt/zefix_analyzer && docker compose --profile collector run --rm collector \
  python -m app.run_collector batch --limit 100 >> /var/log/zefix_batch.log 2>&1
```

---

## Zefix API reference

The app uses the public Zefix REST API — no account required for read-only access.
Full Swagger docs: https://www.zefix.admin.ch/ZefixREST/swagger-ui.html

Base URL: `https://www.zefix.admin.ch/ZefixREST/api/v1`

### Endpoints used

#### `POST /company/search` — search / paginate companies

Used by both `bulk` (canton sweep) and `initial` (name search) modes.

```json
{
  "canton": "ZH",
  "maxEntries": 200,
  "offset": 0,
  "activeOnly": true,
  "languageKey": "en"
}
```

Key request fields:

| Field | Type | Description |
|---|---|---|
| `name` | string | Company name search term (partial match) |
| `canton` | string | Two-letter canton code (`ZH`, `BE`, …) — omit for all cantons |
| `maxEntries` | int | Results per page, max ~500 |
| `offset` | int | Zero-based record offset for pagination |
| `activeOnly` | bool | Filter to active register entries only |
| `languageKey` | string | Response language: `de`, `fr`, `it`, `en` |

Response: `{ "list": [ ... ], "count": 12345 }` or a bare array depending on endpoint version.

Each company object contains: `uid`, `name` (localised dict or string), `legalForm`, `status`, `municipality`, `canton`.

#### `GET /company/uid/{uid}` — full company details

Used by `initial` mode and `batch --refresh-zefix`. UID format: `CHE123456789` (digits only) or `CHE-123.456.789`.

Returns the full company record including:

| Field | Description |
|---|---|
| `uid` | UID in `CHE-XXX.XXX.XXX` format |
| `name` | Localised name dict `{ "de": "...", "fr": "...", "it": "..." }` |
| `legalForm` | `{ "de": "Aktiengesellschaft", "shortName": "AG" }` |
| `status` | `ACTIVE`, `DELETED`, etc. |
| `municipality` | Municipality name string |
| `canton` | Two-letter canton code |
| `address` | `{ "street", "houseNumber", "swissZipCode", "city" }` |
| `purpose` | Business purpose text (used for website scoring) |
| `registrationDate` | ISO date string |

#### `GET /canton` — list all cantons

Returns the list of valid canton codes. The app hardcodes all 26: `AG AI AR BE BL BS FR GE GL GR JU LU NE NW OW SG SH SO SZ TG TI UR VD VS ZG ZH`.

### Authentication

The API is publicly accessible without credentials for read access. If your deployment requires HTTP Basic Auth (e.g. a Zefix test environment), set `ZEFIX_API_USERNAME` and `ZEFIX_API_PASSWORD` in `.env`.

### Rate limiting

Zefix does not publish official rate limits or quota documentation.

**How the app limits its own request rate:**

The `bulk` import loop calls `time.sleep(request_delay)` (default `0.5s`) after every page of results and again between cantons. This means a full 26-canton sweep at `--page-size 200` and `--delay 0.5` produces roughly 1 request every 0.5 seconds. There is no adaptive backoff — if a request fails, the error is recorded and the sweep moves on to the next canton.

| Parameter | Default | Effect |
|---|---|---|
| `--delay` | `0.5s` | Sleep between every API page and between cantons |
| `--page-size` | `200` | Results per request (Zefix cap ~500); fewer pages = fewer requests |

**Recommendations:**
- Keep `--delay` at `0.5s` or higher for a full sweep
- If you get HTTP 429 or connection errors in the logs, increase `--delay` to `1.0` or `2.0`
- The `initial` mode (name search) has no built-in delay — keep the number of search terms small
- There is no retry logic; use `--resume` to continue after a failed run

---

## Running tests

```bash
pytest
```

Tests use an in-memory SQLite database — no PostgreSQL required.

---

## Database migrations

Migrations run automatically on every container start (via `alembic upgrade head` in `app.main` during lifespan startup).
If the DB is reachable and credentials/permissions are valid, all pending revisions are applied before the app becomes ready.

```bash
alembic upgrade head      # apply all migrations
alembic current           # show current revision
alembic history           # list all revisions
```

Migrations live in `alembic/versions/`.
Recent additions include:

| Revision | Description |
|---|---|
| `0001` | Initial schema (companies, notes) |
| `0002` | Status fields (review, proposal, website score, Google results) |
| `0003` | Filter indexes |
| `0004` | Contact fields, industry, tags, collection_runs table |
| `0005` | App settings table (runtime-configurable Google quota) |
| `0010` | `job_runs` queue table + `companies.zefix_score_breakdown` |
| `0011` | Job cancellation support (`job_runs.cancel_requested`) + `job_run_events` log stream |
| `0012` | Job pause support (`job_runs.pause_requested`) |
| `0013` | `users` and `audit_log` tables |
| `0014` | `companies.tfidf_cluster`, `claude_score`, `claude_category`, `claude_scored_at` |

For the complete lineage in your environment, use `alembic history`.

---

## Company Classification

Three optional enrichment passes add deeper categorisation signals. All run as background jobs from **Settings → Industry classification**.

### Re-derive industry labels

Re-runs the keyword-based industry derivation on existing companies using the current taxonomy from Settings. Uses best-match scoring (highest keyword hit count wins) rather than first-match. Re-computes `zefix_score` for any company whose industry label changes.

Useful after editing the industry taxonomy or adding new categories.

### TF-IDF clustering (local, no API)

Groups companies by the semantic content of their `purpose` text using unsupervised machine learning. Requires `scikit-learn` (included in `requirements.txt`).

**How it works:**

1. Each company's `purpose` field is converted to a TF-IDF vector — words that are distinctive to that company's text get high weight; common filler words get near-zero weight
2. K-Means groups the vectors into N clusters of similar companies
3. Each cluster is labelled with its 3 highest-weight terms (e.g. `software · entwicklung · digital`, `bau · immobilien · verwaltung`)
4. The label is stored in `tfidf_cluster` on each matched company

**Checking results:**

- Open any company detail page — `tfidf_cluster` is shown in the info grid if populated
- SQL query to see cluster distribution:
  ```sql
  SELECT tfidf_cluster, count(*) AS n
  FROM companies
  WHERE tfidf_cluster IS NOT NULL
  GROUP BY tfidf_cluster
  ORDER BY n DESC;
  ```
- The job log at `/ui/jobs` shows how many companies were classified

**Tips:**
- Start with `n_clusters=10–15` and a broad filter (or no filter) so K-Means has enough data
- `min_df=2` means a term must appear in at least 2 purpose texts — very small subsets (< ~20 companies) may fail or produce poor clusters
- Re-run whenever you add large batches of new companies; old labels remain valid until overwritten
- The clusters are exploratory, not prescriptive — use them to spot themes, not as ground truth

### Claude classification (Anthropic API)

Sends each company's `purpose` text to Claude Haiku with a configurable system prompt. The model returns a JSON object `{"score": 0–100, "category": "..."}` which is stored as `claude_score` and `claude_category`.

**Setup:** set your Anthropic API key in Settings or via `ANTHROPIC_API_KEY` env var.

**Cost:** ~$0.25 per 1 000 companies at Haiku pricing (~150 input tokens + ~20 output tokens per company).

**Checking results:**

- `claude_score` and `claude_category` are shown on each company detail page
- The dashboard allows filtering by minimum `claude_score` and sorting by `claude_score`
- The job log at `/ui/jobs` shows tokens consumed

**Custom prompt:** override the default evaluation prompt from **Settings → Claude classification prompt**. The model must always output only `{"score": N, "category": "..."}` — keep that constraint in any custom prompt.

---

## Scoring

Three independent scores are computed and stored per company:

### Zefix priority score (0–100)

Computed from Zefix register data alone — no Google Search required. Used to order which companies get searched first during batch enrichment.
Weights and penalties are configurable in **Settings** (`/ui/settings`).

| Component | Points |
|---|---|
| Legal form — AG/SA | +10 · GmbH/Sàrl +25 · Genossenschaft +20 · KG +15 · OG +12 · Stiftung +8 · Verein +5 · unknown +5 |
| Capital nominal > 100 k | +10 · > 0 +5 |
| Purpose text richness (≥ 20 words) | +20 · ≥ 8 words +5 |
| Branch offices present | +10 |
| Industry detected | +15 (configurable) |
| Target keywords matched in purpose/industry (1 hit = +½ bonus, 2+ hits = +full bonus) | configurable keyword list |
| Excluded keywords matched in purpose/industry | −penalty (configurable) |
| Location — canton tier | BE/SO +10 · AG +8 · BL/BS +6 · LU +5 · ZH +4 · all others −8 |
| Location — distance to Muri bei Bern | ≤ 15 km +15 · ≤ 40 km +10 · ≤ 80 km +5 · ≤ 130 km 0 · > 130 km −5 |
| Status not clearly active | −40 (configurable) |
| Status contains force-zero term (default: `being_cancelled`) | score forced to 0 |

Distance is computed with the Haversine formula. Coordinates come from the geocoded address when available, else municipality name lookup, else canton centroid.

### Score explainability

Each company stores a Zefix score breakdown JSON (`zefix_score_breakdown`) with component contributions and final score.
In the company detail page (`/ui/companies/{id}`), open **Zefix Score Breakdown** to inspect how the score was derived.

### Website match score (0–100)

Google Search score — computed after finding the best-matching website result.

### Claude score (0–100)

Returned by Claude Haiku when you run the Claude classification batch job. Stored alongside `claude_category` (a short label). Independent of Zefix and Google scores — use it to rank companies by AI-assessed lead quality or fit.

### Website match score (0–100, shown in green/yellow/red)

Computed after Google Search against the best matching result. Factors: company name overlap in title/snippet, municipality and canton in result text, purpose keyword matches, legal form in domain, directory domain penalty.

---

## Geocoding

Addresses are geocoded offline in two layers — no API key required:

### Primary: swisstopo Amtliches Gebäudeadressverzeichnis
- Source: [data.geo.admin.ch](https://data.geo.admin.ch/ch.swisstopo.amtliches-gebaeudeadressverzeichnis/) — Open Government Data, free for any use
- ~4 million Swiss building addresses with LV95 coordinates, converted to WGS84 at build time
- Indexed into `data/geocoding.db` (SQLite, ~300–400 MB on disk, git-ignored)
- Accuracy: building entrance level, typically **< 10 m**
- Lookup: parses the Zefix address into street + house number + PLZ, queries the SQLite index

### Fallback: GeoNames PLZ centroid
- Source: [GeoNames Switzerland](https://download.geonames.org/export/zip/CH.zip) (CC BY 4.0)
- Used when no building match is found (e.g. unknown street name or PO box address)
- Accuracy: postal code centroid, typically **< 2 km**
- Cached to `data/plz_ch.tsv` (git-ignored)

Both datasets are downloaded automatically during `docker compose build`. Triggered during Zefix detail fetch runs and via the "↻ Refresh from Zefix" button on the company detail page. Once `lat`/`lon` are set, they are reused without re-geocoding.

---

## HTTPS setup

A self-signed certificate is used by default. Generate it once:

```bash
bash scripts/gen-certs.sh             # CN=localhost
bash scripts/gen-certs.sh myhost.local  # custom CN + SAN
```

This writes `certs/cert.pem` and `certs/key.pem` (git-ignored). Nginx mounts them and handles TLS termination; the FastAPI app runs on plain HTTP internally.

To use a CA-signed certificate (e.g. from Let's Encrypt via Certbot), replace the two `.pem` files and restart the `nginx` container:

```bash
certbot certonly --standalone -d yourdomain.com
cp /etc/letsencrypt/live/yourdomain.com/fullchain.pem certs/cert.pem
cp /etc/letsencrypt/live/yourdomain.com/privkey.pem   certs/key.pem
docker compose restart nginx
```

---

## Background jobs

All long-running actions are executed through a **persistent DB-backed queue** (`job_runs`).

- Jobs can be queued from the UI: bulk import, batch enrichment, detail fetch, initial import, and score recalculation
- Closing the browser/UI window does not stop jobs
- Reopening `/ui/jobs` shows queued/running/paused/completed/failed/cancelled runs
- Running jobs support cooperative **pause** and **cancel** at the next checkpoint
- Paused jobs preserve their `progress_done` resume point; resuming re-queues from there
- Per-job event stream is persisted in `job_run_events`
- Collection and Jobs pages auto-refresh while active jobs exist

---

## Roadmap

### Near-term

- [x] **Preserve filters on "Back to list"** — pass current URL as `?back=` param so filters survive opening a company detail
- [x] **Inline status dropdowns in table** — change review/proposal status without opening the company page or using bulk actions; updates via `fetch` with no page reload
- [x] **"Not searched vs no result" distinction** — yellow badge for companies that were searched but returned no Google results, dash for never searched; filter dropdown has a dedicated "No result" option

### Medium-term

- [ ] **Scheduler UI** — configure recurring runs directly from the dashboard; view calendar/history
- [x] **AI-assisted scoring** — Claude Haiku batch classification (`claude_score` + `claude_category`); TF-IDF unsupervised clustering (`tfidf_cluster`); configurable industry taxonomy with best-match derivation
- [ ] **Duplicate detection** — flag companies that appear to share a website, suggesting they are related entities
- [ ] **Website scraping — Phase 1 (httpx + BeautifulSoup, no JS rendering)**

  > **Goal:** After Google search stores a `website_url`, fetch that URL and extract human-written company text to enrich clustering, Claude scoring, and lead qualification.  No new heavy dependencies; builds directly on existing job/batch infrastructure.

  **Why it matters:** Zefix purpose text is legal boilerplate.  The company's own website is written to attract customers — far richer input for NLP and scoring.

  **What to extract per company (homepage only in Phase 1):**
  - `<title>` tag
  - `<meta name="description">` content
  - All `<h1>`, `<h2>`, `<h3>` headings
  - Body text from `<main>`, `<article>`, `<section>` (fall back to `<body>` if none present)
  - Email addresses (regex over full page source: `[\w.+-]+@[\w-]+\.[a-z]{2,}`)
  - Phone numbers (regex: Swiss formats `+41`, `041`, `0\d{2}`)
  - Strip: `<nav>`, `<footer>`, `<header>`, `<script>`, `<style>`, `<noscript>`, cookie banners (`id/class` containing "cookie", "consent", "banner", "gdpr")
  - Truncate combined extracted text to 4 000 characters before storing

  **New DB columns** (add via Alembic migration `0016_add_website_scrape_fields.py`):
  ```
  website_text        String(4000)   nullable  — cleaned extracted text
  website_scraped_at  DateTime       nullable  — timestamp of last scrape
  website_scrape_ok   Boolean        nullable  — True=success, False=failed/blocked, None=not attempted
  ```

  **New service function:** `app/services/scraper.py` → `scrape_company_website(company, *, timeout=10) -> dict`
  - Use `httpx` (already in requirements) with a realistic browser User-Agent string
  - Respect `robots.txt`: use `urllib.robotparser` to check before fetching
  - Follow up to 3 redirects
  - Timeout: 10 s connect + 15 s read
  - Return `{"text": str, "ok": bool, "error": str | None}`

  **New batch function:** `scrape_websites_batch(db, *, limit=500, only_unscraped=True, resume_from=0, progress_cb=None) -> dict`
  - Filters: `Company.website_url.isnot(None)` AND (if `only_unscraped`) `Company.website_scraped_at.is_(None)`
  - Rate limit: `time.sleep(1.0)` between requests (configurable `delay_seconds` param)
  - Commit every 50 companies; honour resume_from checkpointing
  - Stats keys: `scraped`, `skipped`, `failed`, `errors`

  **New job type:** `"website_scrape"` — add to job worker `elif` chain in `app/ui/routes.py` alongside existing `hdbscan_cluster`, `claude_classify`, etc.

  **New route + UI:** POST `/ui/enrich/scrape-websites` form in Settings page under a new "Website enrichment" section.  Fields: limit (default 500), delay_seconds (default 1.0), only_unscraped checkbox.

  **Where scraped text feeds:**
  1. `claude_classify_batch` — append `website_text` to the prompt context when available
  2. `cluster_pipeline.run_pipeline` — concatenate `purpose + " " + (website_text or "")` as the text field fed into spaCy preprocessing

  **Skip conditions** (set `website_scrape_ok=False`, do not retry until manually cleared):
  - HTTP 403, 429, 5xx after 2 retries
  - `website_url` domain is a known directory (already detected by existing `DIRECTORY_DOMAINS` list in `scoring.py`)
  - Extracted text < 50 characters after cleaning (blocked page / login wall)
  - `robots.txt` disallows `/`

  **Dependencies to add to `requirements.txt`:**
  ```
  beautifulsoup4>=4.12.0
  lxml>=5.0.0        # faster BS4 parser
  ```

  **Estimated coverage:** ~60–65 % of `website_url` values (the rest are JS-rendered — handled in Phase 2).

- [ ] **Website scraping — Phase 2 (Playwright, JS-rendered sites)**

  > **Goal:** Extend Phase 1 to cover the ~35–40 % of company websites that return a blank or near-empty page with plain httpx because they are React/Vue/Angular single-page apps.  Phase 2 is gated behind a config flag so the Docker image stays lean for deployments that don't need it.

  **Trigger condition:** Run Phase 2 only for companies where `website_scrape_ok = False` AND the raw response body was < 500 characters (indicates a JS shell, not a genuine block).  Companies blocked by 403/robots remain skipped.

  **New dependency:** `playwright>=1.44.0` — add to `requirements.txt` behind a comment `# Phase 2 — JS scraping`.
  Docker: add to `Dockerfile` after existing `pip install`:
  ```dockerfile
  RUN pip install playwright && playwright install chromium --with-deps
  ```
  Gate the install behind a build arg so the base image is unchanged for Phase-1-only deploys:
  ```dockerfile
  ARG INSTALL_PLAYWRIGHT=false
  RUN if [ "$INSTALL_PLAYWRIGHT" = "true" ]; then pip install playwright && playwright install chromium --with-deps; fi
  ```

  **New function in `app/services/scraper.py`:** `scrape_with_playwright(url, *, timeout=20) -> dict`
  - Launch `async_playwright` with `chromium.launch(headless=True)`
  - `page.goto(url, wait_until="networkidle", timeout=timeout*1000)`
  - Extract same selectors as Phase 1 via `page.query_selector_all()`
  - Reuse same cleaning/truncation logic
  - Close browser after each call (no persistent browser instance — keeps memory bounded)

  **Concurrency:** Run at most 2 Playwright instances simultaneously (configurable `playwright_concurrency` in `PipelineConfig`).  Use `asyncio.Semaphore(2)`.  The batch function becomes `async def scrape_websites_batch_playwright(...)` and is run via `asyncio.run()` from the synchronous job worker thread.

  **New job type:** `"website_scrape_js"` — separate from Phase 1's `"website_scrape"` so they can be triggered independently and their progress tracked separately in the Jobs page.

  **Fallback chain (what the job worker runs for a given company):**
  ```
  website_scrape_ok is None  →  try Phase 1 (httpx)
  Phase 1 result: ok=False AND body<500 chars  →  queue for Phase 2 (Playwright)
  Phase 2 result: ok=False  →  mark permanently skipped
  ```

  **Resource envelope:**
  - Docker image: +~500 MB for Chromium
  - RAM: ~150 MB per browser instance × 2 concurrent = ~300 MB peak overhead
  - Speed: ~1–2 pages/s → ~14–28 hrs for 40k JS-sites (run overnight)
  - Use `delay_seconds=0.5` default (faster than Phase 1 since JS sites handle concurrent load better)

  **Estimated combined coverage (Phase 1 + Phase 2):** ~93–96 % of valid `website_url` values.
- [ ] **Concurrent job workers** — replace the single-threaded job worker with a `ThreadPoolExecutor`; use PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` for race-condition-free job pickup; configurable `JOB_WORKER_CONCURRENCY` env var; enables crawler + import + scoring jobs to run in parallel

### Multi-user / public hosting

- [x] **Authentication** — session-based login with bcrypt-hashed passwords; admin user auto-created from `ADMIN_USERNAME` + `ADMIN_PASSWORD` env vars on first startup
- [x] **Audit log** — records who changed which field (website URL, review/proposal status, contact info, industry, tags) and when; visible on each company detail page under "Change history"
- [ ] **User management UI** — add/deactivate users from the dashboard; currently requires direct DB access or re-setting env vars
- [ ] **Password reset** — self-service reset flow or admin-triggered reset link
- [ ] **Role-based access** — read-only viewer role vs full editor role
- [ ] **Per-user quota tracking** — replace the global Google quota counter with per-user accounting
- [ ] **Rate limiting** — throttle Google Search triggers per user to prevent quota exhaustion from concurrent users

---

## API Integrations

Currently integrated APIs and planned enrichment sources.

### Integrated

| API | Purpose | Docs |
|---|---|---|
| **Zefix REST API** | Primary data source — Swiss commercial register (bulk import, per-UID detail) | [swagger](https://www.zefix.admin.ch/ZefixREST/swagger-ui.html) |
| **Serper.dev** | Find company websites; results scored 0–100 against company profile | [serper.dev](https://serper.dev) |

### Website search alternatives

Other options if you want to swap out Serper.dev — all return `title`/`link`/`snippet` and only require changing `google_search_client.py`:

| API | Free tier | Paid | Notes |
|---|---|---|---|
| **Brave Search API** | 2 000/month | $3 / 1 000 queries | Privacy-focused; good .ch coverage |
| **Bing Web Search** (Azure) | 1 000/month | $3–7 / 1 000 queries | Reliable; often indexes Swiss SMEs well |
| **Google Custom Search** | 100/day | $5 / 1 000 queries | Original integration; requires CSE setup |
| **SerpAPI** | 100/month | $50 / 5k queries | Scrapes live Google; highest fidelity |

### Swiss-specific enrichment

| API | What it adds | Notes |
|---|---|---|
| **Moneyhouse** (moneyhouse.ch) | Revenue estimates, employee headcount, balance sheet summaries | Best signal for lead scoring; no public API — requires partnership or scraping |
| **local.ch / search.ch** | Phone numbers, opening hours, customer reviews | Covers most Swiss SMEs; no official API |
| **Swiss Post Address API** | Address validation and normalisation, PLZ lookup | Free for moderate volumes; useful for deduplication |
| **SECO / cantonal registers** | Official cantonal excerpt links (already extracted as `cantonal_excerpt_web`) | Already partially integrated |

### Company enrichment (global, works for .ch companies)

| API | What it adds | Free tier |
|---|---|---|
| **Clearbit Enrichment** | Industry, employee count, revenue range, tech stack, LinkedIn URL, logo | 50 lookups/month free |
| **Apollo.io** | Contact emails, phone numbers, company size, funding rounds | 50 exports/month free |
| **Hunter.io** | Email addresses by domain (auto-populate `contact_email`) | 25 searches/month free |
| **OpenCorporates** | Global company register data including CH; alternative to Zefix for cross-border | Free for non-commercial |
| **Crunchbase** | Startup funding, investor data, founded date | Paid API |

### Lead scoring signals

| API | Signal | Why it helps |
|---|---|---|
| **Google Maps Places** | Ratings, reviews, phone number, business category | Validates the found website URL; rating count signals active business |
| **Wappalyzer API** | Tech stack detection from website | Filter leads by technology — e.g. only companies without a CRM are worth targeting |
| **SimilarWeb** | Monthly traffic estimate | Filters out ghost companies with no web presence |
| **BuiltWith** | Detailed tech stack + CMS/e-commerce platform | Identifies upsell opportunities or disqualifiers |

### CI/CD & infrastructure

| Tool | Purpose |
|---|---|
| **GitHub Actions + SSH** | Deploy on push to `main` — `git pull` → `docker compose build` → `alembic upgrade head` → `docker compose up -d` |
| **Tailscale** | Secure access to the server without opening ports, enables GitHub Actions deployment to a NAT-ed home server |
| **Watchtower** | Auto-pull updated Docker images (alternative to SSH deploy for simple setups) |
