# Zefix Analyzer

Internal leads dashboard for Swiss registered companies. Bulk-imports the entire Zefix commercial register, runs Google Search to find each company's website, and provides a GUI to review, score, and track outreach.

* **Zefix API** – bulk-import all ~700k companies from the official Swiss commercial register ([zefix.admin.ch](https://www.zefix.admin.ch/ZefixREST/swagger-ui.html)), canton by canton with resume support
* **Google Custom Search** – automatically find and score each company's website (0–100 match score)
* **Leads dashboard** – filter/sort/paginate companies, bulk-update review and proposal status
* **Company detail** – view enriched data, pick best website from search results, add contact info and notes
* **CSV export** – export any filtered view to CSV
* **PostgreSQL** – all data persisted in Postgres; DB indexes on all filter columns
* **FastAPI + Jinja2** – server-rendered UI, no JS framework required

---

## Quick start (Docker Compose)

```bash
cp .env.example .env
# Edit .env: set GOOGLE_API_KEY and GOOGLE_CSE_ID

docker compose up --build
```

GUI: <http://localhost:8000/ui>
Health check: <http://localhost:8000/health>

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

---

## GUI Workflow

1. **Bulk import** all companies from Zefix (see below) — one-time, ~hours
2. **Batch enrich** with Google Search to find websites — runs daily against free 100-query quota
3. **Dashboard** at `/ui` — filter by canton, industry, tags, review/proposal status, score; sort; paginate; bulk-update
4. **Company detail** — pick best website from Google results, set contact info, add research notes
5. **Export CSV** — download any filtered view

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
* `--skip 200` — record offset, for manual pagination of large runs
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

```bash
alembic upgrade head      # apply all migrations
alembic current           # show current revision
alembic history           # list all revisions
```

Migrations live in `alembic/versions/`. Current chain:

| Revision | Description |
|---|---|
| `0001` | Initial schema (companies, notes) |
| `0002` | Status fields (review, proposal, website score, Google results) |
| `0003` | Filter indexes |
| `0004` | Contact fields, industry, tags, collection_runs table |
| `0005` | App settings table (runtime-configurable Google quota) |
| `0006` | Zefix administrative fields (ehraid, chid, legalSeatId, legalFormId/uid/shortName, sogcDate, deletionDate) |
| `0007` | Extended Zefix detail fields (sogcPub, capital, headOffices, branchOffices, hasTakenOver, oldNames, cantonalExcerptWeb) |

---

## Roadmap

### Near-term

- [ ] **Preserve filters on "Back to list"** — pass referrer or session state so filters survive opening a company detail
- [ ] **Inline status dropdowns in table** — change review/proposal status without opening the company page or using bulk actions
- [ ] **"Not searched vs no result" distinction** — show a visual indicator in the table for companies that were searched but returned no Google results, vs never searched

### Medium-term

- [ ] **Background task queue** — move bulk import and batch enrichment to a task queue (Celery + Redis or FastAPI `BackgroundTasks`) so runs can be triggered and monitored from the UI rather than via CLI/SSH
- [ ] **Scheduler UI** — configure and trigger batch runs from the dashboard; view run history and stats
- [ ] **AI-assisted scoring** — use an LLM to read the company purpose and website snippet to produce a richer match score and auto-suggest industry classification
- [ ] **Duplicate detection** — flag companies that appear to share a website, suggesting they are related entities

### Multi-user / public hosting

- [ ] **Authentication** — login system (session-based or OAuth) before any public exposure; per-user data isolation
- [ ] **Role-based access** — read-only viewer role vs full editor role
- [ ] **Per-user quota tracking** — replace the global Google quota counter with per-user accounting
- [ ] **Rate limiting** — throttle Google Search triggers per user to prevent quota exhaustion from concurrent users
- [ ] **Audit log** — record who changed review/proposal status and when

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
