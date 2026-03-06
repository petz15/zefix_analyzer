# Zefix Analyzer

Internal open-source tool for analysing Swiss registered companies.

* **Zefix API** – search and import companies from the official Swiss commercial register ([zefix.admin.ch](https://www.zefix.admin.ch/ZefixREST/swagger-ui.html))
* **Google Custom Search** – automatically find a company's website
* **Notes** – attach free-text notes to any company for manual research
* **PostgreSQL** – all data is persisted in a Postgres database
* **FastAPI** – interactive REST API with auto-generated docs at `/docs`

---

## Quick start (Docker Compose)

```bash
cp .env.example .env
# Edit .env and set GOOGLE_API_KEY and GOOGLE_CSE_ID if needed

docker compose up --build
```

The API will be available at <http://localhost:8000>.  
Interactive docs: <http://localhost:8000/docs>

---

## Local development

### Prerequisites

* Python 3.12+
* PostgreSQL 14+

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your values
```

### Run database migrations

```bash
alembic upgrade head
```

### Start the server

```bash
uvicorn app.main:app --reload
```

---

## Configuration

All settings are read from environment variables (or a `.env` file):

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection URL | `postgresql://user:password@localhost:5432/zefix_analyzer` |
| `ZEFIX_API_BASE_URL` | Zefix REST API base URL | `https://www.zefix.admin.ch/ZefixREST/api/v1` |
| `ZEFIX_API_USERNAME` | HTTP Basic Auth username (optional) | *(empty)* |
| `ZEFIX_API_PASSWORD` | HTTP Basic Auth password (optional) | *(empty)* |
| `GOOGLE_API_KEY` | Google Cloud API key | *(required for Google Search)* |
| `GOOGLE_CSE_ID` | Google Custom Search Engine ID | *(required for Google Search)* |

---

## API Overview

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/companies/zefix/search?name=…` | Search Zefix (no DB write) |
| `GET` | `/api/v1/companies/zefix/{uid}` | Full Zefix company detail |
| `POST` | `/api/v1/companies/zefix/import/{uid}` | Import/update company from Zefix |
| `GET` | `/api/v1/companies` | List companies in DB |
| `POST` | `/api/v1/companies` | Create company manually |
| `GET` | `/api/v1/companies/{id}` | Get company by ID |
| `PATCH` | `/api/v1/companies/{id}` | Update company |
| `DELETE` | `/api/v1/companies/{id}` | Delete company |
| `GET` | `/api/v1/companies/{id}/google-search` | Search Google & save website |
| `GET` | `/api/v1/companies/{id}/notes` | List notes |
| `POST` | `/api/v1/companies/{id}/notes` | Add a note |
| `GET` | `/api/v1/companies/{id}/notes/{nid}` | Get note |
| `PATCH` | `/api/v1/companies/{id}/notes/{nid}` | Update note |
| `DELETE` | `/api/v1/companies/{id}/notes/{nid}` | Delete note |

---

## Running tests

```bash
pytest
```

Tests use an in-memory SQLite database — no PostgreSQL required.
