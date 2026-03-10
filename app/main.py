import asyncio
import time
from contextlib import asynccontextmanager

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from sqlalchemy import inspect as sa_inspect

from app.database import Base, engine
from app.services.scoring import get_default_scoring_config
from app.ui.routes import kick_job_worker, router as ui_router


# ── Startup helpers ───────────────────────────────────────────────────────────

def _database_has_tables() -> bool:
    """Return True if the database already has any tables."""
    with engine.connect() as conn:
        return bool(sa_inspect(conn).get_table_names())


def _run_migrations(app_state) -> None:
    cfg = AlembicConfig("alembic.ini")

    app_state.startup_message = "Connecting to database…"
    try:
        has_tables = _database_has_tables()
    except Exception as exc:
        raise RuntimeError(f"Cannot connect to database: {exc}") from exc

    # ── Fresh database: create all tables in one shot, then stamp Alembic ─────
    if not has_tables:
        app_state.startup_message = "Empty database — creating schema…"
        try:
            Base.metadata.create_all(engine)
        except Exception as exc:
            raise RuntimeError(f"Failed to create schema: {exc}") from exc

        app_state.startup_message = "Schema created — stamping Alembic version…"
        try:
            alembic_command.stamp(cfg, "head")
        except Exception as exc:
            raise RuntimeError(f"Failed to stamp Alembic version: {exc}") from exc

        app_state.startup_message = "Database initialised ✓"
        return

    # ── Existing database: apply all pending migrations in one call ────────────
    app_state.startup_message = "Applying pending migrations…"
    try:
        alembic_command.upgrade(cfg, "head")
    except Exception as exc:
        raise RuntimeError(f"Database migration failed: {exc}") from exc

    app_state.startup_message = "Database schema is up to date ✓"


def _seed_settings(app_state) -> None:
    app_state.startup_message = "Seeding application settings…"
    from app.config import settings
    from app.crud import seed_defaults
    from app.database import SessionLocal

    defaults = {
        "google_search_enabled": "true" if settings.google_search_enabled else "false",
        "google_daily_quota": str(settings.google_daily_quota),
    }
    defaults.update(get_default_scoring_config())
    try:
        with SessionLocal() as db:
            seed_defaults(db, defaults)
    except Exception as exc:
        raise RuntimeError(f"Failed to seed settings: {exc}") from exc


def _maybe_enqueue_geocode_upgrade(app, app_state) -> None:
    """Queue the one-time re-geocode job if it hasn't been completed yet.

    The job itself will trigger the geocoding DB download if it doesn't exist yet.
    """
    from app.crud import create_event, create_job, get_setting, list_jobs
    from app.database import SessionLocal

    queued_job = False
    with SessionLocal() as db:
        if get_setting(db, "geocoding_building_level_done", "false") == "true":
            return  # already upgraded

        already_queued = any(
            j.job_type == "re_geocode" and j.status in ("queued", "running", "paused")
            for j in list_jobs(db, limit=50)
        )
        if already_queued:
            return

        job = create_job(
            db,
            job_type="re_geocode",
            label="One-time re-geocode — upgrade to building-level coordinates",
            params={},
        )
        create_event(db, job_id=job.id, level="info", message="Auto-queued by startup")
        queued_job = True

    if queued_job:
        # Ensure the auto-queued upgrade job does not wait for a UI request.
        kick_job_worker(app)
    app_state.startup_message = "Queued one-time geocoding upgrade"


def _recover_jobs_and_start_worker(app, app_state) -> None:
    """Requeue interrupted jobs and ensure queued jobs resume on startup."""
    from app.crud import list_active_jobs, requeue_interrupted_jobs
    from app.database import SessionLocal

    app_state.startup_message = "Recovering background jobs…"
    try:
        with SessionLocal() as db:
            recovered = requeue_interrupted_jobs(db)
            active_count = len(list_active_jobs(db))
        kick_job_worker(app)
        app_state.startup_message = (
            f"Background jobs ready — recovered {recovered}, active {active_count}"
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to recover background jobs: {exc}") from exc


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.ready = False
    app.state.startup_message = "Initialising…"
    app.state.startup_error = None
    app.state.startup_started_at = time.time()
    app.state.collection_task = None  # populated while a collection job runs
    app.state.job_worker_running = False

    async def _startup() -> None:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _run_migrations, app.state)
            await loop.run_in_executor(None, _seed_settings, app.state)
            await loop.run_in_executor(None, _recover_jobs_and_start_worker, app, app.state)
            await loop.run_in_executor(None, _maybe_enqueue_geocode_upgrade, app, app.state)
            app.state.ready = True
            app.state.startup_message = "Ready"
        except Exception as exc:  # noqa: BLE001
            app.state.startup_error = str(exc)

    asyncio.create_task(_startup())
    yield


# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Zefix Analyzer",
    description=(
        "Internal GUI tool for analysing Swiss registered companies via the Zefix API, "
        "Google Search enrichment, and manual notes stored in PostgreSQL."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(ui_router)


# ── Startup gate middleware ───────────────────────────────────────────────────

_LOADING_HTML = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="2">
  <title>Zefix Analyzer — Starting</title>
  <link rel="stylesheet" href="/static/styles.css">
  <style>
    .startup-box {{ max-width: 520px; margin: 5rem auto; text-align: center; }}
    .spinner {{
      width: 48px; height: 48px;
      border: 5px solid #d7dee7; border-top-color: #146c94;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
      margin: 0 auto 1.5rem;
    }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
    .step-msg {{ font-weight: 600; margin: 0.5rem 0 0.2rem; }}
    .elapsed {{ color: #4d6274; font-size: 0.82rem; margin-top: 0.8rem; }}
  </style>
</head>
<body>
  <header class="site-header">
    <div class="container"><a class="brand" href="/ui">Zefix Analyzer</a></div>
  </header>
  <main class="container">
    <div class="startup-box card">
      <div class="spinner"></div>
      <h2 style="margin-bottom:0.3rem;">Starting up…</h2>
      <p class="step-msg">{message}</p>
      <p class="elapsed">Elapsed: {elapsed}s &nbsp;·&nbsp; page refreshes every 2 s</p>
    </div>
  </main>
</body>
</html>
"""

_ERROR_HTML = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Zefix Analyzer — Startup failed</title>
  <link rel="stylesheet" href="/static/styles.css">
</head>
<body>
  <header class="site-header">
    <div class="container"><a class="brand" href="/ui">Zefix Analyzer</a></div>
  </header>
  <main class="container">
    <div class="card" style="margin-top:2rem;border-color:#a43d3d;">
      <h2 style="color:#a43d3d;margin-top:0;">Startup failed</h2>
      <p>The application could not complete startup. Fix the issue below and restart the container.</p>
      <pre style="background:#fdecec;border-color:#a43d3d;color:#7f1f1f;white-space:pre-wrap;word-break:break-word;">{error}</pre>
      <p class="muted-hint">Elapsed before failure: {elapsed}s</p>
    </div>
  </main>
</body>
</html>
"""


@app.middleware("http")
async def startup_gate(request: Request, call_next):
    path = request.url.path
    if path.startswith("/static") or path == "/health":
        return await call_next(request)

    elapsed = int(time.time() - getattr(app.state, "startup_started_at", time.time()))
    error = getattr(app.state, "startup_error", None)

    if error:
        return HTMLResponse(
            _ERROR_HTML.format(error=error, elapsed=elapsed), status_code=500
        )

    if not getattr(app.state, "ready", False):
        message = getattr(app.state, "startup_message", "Initialising…")
        return HTMLResponse(
            _LOADING_HTML.format(message=message, elapsed=elapsed), status_code=503
        )

    return await call_next(request)


@app.get("/health", tags=["health"])
def health():
    ready = getattr(app.state, "ready", False)
    error = getattr(app.state, "startup_error", None)
    elapsed = int(time.time() - getattr(app.state, "startup_started_at", time.time()))
    message = getattr(app.state, "startup_message", "")
    if error:
        return {"status": "error", "detail": error, "elapsed_s": elapsed}
    return {"status": "ok" if ready else "starting", "step": message, "elapsed_s": elapsed}
