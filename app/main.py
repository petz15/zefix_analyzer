from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.ui.routes import router as ui_router

app = FastAPI(
    title="Zefix Analyzer",
    description=(
        "Internal GUI tool for analysing Swiss registered companies via the Zefix API, "
        "Google Search enrichment, and manual notes stored in PostgreSQL."
    ),
    version="0.1.0",
)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(ui_router)


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}
