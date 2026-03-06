from fastapi import FastAPI

from app.api.routes.companies import router as companies_router
from app.api.routes.notes import router as notes_router

app = FastAPI(
    title="Zefix Analyzer",
    description=(
        "Internal tool for analysing Swiss registered companies via the Zefix API, "
        "Google Search, and manual notes stored in PostgreSQL."
    ),
    version="0.1.0",
)

app.include_router(companies_router, prefix="/api/v1")
app.include_router(notes_router, prefix="/api/v1")


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}
