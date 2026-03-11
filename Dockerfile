FROM python:3.12-slim

WORKDIR /app

ARG BUILD_DATE=unknown
ARG BUILD_GIT_SHA=unknown

LABEL org.opencontainers.image.created=$BUILD_DATE
LABEL org.opencontainers.image.revision=$BUILD_GIT_SHA

ENV APP_BUILD_DATE=$BUILD_DATE
ENV APP_GIT_SHA=$BUILD_GIT_SHA

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Build geocoding datasets (both git-ignored, downloaded during image build):
# 1. GeoNames PLZ centroid table (~800 KB) — PLZ-level fallback
# 2. swisstopo Amtliches Gebäudeadressverzeichnis (~143 MB zip) — building-level primary
RUN python -c "from app.api.geocoding_client import _load_plz_table; _load_plz_table()" \
    && echo "PLZ table ready: $(wc -l < data/plz_ch.tsv) entries"
RUN python -c "from app.api.geocoding_client import build_geocoding_db; build_geocoding_db()" \
    && echo "Building DB ready: $(du -sh data/geocoding.db)"


EXPOSE 8000

ENTRYPOINT ["sh", "entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
