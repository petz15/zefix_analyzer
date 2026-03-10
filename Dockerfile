FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Pre-download GeoNames CH postal code dataset so geocoding works offline
# (file is git-ignored; baking it into the image avoids runtime download failures)
RUN python -c "from app.api.geocoding_client import _load_plz_table; _load_plz_table()" \
    && echo "PLZ geocoding data ready: $(wc -l < data/plz_ch.tsv) entries"


EXPOSE 8000

ENTRYPOINT ["sh", "entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
