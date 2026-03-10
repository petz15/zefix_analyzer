"""Offline geocoding for Swiss addresses using the GeoNames postal code dataset.

Data source: GeoNames Switzerland postal codes
  URL:     https://download.geonames.org/export/zip/CH.zip
  License: Creative Commons Attribution 4.0 (CC BY 4.0)
  Size:    ~200 KB compressed / ~800 KB uncompressed

The dataset is downloaded once on first use and cached to ``data/plz_ch.tsv``.
Subsequent calls use the in-memory table — no network required.

Geocoding strategy:
  1. Extract the Swiss PLZ (4-digit postal code) from the address string.
  2. Look up the PLZ in the downloaded table to get (lat, lon).

Accuracy: PLZ centroid (~village level, typically <2 km error).
This is sufficient for the Haversine distance scoring used in this app.
"""

import io
import re
import zipfile
from pathlib import Path

import httpx

_GEONAMES_URL = "https://download.geonames.org/export/zip/CH.zip"

# Cache path relative to the repository root
_CACHE_PATH = Path(__file__).resolve().parents[2] / "data" / "plz_ch.tsv"

# Regex for a Swiss 4-digit PLZ embedded in an address string
_PLZ_RE = re.compile(r"\b(\d{4})\b")

# In-memory table: PLZ string → (lat, lon) — populated lazily on first call
_plz_table: dict[str, tuple[float, float]] | None = None


def _download_plz_data() -> None:
    """Download and cache the GeoNames CH postal code file."""
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(_GEONAMES_URL)
        resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("CH.txt") as src:
            _CACHE_PATH.write_bytes(src.read())


def _load_plz_table() -> dict[str, tuple[float, float]]:
    """Return the PLZ → (lat, lon) dict, downloading the data file if needed."""
    global _plz_table
    if _plz_table is not None:
        return _plz_table

    if not _CACHE_PATH.exists():
        _download_plz_data()

    table: dict[str, tuple[float, float]] = {}
    with _CACHE_PATH.open(encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 11:
                continue
            plz = parts[1].strip()
            try:
                lat = float(parts[9])
                lon = float(parts[10])
            except (ValueError, IndexError):
                continue
            if plz not in table:   # keep first occurrence per PLZ
                table[plz] = (lat, lon)

    _plz_table = table
    return table


def geocode_address(address: str) -> tuple[float, float] | None:
    """Return (lat, lon) for *address* by extracting its Swiss PLZ.

    Uses the locally cached GeoNames postal code dataset — no API call is made
    after the initial one-time download.

    Args:
        address: Free-form address string containing a 4-digit Swiss PLZ,
                 e.g. ``"Musterstrasse 1, 3074 Muri bei Bern"``.

    Returns:
        ``(latitude, longitude)`` float tuple, or ``None`` if no PLZ is found
        in the address or the PLZ is not in the dataset.
    """
    if not address:
        return None

    match = _PLZ_RE.search(address)
    if not match:
        return None

    plz = match.group(1)
    return _load_plz_table().get(plz)
