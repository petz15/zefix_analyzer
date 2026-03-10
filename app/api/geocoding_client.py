"""Geocoding for Swiss addresses using two data sources:

1. Primary: swisstopo Amtliches Gebäudeadressverzeichnis (building-level, <10 m)
   Downloaded from data.geo.admin.ch and indexed into a local SQLite database.
   Data: https://data.geo.admin.ch/ch.swisstopo.amtliches-gebaeudeadressverzeichnis/
   License: Open Government Data (OGD), free for any use

2. Fallback: GeoNames CH postal code dataset (PLZ centroid, ~2 km)
   Downloaded from download.geonames.org/export/zip/CH.zip

Both datasets are downloaded on first use (or during Docker build) and cached
to the ``data/`` directory.  No API key is required.
"""

import csv
import io
import re
import sqlite3
import threading
import zipfile
from pathlib import Path

import httpx

# ── Data paths ────────────────────────────────────────────────────────────────
_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
_PLZ_CACHE = _DATA_DIR / "plz_ch.tsv"
_BUILDING_DB = _DATA_DIR / "geocoding.db"

# ── Download URLs ─────────────────────────────────────────────────────────────
_GEONAMES_URL = "https://download.geonames.org/export/zip/CH.zip"
_BUILDING_URL = (
    "https://data.geo.admin.ch"
    "/ch.swisstopo.amtliches-gebaeudeadressverzeichnis"
    "/amtliches-gebaeudeadressverzeichnis_ch"
    "/amtliches-gebaeudeadressverzeichnis_ch_2056.csv.zip"
)

# ── Regex helpers ─────────────────────────────────────────────────────────────
_PLZ_RE = re.compile(r"\b(\d{4})\b")

# Matches a "street housenumber" segment: the part immediately before the PLZ city segment.
# Groups: street, house
_STREET_HOUSE_RE = re.compile(r"^(?P<street>.+?)\s+(?P<house>\S+)$")

# ── Thread locks ──────────────────────────────────────────────────────────────
_plz_lock = threading.Lock()
_db_lock = threading.Lock()

# ── In-memory PLZ table (fallback) ───────────────────────────────────────────
_plz_table: dict[str, tuple[float, float]] | None = None

# ── SQLite connection (building lookup) ───────────────────────────────────────
_db_conn: sqlite3.Connection | None = None


# ── LV95 → WGS84 conversion ───────────────────────────────────────────────────

def _lv95_to_wgs84(e: float, n: float) -> tuple[float, float]:
    """Convert Swiss LV95 (EPSG:2056) to WGS84 lat/lon.

    Uses the approximate formula published by swisstopo (accuracy < 1 m).
    Reference: swisstopo 'Approximative Umrechnung von ETRS89/WGS84 nach LV95'
    """
    y = (e - 2_600_000.0) / 1_000_000.0
    x = (n - 1_200_000.0) / 1_000_000.0

    lon_sex = (
        2.6779094
        + 4.728982 * y
        + 0.791484 * y * x
        + 0.1306 * y * x ** 2
        - 0.0436 * y ** 3
    )
    lat_sex = (
        16.9023892
        + 3.238272 * x
        - 0.270978 * y ** 2
        - 0.002528 * x ** 2
        - 0.0447 * y ** 2 * x
        - 0.0140 * x ** 3
    )
    return lat_sex * 100.0 / 36.0, lon_sex * 100.0 / 36.0


# ── Normalization ─────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Normalize a street name or house number for consistent lookup."""
    s = s.lower().strip()
    s = re.sub(r"[.\-–/]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# ── PLZ centroid fallback ─────────────────────────────────────────────────────

def _load_plz_table() -> dict[str, tuple[float, float]]:
    global _plz_table
    with _plz_lock:
        if _plz_table is not None:
            return _plz_table

        if not _PLZ_CACHE.exists():
            _DATA_DIR.mkdir(parents=True, exist_ok=True)
            with httpx.Client(timeout=30.0) as client:
                resp = client.get(_GEONAMES_URL)
                resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                with zf.open("CH.txt") as src:
                    _PLZ_CACHE.write_bytes(src.read())

        table: dict[str, tuple[float, float]] = {}
        with _PLZ_CACHE.open(encoding="utf-8") as f:
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
                if plz not in table:
                    table[plz] = (lat, lon)

        _plz_table = table
        return _plz_table


def _plz_fallback(address: str) -> tuple[float, float] | None:
    m = _PLZ_RE.search(address)
    if not m:
        return None
    return _load_plz_table().get(m.group(1))


# ── Building-level geocoding DB ───────────────────────────────────────────────

def build_geocoding_db() -> None:
    """Download and index the swisstopo building address register into SQLite.

    Output: data/geocoding.db  (~300–400 MB on disk, ~4 M addresses)
    Columns used from source CSV (semicolon-delimited, UTF-8 BOM):
      STN_LABEL    – street name
      ADR_NUMBER   – house number designation (e.g. "16", "12a", "5.1")
      ZIP_LABEL    – postal label "4566 Oekingen" — first 4 chars = PLZ
      ADR_EASTING  – LV95 east coordinate
      ADR_NORTHING – LV95 north coordinate
      ADR_STATUS   – "real" | "planned"  (we keep both)
    """
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = _BUILDING_DB.with_suffix(".tmp.db")
    tmp.unlink(missing_ok=True)

    print("Downloading swisstopo building address register (~143 MB)…")
    with httpx.Client(timeout=300.0, follow_redirects=True) as client:
        resp = client.get(_BUILDING_URL)
        resp.raise_for_status()
    print(f"Downloaded {len(resp.content) / 1_048_576:.1f} MB. Building SQLite index…")

    conn = sqlite3.connect(str(tmp))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute(
            "CREATE TABLE addresses "
            "(plz TEXT, street TEXT, house TEXT, lat REAL, lon REAL)"
        )

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            with zf.open(csv_name) as raw:
                reader = csv.DictReader(
                    io.TextIOWrapper(raw, encoding="utf-8-sig"),
                    delimiter=";",
                )
                batch: list[tuple[str, str, str, float, float]] = []
                n_rows = 0
                for row in reader:
                    try:
                        e = float(row["ADR_EASTING"])
                        n_coord = float(row["ADR_NORTHING"])
                        lat, lon = _lv95_to_wgs84(e, n_coord)
                        zip_label = row["ZIP_LABEL"].strip()
                        plz = zip_label[:4]
                        street = _norm(row["STN_LABEL"])
                        house = _norm(row["ADR_NUMBER"])
                        if plz.isdigit() and street:
                            batch.append((plz, street, house, lat, lon))
                            n_rows += 1
                    except (ValueError, KeyError):
                        continue
                    if len(batch) >= 50_000:
                        conn.executemany(
                            "INSERT INTO addresses VALUES (?,?,?,?,?)", batch
                        )
                        batch.clear()
                if batch:
                    conn.executemany(
                        "INSERT INTO addresses VALUES (?,?,?,?,?)", batch
                    )

        print(f"Indexed {n_rows:,} addresses. Creating index…")
        conn.execute(
            "CREATE INDEX idx_addr ON addresses (plz, street, house)"
        )
        conn.commit()
        print("Done.")
    except Exception:
        conn.close()
        tmp.unlink(missing_ok=True)
        raise
    else:
        conn.close()
        tmp.replace(_BUILDING_DB)


def _get_db() -> sqlite3.Connection | None:
    global _db_conn
    with _db_lock:
        if _db_conn is not None:
            return _db_conn
        if not _BUILDING_DB.exists():
            try:
                build_geocoding_db()
            except Exception as exc:
                print(f"[geocoding] Building DB unavailable ({exc}); using PLZ fallback.")
                return None
        _db_conn = sqlite3.connect(str(_BUILDING_DB), check_same_thread=False)
        return _db_conn


_NON_STREET_RE = re.compile(r"^(postfach|c/o|p\.?o\.?\s*box)\b", re.IGNORECASE)


def _parse_address(address: str) -> tuple[str, str, str] | None:
    """Extract (plz, street, house) from a Zefix address string.

    Zefix builds addresses as comma-joined segments:
        [org,] [c/o careOf,] street housenumber, [addon,] [Postfach N,] plz city

    We find the PLZ segment, then search backwards skipping Postfach/addon/c/o
    entries until we find a segment that looks like "street housenumber".
    """
    parts = [p.strip() for p in address.split(",")]
    for i, part in enumerate(parts):
        m_plz = re.match(r"^(\d{4})\b", part)
        if m_plz and i > 0:
            plz = m_plz.group(1)
            # Walk backwards, skipping non-street segments
            for j in range(i - 1, -1, -1):
                candidate = parts[j].strip()
                if _NON_STREET_RE.match(candidate):
                    continue
                m_sh = _STREET_HOUSE_RE.match(candidate)
                if m_sh:
                    return plz, m_sh.group("street"), m_sh.group("house")
                # Segment has no house number (e.g. bare street name) — still usable
                if candidate:
                    return plz, candidate, ""
            break
    return None


def _lookup_building(address: str) -> tuple[float, float] | None:
    parsed = _parse_address(address.strip())
    if not parsed:
        return None

    plz, street, house = parsed
    street = _norm(street)
    house = _norm(house)

    db = _get_db()
    if db is None:
        return None

    try:
        # 1. Exact match
        row = db.execute(
            "SELECT lat, lon FROM addresses WHERE plz=? AND street=? AND house=? LIMIT 1",
            (plz, street, house),
        ).fetchone()
        if row:
            return float(row[0]), float(row[1])

        # 2. Strip trailing letter from house number (e.g. "12a" → "12")
        house_digits = re.sub(r"[^0-9]", "", house)
        if house_digits and house_digits != house:
            row = db.execute(
                "SELECT lat, lon FROM addresses WHERE plz=? AND street=? AND house=? LIMIT 1",
                (plz, street, house_digits),
            ).fetchone()
            if row:
                return float(row[0]), float(row[1])

        # 3. Any address on that street in that PLZ (mid-point of first result)
        row = db.execute(
            "SELECT lat, lon FROM addresses WHERE plz=? AND street=? LIMIT 1",
            (plz, street),
        ).fetchone()
        if row:
            return float(row[0]), float(row[1])

        return None
    except sqlite3.Error:
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def geocode_address(address: str) -> tuple[float, float] | None:
    """Return (lat, lon) for a Swiss address string.

    Resolution order:
    1. swisstopo building register — exact street + house match (<10 m accuracy)
    2. swisstopo building register — street-level match (same PLZ + street name)
    3. GeoNames PLZ centroid (~2 km accuracy)
    """
    if not address:
        return None

    result = _lookup_building(address)
    if result is not None:
        return result

    return _plz_fallback(address)
