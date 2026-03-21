#!/usr/bin/env python
"""Identify boilerplate sentences in company purpose texts.

Can be run inside the container OR from your local machine as long as the
PostgreSQL port is reachable.

Usage:
    # Uses DB credentials from .env (default)
    python scripts/analyze_boilerplate.py [--top N] [--min-freq F] [--insert]

    # Override the DB URL directly (useful when running outside the container)
    python scripts/analyze_boilerplate.py --db-url postgresql://user:pass@localhost:5432/zefix_analyzer --insert

Dependencies (if running outside the container):
    pip install sqlalchemy psycopg2-binary pydantic-settings

Steps:
  1. Loads all purpose texts from DB.
  2. Splits each into sentences.
  3. Counts how often each unique sentence appears across companies.
  4. Prints the top-N most frequent sentences (likely boilerplate).
  5. If --insert is passed, prompts you interactively to accept/skip/edit
     each candidate and inserts accepted ones into boilerplate_patterns.
"""

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

# Make sure the app package is importable when running from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


from contextlib import contextmanager

@contextmanager
def _make_session(db_url: str | None):
    """Yield a SQLAlchemy Session, optionally overriding the URL from .env."""
    if db_url:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        db = Session()
    else:
        from app.database import SessionLocal
        db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


from app.models.company import Company
from app import crud

_SENTENCE_SPLIT = re.compile(r"(?<=\.)\s+(?=[A-ZÄÖÜ])")

# Minimum sentence length to consider (chars) — filters out very short fragments
MIN_SENTENCE_LEN = 30
# Normalise whitespace for deduplication but keep original for display
_WS = re.compile(r"\s+")


def _normalise(s: str) -> str:
    return _WS.sub(" ", s.strip()).lower()


def load_sentences(db) -> Counter:
    """Return a Counter of normalised sentence → occurrence count."""
    counter: Counter = Counter()
    q = db.query(Company.purpose).filter(Company.purpose.isnot(None))
    for (purpose,) in q:
        for sentence in _SENTENCE_SPLIT.split(purpose.strip()):
            s = sentence.strip()
            if len(s) >= MIN_SENTENCE_LEN:
                counter[_normalise(s)] += 1
    return counter


def build_example_map(db, top_normalised: set[str]) -> dict[str, str]:
    """Return original (non-lowercased) example for each normalised sentence."""
    examples: dict[str, str] = {}
    q = db.query(Company.purpose).filter(Company.purpose.isnot(None))
    for (purpose,) in q:
        for sentence in _SENTENCE_SPLIT.split(purpose.strip()):
            s = sentence.strip()
            n = _normalise(s)
            if n in top_normalised and n not in examples:
                examples[n] = s
            if len(examples) == len(top_normalised):
                return examples
    return examples


def sentence_to_pattern(sentence: str) -> str:
    """Convert a literal sentence to a conservative regex.

    Escapes the sentence, then relaxes internal whitespace to \\s+ so minor
    formatting differences still match.
    """
    escaped = re.escape(sentence)
    # relax whitespace
    relaxed = re.sub(r"\\ ", r"\\s+", escaped)
    return relaxed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--top", type=int, default=40, help="Show top N sentences (default 40)")
    parser.add_argument("--min-freq", type=int, default=5, help="Minimum occurrence count (default 5)")
    parser.add_argument("--insert", action="store_true", help="Interactively insert accepted patterns into DB")
    parser.add_argument(
        "--db-url",
        default=None,
        help="Override DB connection URL (e.g. postgresql://user:pass@localhost:5432/zefix_analyzer). "
             "If omitted, reads from .env via app.config.",
    )
    args = parser.parse_args()

    with _make_session(args.db_url) as db:
        print("Loading purpose texts from DB…")
        counter = load_sentences(db)
        total_companies = db.query(Company).filter(Company.purpose.isnot(None)).count()
        print(f"  {total_companies} companies with purpose text")
        print(f"  {len(counter)} unique sentences found\n")

        candidates = [
            (sentence, count)
            for sentence, count in counter.most_common(args.top * 5)
            if count >= args.min_freq
        ][: args.top]

        if not candidates:
            print("No candidates found with the given filters.")
            return

        top_set = {s for s, _ in candidates}
        examples = build_example_map(db, top_set)

        print(f"{'Rank':<5} {'Count':>6}  {'% companies':>11}  Sentence")
        print("-" * 90)
        for rank, (norm, count) in enumerate(candidates, 1):
            pct = count / total_companies * 100
            preview = (norm[:80] + "…") if len(norm) > 80 else norm
            print(f"{rank:<5} {count:>6}  {pct:>10.1f}%  {preview}")

        if not args.insert:
            print("\nRe-run with --insert to interactively add patterns to the DB.")
            return

        # ── Interactive insertion ──────────────────────────────────────────────
        existing = {row.pattern for row in crud.list_boilerplate_patterns(db)}
        added = 0

        print("\n" + "=" * 90)
        print("Interactive mode — for each candidate:")
        print("  [y] Accept as-is (auto-generated regex)")
        print("  [e] Edit the regex before inserting")
        print("  [s] Skip")
        print("  [q] Quit\n")

        for rank, (norm, count) in enumerate(candidates, 1):
            example = examples.get(norm, norm)
            pct = count / total_companies * 100
            print(f"\n[{rank}/{len(candidates)}]  {count}× ({pct:.1f}%)  —  {example[:120]}")

            suggested_pattern = sentence_to_pattern(example)
            print(f"  Suggested regex: {suggested_pattern[:100]}")

            while True:
                choice = input("  Accept [y/e/s/q]? ").strip().lower()
                if choice in ("y", "e", "s", "q"):
                    break

            if choice == "q":
                break
            if choice == "s":
                continue

            final_pattern = suggested_pattern
            if choice == "e":
                print(f"  Current pattern (edit and press Enter):")
                try:
                    import readline  # noqa: F401  (enables editing on Linux/Mac)
                except ImportError:
                    pass
                final_pattern = input(f"  > ") or suggested_pattern

            # Validate regex
            try:
                re.compile(final_pattern, re.IGNORECASE)
            except re.error as exc:
                print(f"  ✗ Invalid regex: {exc} — skipping")
                continue

            if final_pattern in existing:
                print("  ✗ Pattern already exists in DB — skipping")
                continue

            description = input("  Description (optional, Enter to skip): ").strip() or None

            crud.create_boilerplate_pattern(
                db,
                pattern=final_pattern,
                description=description,
                example=example,
                match_count=count,
                active=True,
            )
            existing.add(final_pattern)
            added += 1
            print(f"  ✓ Inserted (id auto-assigned)")

        print(f"\nDone. {added} pattern(s) inserted.")


if __name__ == "__main__":
    main()
