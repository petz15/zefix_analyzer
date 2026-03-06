import argparse
from pathlib import Path

from app.api.zefix_client import SWISS_CANTONS
from app.database import SessionLocal
from app.services.collection import bulk_import_zefix, initial_collect, run_batch_collect


def _read_lines(path: str | None) -> list[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect company data from Zefix and Google Search into the local database."
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    # ── bulk ──────────────────────────────────────────────────────────────────
    bulk = subparsers.add_parser(
        "bulk",
        help="Mass-import all companies from Zefix by canton (no Google Search).",
    )
    bulk.add_argument(
        "--canton",
        action="append",
        default=[],
        metavar="XX",
        help=f"Limit to specific canton(s), e.g. --canton ZH --canton BE. "
             f"Defaults to all 26: {', '.join(SWISS_CANTONS)}",
    )
    bulk.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Companies fetched per API request (default: 200, Zefix max: ~500).",
    )
    bulk.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds to sleep between API calls (default: 0.5).",
    )
    bulk.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include companies with an inactive register entry.",
    )
    bulk.add_argument(
        "--resume",
        action="store_true",
        help="Resume the last incomplete bulk import from its checkpoint.",
    )

    # ── batch ─────────────────────────────────────────────────────────────────
    batch = subparsers.add_parser(
        "batch",
        help="Recurring Google Search enrichment over companies already in the DB.",
    )
    batch.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum companies to process (default: 100, matches free Google quota).",
    )
    batch.add_argument("--skip", type=int, default=0, help="Record offset before selecting companies.")
    batch.add_argument(
        "--all-companies",
        action="store_true",
        help="Process all companies, not only those missing a website_url.",
    )
    batch.add_argument("--refresh-zefix", action="store_true", help="Re-fetch full Zefix details before Google step.")
    batch.add_argument("--skip-google", action="store_true", help="Do not run Google Search enrichment.")

    # ── initial ───────────────────────────────────────────────────────────────
    initial = subparsers.add_parser(
        "initial",
        help="One-time import from explicit UIDs or name search terms.",
    )
    initial.add_argument("--name", action="append", default=[], help="Company name search term (repeatable).")
    initial.add_argument("--names-file", help="Text file with one search term per line.")
    initial.add_argument("--uid", action="append", default=[], help="Direct Zefix UID to import (repeatable).")
    initial.add_argument("--uids-file", help="Text file with one Zefix UID per line.")
    initial.add_argument("--search-max-results", type=int, default=25, help="Zefix max results per name search.")
    initial.add_argument("--import-limit-per-name", type=int, default=10, help="How many UIDs to import per name.")
    initial.add_argument("--include-inactive", action="store_true", help="Include inactive companies from Zefix.")
    initial.add_argument("--skip-google", action="store_true", help="Do not run Google Search enrichment.")

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    with SessionLocal() as db:

        # ── bulk ──────────────────────────────────────────────────────────────
        if args.mode == "bulk":
            cantons = args.canton or None

            def _progress(canton: str, offset: int, created: int, skipped: int) -> None:
                print(f"  {canton} +{offset}: {created} created, {skipped} skipped so far", flush=True)

            print(f"Starting bulk Zefix import — cantons: {cantons or 'all 26'}{' (resuming)' if args.resume else ''}")
            stats = bulk_import_zefix(
                db,
                cantons=cantons,
                active_only=not args.include_inactive,
                page_size=args.page_size,
                request_delay=args.delay,
                resume=args.resume,
                progress_cb=_progress,
            )
            print("\nBulk import completed")
            print(f"  Cantons scanned : {stats['cantons_done']}")
            print(f"  Created         : {stats['created']}")
            print(f"  Skipped (exists): {stats['skipped']}")
            print(f"  Errors          : {len(stats['errors'])}")
            for err in stats["errors"]:
                print(f"    - {err}")
            return 0

        # ── batch ─────────────────────────────────────────────────────────────
        if args.mode == "batch":
            print(f"Starting Google batch — limit: {args.limit}, skip: {args.skip}")
            stats = run_batch_collect(
                db,
                limit=args.limit,
                skip=args.skip,
                only_missing_website=not args.all_companies,
                refresh_zefix=args.refresh_zefix,
                run_google=not args.skip_google,
            )
            print("Batch run completed")
            print(f"  Selected        : {stats['selected']}")
            print(f"  Zefix refreshed : {stats['zefix_refreshed']}")
            print(f"  Google enriched : {stats['google_enriched']}")
            print(f"  Google no result: {stats['google_no_result']}")
            print(f"  Errors          : {len(stats['errors'])}")
            for err in stats["errors"]:
                print(f"    - {err}")
            return 0

        # ── initial ───────────────────────────────────────────────────────────
        names = [*args.name, *_read_lines(args.names_file)]
        uids = [*args.uid, *_read_lines(args.uids_file)]
        if not names and not uids:
            parser.error("initial mode requires at least one --name/--names-file or --uid/--uids-file")

        stats = initial_collect(
            db,
            names=names,
            uids=uids,
            search_max_results=args.search_max_results,
            import_limit_per_name=args.import_limit_per_name,
            active_only=not args.include_inactive,
            run_google=not args.skip_google,
        )
        print("Initial run completed")
        print(f"  Created         : {stats['created']}")
        print(f"  Updated         : {stats['updated']}")
        print(f"  Google enriched : {stats['google_enriched']}")
        print(f"  Google no result: {stats['google_no_result']}")
        print(f"  Errors          : {len(stats['errors'])}")
        for err in stats["errors"]:
            print(f"    - {err}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
