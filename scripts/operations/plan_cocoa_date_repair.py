"""Build a reviewable cocoa-date repair candidate from captured inputs; no cloud writes."""

import argparse
import hashlib
import json
import math
import re
from datetime import date, datetime
from pathlib import Path


def valid_date(value):
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("Expected canonical source date")
    return value


def price(value):
    if isinstance(value, bool):
        raise ValueError("Boolean is not a cocoa price")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError("Expected finite positive cocoa price")
    return parsed


def fingerprint(rows):
    normalized = sorted(json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False) for row in rows)
    return hashlib.sha256("\n".join(normalized).encode()).hexdigest()


def render_transaction(sql, names):
    """Bind only the fixed production pair or matching isolated rehearsal tables."""
    suffixes = {"STAGING_BEFORE": "staging_before", "CURATED_BEFORE": "curated_before",
                "CANDIDATE_STAGING": "candidate_staging", "CANDIDATE_CURATED": "candidate_curated",
                "LEGACY_ARCHIVE": "legacy_observations"}
    expected_keys = {"ACTIVE_STAGING", "ACTIVE_CURATED", *suffixes}
    if set(names) != expected_keys or set(re.findall(r"\$\{([A-Z_]+)\}", sql)) != expected_keys:
        raise ValueError("Unexpected transaction placeholders")
    namespace = names["STAGING_BEFORE"].rsplit(".", 1)[0]
    if not re.fullmatch(r"cocoa-prices-430315\.cocoa_date_repair_[0-9]{8}_[0-9a-f]{8}", namespace):
        raise ValueError("Expected isolated recovery namespace")
    if any(names[key] != f"{namespace}.{suffix}" for key, suffix in suffixes.items()):
        raise ValueError("Recovery tables must belong to the same repair")
    active = names["ACTIVE_STAGING"], names["ACTIVE_CURATED"]
    if active not in (
        ("cocoa-prices-430315.stream_staging.cocoa_prices", "cocoa-prices-430315.cocoa_related.cocoa"),
        (f"{namespace}.rehearsal_staging", f"{namespace}.rehearsal_curated"),
    ):
        raise ValueError("Unexpected or mixed repair destinations")
    rendered = re.sub(r"\$\{([A-Z_]+)\}", lambda match: names[match[1]], sql)
    if "${" in rendered:
        raise ValueError("Unresolved transaction placeholder")
    return rendered


def build_plan(staging, curated, publisher, *, archive_reference):
    if publisher.get("publisher") != "ICCO" or publisher.get("units") != "EUR/metric_tonne":
        raise ValueError("Wrong publisher or measurement units")
    if publisher.get("source_url") != "https://www.icco.org/statistics/":
        raise ValueError("Expected publisher's dated daily-price source")
    acquired = datetime.fromisoformat(publisher["acquired_at"])
    if acquired.tzinfo is None:
        raise ValueError("Publisher acquisition time must be timezone-aware")
    if not staging:
        raise ValueError("No legacy observations to assess")
    for row in staging:
        valid_date(row["date"])
        price(row["cocoa_price"])
        if json.loads(row["raw_payload"]).get("date_basis"):
            raise ValueError("Captured staging already includes a corrected producer; refresh the plan")
    lower, upper = min(r["date"] for r in staging), max(r["date"] for r in staging)
    if upper > acquired.date().isoformat():
        raise ValueError("Legacy interval includes future dates")
    published = {}
    for row in publisher["observations"]:
        observed, value = valid_date(row["date"]), price(row["cocoa_price"])
        if observed > acquired.date().isoformat():
            raise ValueError("Publisher supplied a future observation")
        if observed in published and published[observed] != value:
            raise ValueError("Conflicting publisher observations")
        published[observed] = value
    if not published or min(published) > lower:
        raise ValueError("Published history does not cover the affected start boundary")
    replacement = {d: v for d, v in published.items() if lower <= d <= upper}
    if not replacement:
        raise ValueError("No dated source observations cover the affected interval")
    before = {}
    for row in curated:
        observed, value = valid_date(row["date"]), price(row["cocoa_price"])
        if observed in before:
            raise ValueError("Curated duplicate dates need separate reconciliation")
        before[observed] = value
    after = {d: v for d, v in before.items() if not lower <= d <= upper}
    after.update(replacement)
    candidate_staging = []
    for observed, value in sorted(replacement.items()):
        raw = {"source": "ICCO", "source_url": publisher["source_url"], "date": observed, "price": value,
               "units": publisher["units"], "date_basis": "publisher_dated_history", "acquisition_archive": archive_reference}
        candidate_staging.append({"date": observed, "cocoa_price": value, "ingestion_time": acquired.isoformat(),
                                  "raw_payload": json.dumps(raw, separators=(",", ":"), allow_nan=False)})
    removed = sorted(set(before) - set(after))
    added = sorted(set(after) - set(before))
    changed = sorted(d for d in set(before) & set(after) if before[d] != after[d])
    return {
        "version": 1, "interval_start": lower, "interval_end": upper,
        "source_acquired_at": acquired.isoformat(), "source_archive": archive_reference,
        "before_staging_fingerprint": fingerprint(staging), "before_curated_fingerprint": fingerprint(curated),
        "summary": {"legacy_rows_to_preserve_and_quarantine": len(staging), "dated_staging_rows": len(candidate_staging),
                    "curated_rows_before": len(before), "curated_rows_after": len(after),
                    "removed_unsupported_dates": len(removed), "added_published_dates": len(added), "changed_prices": len(changed)},
        "removed_dates": removed, "added_dates": added, "changed_dates": changed,
        "candidate_staging": candidate_staging,
        "candidate_curated": [{"date": d, "cocoa_price": v} for d, v in sorted(after.items())],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--curated", type=Path, required=True)
    parser.add_argument("--publisher", type=Path, required=True)
    parser.add_argument("--archive-reference", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = build_plan(json.loads(args.staging.read_text())["rows"], json.loads(args.curated.read_text())["rows"],
                      json.loads(args.publisher.read_text()), archive_reference=args.archive_reference)
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(plan, output, indent=2, allow_nan=False)
        output.write("\n")
    print(json.dumps(plan["summary"]))


if __name__ == "__main__":
    main()
