# Historical bootstrap data

This project has two acquisition modes:

- Historical bootstrap: one-time source acquisition followed by Beam backfills
  to seed warehouse/model history.
- Operational ingestion: automated daily functions -> Pub/Sub -> Beam -> BigQuery.

The historical files were individually downloaded from the source websites,
except cocoa data, which was scraped from ICCO. Manual acquisition is recorded
explicitly; it is not presented as an automated ingestion workflow.

## Authoritative snapshots

[historical_manifest.yaml](historical_manifest.yaml) records source URLs,
acquisition methods, measured coverage, row counts, exact GCS generations,
SHA-256 and successful recovery verification. Acquisition dates were not
recorded; snapshot_date=2025-10-02 is the original GCS upload date.

~~~text
gs://raw-historic-data/snapshots/2025-10-02/
  icco/Daily_Prices_Home.csv
  fred/brent_oil_fred.json
  nasa_power/POWER_Point_Daily.csv
  manifest.yaml
~~~

| Dataset | Measured coverage | Records | Acquisition |
| --- | --- | ---: | --- |
| ICCO cocoa | 2014-01-02 to 2025-10-01 | 3,020 | One-time manual web scrape |
| FRED DCOILBRENTEU | 2014-01-01 to 2025-09-22 | 3,118 | Manual download |
| NASA POWER, 4.745 / -6.635 | 2014-01-01 to 2025-09-29 | 4,290 | Manual download |

Coverage was calculated from records, rather than copied from request/header
date ranges. The NASA header describes the requested span, which is longer
than the observations actually present.

On 2026-10-26, exact original object generations were copied into this layout
without overwriting existing objects. The three snapshots and archived manifest
have [GCS temporary holds](https://docs.cloud.google.com/storage/docs/object-holds),
which prevent replacement/deletion until an authorized
operator explicitly releases the hold. This is object-scoped protection, not
an irreversible bucket retention lock. The original root objects remain intact.

Each new snapshot was downloaded again and SHA-256 checked before full files
were removed from the source tree. The original GCS bytes are authoritative
because these were the historical backfill inputs. Windows/Git line-ending
differences and FRED JSON formatting differences were checked for data
equivalence and recorded in the manifest; no full source was reformatted.

Full working copies are retained locally under ignored data/raw/. They are
absent from the current working source tree, with tracked deletions awaiting
review/commit. Earlier commits were not rewritten.

To recover an exact snapshot, use its generation from the manifest:

~~~bash
gcloud storage cp "gs://raw-historic-data/snapshots/2025-10-02/icco/Daily_Prices_Home.csv#1793012110386226" Daily_Prices_Home.csv
sha256sum Daily_Prices_Home.csv
~~~

Full snapshots require GCS read access. The [backfill pipelines](../pipelines/backfill/README.md)
read the protected snapshot names. Acquisition scripts may be added later;
they are not required to reproduce processing of the archived inputs.

## Small source-format examples

Samples were derived from the unchanged working snapshots before archival.
They are examples, not training data or substitutes for the full snapshots.

| Sample | Selection |
| --- | --- |
| [cocoa_prices.csv](samples/cocoa_prices.csv) | Source lines 1-6: header and first five records, quoted thousands separators, sterling header, and day/month/year dates. |
| [power_daily.csv](samples/power_daily.csv) | Source lines 1-14, 1107 and 4301: complete metadata preamble, header, first three records, first DOY=366 record and final record. Five data rows; only trailing metadata whitespace stripped. The source has no -999 data row, although its missing-value convention is retained in the header. |
| [brent_oil_fred.json](samples/brent_oil_fred.json) | First five observations, including '.', inside the original response envelope. count=5 describes the sample; other original metadata is retained. This derived JSON sample is formatted with indentation and LF. |

Both CSV samples and the JSON sample parse locally and contain five selected
records each. Source/sample checksums remain in the dated migration evidence.
