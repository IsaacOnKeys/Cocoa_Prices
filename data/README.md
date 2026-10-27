# Historical source data and provenance

Historical acquisition and daily ingestion are separate workflows. The
[manifest](historical_manifest.yaml) identifies each accepted snapshot by
source, acquisition method, observed coverage, checksum and exact GCS generation.

## Accepted snapshots

| Dataset | Coverage | Records | Acquisition |
| --- | --- | ---: | --- |
| ICCO cocoa | 2014-01-02 to 2025-10-01 | 3,020 | Original manual web scrape; protected snapshot copied in October 2026. |
| EIA RBRTE | 2014-01-02 to 2026-10-20 | 3,249 | New extraction of EIA's explicitly dated published weekday table on 2026-10-27. |
| NASA POWER, 4.745 / -6.635 | 2014-01-01 to 2025-09-29 | 4,290 | Original manual download; protected snapshot copied in October 2026. |

The manifest's original snapshot/upload date applies to the initial bootstrap.
Each dataset's acquisition date, URI and generation govern its own provenance;
the new oil acquisition is not represented as a 2025 upload or a manual download.

```text
gs://raw-historic-data/snapshots/2025-10-02/
  icco/Daily_Prices_Home.csv
  nasa_power/POWER_Point_Daily.csv
gs://raw-historic-data/snapshots/2026-10-27/
  eia/brent_oil_eia.json
```

Snapshots use [GCS temporary holds](https://docs.cloud.google.com/storage/docs/object-holds).
Authorized administrators can release these holds; they are not a permanent
retention lock. Recovering the exact new EIA generation and hashing its bytes
reproduced the manifest's SHA-256. Existing original cloud objects have not
been overwritten or relabeled.

## EIA authority and missing dates

The owner accepted the published weekday table as the historical authority.
New daily API acquisitions must agree with that table for their requested
window before any Avro publication. The API and table are not assumed to have
identical historical coverage: [migration evidence](../docs/operations/eia-migration.md)
records the observed differences and the accepted policy.

There are 28 valid dates in the prior supplier archive without a quote in the
new published-table history. Preserve their old provenance; represent them as
absent source observations in the new EIA version rather than silently carrying
them over as EIA quotes. The prior manifest, samples and their original source
names remain recoverable at Git commit
`8b2861236dcc7fbe6ac28bd59f8d67960ec8928b` and in the original held objects.
Historical warehouse replacement has not been implied by updating this manifest.

Oil values are USD per barrel. The existing curated field brent_price_eu refers
to the Europe series and does not indicate an implemented EUR conversion.
EIA source attribution and publication/acquisition dates belong with derived
reports; see [EIA reuse guidance](https://www.eia.gov/about/copyrights_reuse.php).

## Acquisition and recovery

Acquire a new, non-overwriting EIA source representation:

```bash
python scripts/operations/acquire_eia_history.py \
  --interface published-table --start 2014-01-01 \
  --output data/raw/brent_oil_eia.json
```

The optional API acquisition mode retrieves its key from Secret Manager and
requires corroboration against the published table. Credentials, full query
URLs and arbitrary source errors are not printed or archived.

Recover the accepted EIA snapshot exactly:

```bash
gcloud storage cp \
  "gs://raw-historic-data/snapshots/2026-10-27/eia/brent_oil_eia.json#1793113993070636" \
  recovered_brent_oil_eia.json
sha256sum recovered_brent_oil_eia.json
```

Expected SHA-256:
`4adb1bab95f29b3fc3b64bac6cf5b11f0c14c4e7fa9ec6c7b61cf22de09af087`.

Full archives remain in private cloud storage and ignored local directories.
The [backfill scripts](../pipelines/backfill/README.md) load their declared
historical inputs; existing production WRITE_TRUNCATE behavior still requires
the separate safe-backfill/recovery work. Acquisition and archive validation
do not execute those loaders.

## Small source-format samples

| Sample | Selection |
| --- | --- |
| [cocoa_prices.csv](samples/cocoa_prices.csv) | Header and five source records with quoted prices and source date formatting. |
| [power_daily.csv](samples/power_daily.csv) | Original metadata preamble/header and five records including leap-year coverage. |
| [brent_oil_eia.json](samples/brent_oil_eia.json) | First five dated EIA published observations in the normalized acquisition envelope; response.total describes the sample, and original_archive_record_count identifies the full source size. |

Samples illustrate parsing and contracts; they are not substitutes for complete
training history. Missing-value and incompatible-record fixtures live in the
unit tests even when the small source samples contain no missing measurements.
