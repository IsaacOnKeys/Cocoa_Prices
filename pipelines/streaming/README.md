# Retired daily Beam entrypoints

Daily production delivery uses the bounded
[Cloud Run consumer](../../functions/ingestion/README.md). These DirectRunner
scripts preserve previous transforms for historical compatibility checks.
Their executable entrypoints and stream_start refuse startup. The captured old
VM installation remains disabled and preserved as release-recovery material;
changed source is not installed over those copies.

Use [retained-input replay](../../docs/operations/daily-raw-inputs.md) through
the supported consumer. Never compete with production push subscriptions or
change them to pull for a local setup step. Any review of a captured old release
must follow the [handoff/rollback procedure](../../docs/operations/ingestion-cutover.md).

Historical processing remains under [backfill/](../backfill/README.md).
Its runner/dependency reconciliation and safe-write controls are separate
checklist items; retaining Beam does not make existing WRITE_TRUNCATE safe.
