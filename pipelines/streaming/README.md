# Local and rollback Beam consumers

Daily production delivery uses the bounded
[Cloud Run consumer](../../functions/ingestion/README.md). These DirectRunner
scripts preserve the previous transforms for local compatibility checks and
emergency rollback. They are not the supported daily production runtime.

Use isolated pull subscriptions and explicit table overrides for development.
Do not run these scripts against production push subscriptions or change a
production subscription to pull as a local setup step. The recorded
[handoff/rollback procedure](../../docs/operations/ingestion-cutover.md)
coordinates delivery modes and old service state.

Historical processing remains under [backfill/](../backfill/README.md).
Its runner/dependency reconciliation and safe-write controls are separate
checklist items; retaining Beam does not make existing WRITE_TRUNCATE safe.
