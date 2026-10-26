# Deferred engineering work

These are behavior/infrastructure improvements, outside the structural
migration. They were not implemented.

| Priority | Finding | Later work |
| --- | --- | --- |
| High | Cocoa observation and published dates differ | Define and test observation/event-date semantics. |
| High | Historical predictions are not a chronological out-of-sample backtest | Implement chronological/rolling-origin evaluation. |
| High | Producer/Airflow failure handling can return or skip | Define feed requirements, retries, and alerts. |
| High | Beam decoding failures are printed | Add invalid-record handling and recovery. |
| High | DLQ topics alone do not establish dead-letter routing | Implement and failure-test routing and monitoring. |
| Medium | Airflow can proceed after one feed succeeds | Define partial-feed freshness/completeness and quiet-period policies. |
| Medium | Backfills overwrite curated tables | Isolate writes and define merge/recovery behavior. |
| Medium | Backfill limits, formats, and parsing/error paths need review | Test malformed/missing rows, real formats, and date bounds. |
| Medium | Dependencies differ among environments and Docker | Reconcile versions separately with compatibility checks. |
| Medium | VM metadata, IAM/firewalls, and runtime ownership need review | Investigate actual bootstrap and access requirements. |
| Medium | Deployment is not fully reproducible from Git | Add controlled application/runtime deployment workflows. |
| Medium | Dated raw snapshots now have verified recovery and object holds; original request/retrieval metadata is incomplete | Preserve the existing manifest and hold policy; capture full provenance for future acquisitions. Holds can be released by authorized administrators and are not a permanent retention lock. |
| Future | Unit/contract coverage exists for logging and consumer transforms; broader runtime coverage remains absent | Add real-runner, integration, data-quality, and model-output tests. |
| Future | CI is configured locally; continuous deployment is absent | Publish and verify the first hosted CI run; add separately reviewed deployment workflows. |
| Future | Observability configuration is outside source | Capture Ops Agent configuration and add reviewed monitoring resources and failure tests. |
| Future | Feature expansion needs reliable evaluation | Compare temporal/exogenous feature sets after evaluation is corrected. |

## Repository fixes prepared on 2026-10-26

- Oil logs no longer print request parameters, HTTP error bodies, raw response
  payloads or arbitrary exception messages. Request failures retain a failure
  signal with a sanitized exception. Canary tests cover success/error/fallback
  paths. The deployed source matched the pre-fix repository bytes; the fixed
  artifact is prepared but has not been deployed.
- Consumer authority is established from captured deployed sources. Cocoa
  preserves producer timestamps; weather retains deployed override aliases.
  Tested row mapping agrees with deployed code while repository compatibility
  options remain. No installed consumer files were overwritten.
- A read-only credential-free GitHub Actions workflow is prepared for Python
  3.11/3.12. Hosted execution awaits commit/push.

Production oil rollout and any historical log-exposure assessment remain
separate operational steps. No secret values were printed or rotated.
See the [reliability handoff](reliability-handoff-2026-10-26.md).
