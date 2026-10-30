# Read-only analytics watchdog

The private `check_analytics_delivery` handler runs independently of Airflow.
See [alert policy, operation and evidence](../../docs/operations/actionable-alerts.md).

Runtime inputs are only `main.py` and `requirements.txt`; no source schemas,
publisher request policy, model library or secret is packaged. Environment:
`GCP_PROJECT`, `WATCH_SUBSCRIPTIONS` (the five managed source subscriptions).

Cloud Run IAM accepts the Scheduler identity. Request bodies cannot override
the clock or underlying health evidence. An authenticated `{"probe":true}`
request performs read-only checks with alert emission suppressed. Normal
Scheduler calls use the real Berlin operating window.

Exceptions produce a sanitized `monitor_failed` event and HTTP 503 for the
single configured Scheduler retry. Platform/Scheduler errors are also covered
by the independent log alert policy. Structured logs omit credentials and
upstream HTTP/SDK bodies.
