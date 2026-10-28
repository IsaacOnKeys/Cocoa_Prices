# Bounded daily source requests

Checklist item 6 applies to the ICCO, EIA and NASA POWER producers. All use
the same packaged `request_policy.py`; the staging consumer's existing bounded
BigQuery request is unchanged. Source dates, timestamp provenance, Avro contracts,
weather coordinates/time standard and EIA API/published-table agreement remain
unchanged.

Each producer starts a 50-second cooperative budget within its configured
60-second function request limit. HTTP acquisition shares the first 40 seconds;
publication retains the remaining budget. Each HTTP attempt has explicit connect
and read inactivity limits, at most 5 and 20 seconds respectively, reduced when
the budget is short. EIA pagination and corroboration share one deadline.
Secret Manager access has a 10-second timeout with SDK retries disabled.

Only transport errors and HTTP 408, 429, 500, 502, 503 and 504 are retried, at
most three attempts per request, with 0.5/1-second backoff. A valid `Retry-After`
delay in seconds or HTTP-date takes precedence. If it cannot fit the remaining
budget, acquisition fails without retrying early. Other HTTP failures and invalid
source payloads fail immediately. Response bodies, query strings and SDK exception
messages are excluded from failure output.

Every publication checks the remaining invocation budget before submission and
waits at most 10 seconds, reduced to the remaining budget. A weather or oil
partial batch fails the invocation. A timed-out publish can still complete;
downstream date-key reconciliation remains necessary. This does not provide
exactly-once delivery or change Eventarc/Airflow retry policy.

Requests timeouts measure socket inactivity, not total DNS or body-transfer time.
The application checks elapsed time between operations and rejects late HTTP
responses; the 60-second platform request limit is a further response bound,
not proof that background SDK threads or all underlying work are terminated.

Credential-free fixtures cover persistent and recovered transport/status errors,
rate limits, shared deadlines, expired publish waits, partial weather publication,
and canaries in response bodies and exception messages. Packaging tests verify
all four producer inputs byte-for-byte and exclude private/generated files.

Deployment and controlled daily-cycle validation are pending.
