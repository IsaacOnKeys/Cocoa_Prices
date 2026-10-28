# Producer failure signals

Checklist item 7 makes source acquisition failure visible to the actual HTTP and
event adapters. Python 3.11 remains the runtime. The producers explicitly pin
Functions Framework 3.8.3, matching the consumer and credential-free adapter
tests, rather than relying on an untested framework selected at build time.

ICCO returns HTTP 500 on acquisition/publication failure. EIA and NASA POWER
raise sanitized exceptions so their background-event adapters return HTTP 500.
Request, credential, malformed-response and publish failures do not emit a
successful acquisition receipt. An acknowledged prefix of a failed batch can
already exist downstream; retries and reconciliation must account for it.
Trigger retry ownership is unchanged here and belongs to item 8.

NASA POWER validates both parameter maps, nonempty matching date keys, canonical
dates inside the requested window and finite numeric measurements before any
row is published. It no longer silently intersects mismatched parameter dates
or coerces malformed measurements into missing observations. Explicit source
nulls and values at/below -999 remain nullable. Publisher initialization and
partial publication fail the invocation with sanitized messages.

A successful invocation emits one structured acquisition_complete log after
all publish futures acknowledge. Fields include source, status, published count,
actual observation range and aware acquisition time. The receipt does not claim
freshness, new observations or a complete analytical release. ICCO's existing
HTTP response shape is preserved; event handlers return the receipt internally,
while the framework supplies the HTTP acknowledgement.

Expected source lag is distinct from an acquisition failure. An ICCO response
with a valid older exposed quote keeps its real date and can publish successfully.
An empty EIA API window is valid only after corroboration with its published
table; it emits no_observations with zero publishes and null observation bounds.
An EIA disagreement or malformed response fails instead. Empty NASA parameter
maps are invalid, rather than evidence of a source holiday. Receipt status is
not permission to release stale/empty inputs: the approved age, coverage and
required-feed gates remain the responsibility of items 13–16.

Credential-free tests use the real Functions Framework Flask HTTP handler and
Gen2 Pub/Sub CloudEvent conversion with packaged roots, real parsing/Avro and
stubbed external IO. They check HTTP status, absence of completion receipts and
credential canaries in response bodies, stdout/stderr and exception logs. Unit
tests also cover mismatched series, invalid later records before any publication,
out-of-window/noncanonical dates, nonfinite/boolean measurements and failed
publisher initialization. Network access remains blocked throughout these tests.

Deployment and controlled-cycle validation are pending.
