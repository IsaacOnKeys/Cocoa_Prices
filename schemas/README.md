# Source and schema contracts

Version 1 source definitions for checklist item 2, reviewed 2026-10-27. The
owner accepted EIA published-table history with corroborated daily API
acquisition. The serving producer implements EIA validation and compatibility checks;
the remaining feed-wide publication gates are later checklist work. Existing
Avro wire schemas remain unchanged.

The [service contract](../docs/operations/service-contract.md) defines required
feeds, accepted delivery/recovery targets, source-age limits and withholding
the entire candidate release when any mandatory product fails.

## Identity, grain and units

Isaac, the repository owner, owns the three project contracts. The originating
publisher remains authoritative for observations and revisions. A source
revision is a new version of the same logical observation, not a second fact.

| Feed | Current acquisition source | Observation key and grain | Measurement semantics |
| --- | --- | --- | --- |
| cocoa | ICCO daily-price table, EUR column | ICCO series + source observation date; one quoted daily value | cocoa_price / curated Euro_Price: EUR per metric tonne. No FX conversion is implied. |
| oil | EIA RBRTE: published weekday table for history; API observations corroborated against it for daily ingestion | Europe Brent spot FOB series + observation date; one published daily value | oil_price / curated brent_price_eu: USD per barrel. The eu suffix denotes the existing Europe-series identifier; it is not evidence of conversion to EUR. |
| weather | NASA POWER daily point API, community AG, latitude 4.745, longitude -6.635 | Dataset/product + fixed point + parameter + observation date; the wire row combines two parameters for one date | precipitation: PRECTOTCORR, daily corrected precipitation in mm/day. soil_moisture: GWETROOT, dimensionless root-zone wetness; do not label it volumetric soil moisture. Verify parameter units in the acquired response before acceptance. |

Do not silently mix a new point, parameter, price currency, averaging
frequency or data product into an existing observation key. Persist dataset,
source and parameter identity in acquisition provenance, even where the
legacy wire format relies on a feed-specific topic.

Source definitions:
[ICCO table column labels](https://www.icco.org/),
[EIA Europe Brent daily table](https://www.eia.gov/dnav/pet/hist/RBRTED.htm),
[NASA daily API and community behavior](https://power.larc.nasa.gov/docs/services/api/temporal/daily/),
and [NASA wetness-versus-volumetric moisture distinction](https://gs6101-gmao.gsfc.nasa.gov/gmao-products/merra/faq_merra/).

## Existing binary wire contract: version 1

The canonical schemas are [cocoa](cocoa_schema.avsc), [oil](oil_schema.avsc)
and [weather](weather_schema.avsc). Each topic carries Avro schemaless binary
records under its configured schema. Field order, record name, unions and
defaults are part of the deployed interface. Do not infer an embedded writer
schema from the payload.

| Field | Existing wire type/default | Business interpretation and planned acceptance rule |
| --- | --- | --- |
| date | null or string; default null | Source observation date, exactly YYYY-MM-DD and a real calendar date. Required for an accepted observation; reject future dates relative to the release business date. The deployed cocoa producer preserves the parsed ICCO observation date; see the [date correction](../docs/operations/cocoa-date-correction.md). |
| cocoa_price | null or double; default null | Finite positive EUR/tonne value. Null is a recorded missing value, not a valid price for publication. |
| oil_price | null or double; default null | Finite USD/barrel value. Do not impose cocoa's positive-price rule on a different economic series without a justified source-specific policy. |
| precipitation | null or double; default null | Finite non-negative daily precipitation; zero is valid. Source missing sentinels become null with their original representation retained in raw evidence. |
| soil_moisture | null or double; default null | Finite dimensionless wetness in the source's documented domain; validate its unit metadata rather than assume a percent or m3/m3 conversion. |
| ingestion_time | null or string; default null | Producer acquisition instant, normalized to timezone-aware UTC. Null/naive/unparseable values do not prove freshness. Existing weather production and oil/weather receipt-time mapping need item 5; preserve consumer receipt separately. |
| raw_payload | null or string; default null | Original source evidence or a stable reference, with secrets excluded. Missing legacy payloads do not create provenance that was never captured. Durable raw acquisition is item 10. |

Wire nullability permits representing missing data. It does not certify the
record as a current, valid business observation. Keep wire decoding, source
missingness, source-age checks and publication eligibility distinct.

The daily operational start boundary is a recovery/acquisition parameter, not
a permanent validation end date. Historical bootstrap coverage is recorded in
the [historical manifest](../data/historical_manifest.yaml). Do not copy the
backfill scripts' fixed 2025 end date into the daily contract or fabricate
observations between published dates.

## Missing and incompatible record outcomes

| Input case | Required outcome |
| --- | --- |
| Valid dated finite observation with known units and provenance | Eligible for staging and the later release gates; not automatically a complete release. |
| EIA explicit missing marker/null, a blank published weekday, or NASA missing sentinel | Represent missingness with source evidence; count it as absent/missing rather than a new valid observation or malformed-message loss. |
| Weather record missing one measurement | Preserve the available measurement and missingness; assess each parameter's latest valid date. Do not silently discard a date merely because one parameter's key is absent. |
| Null measurement followed by a valid historical value | Use only the service contract's visible, age-bounded carry-forward rule; never silently claim a fresh measurement. |
| Invalid/missing date, non-finite number, wrong type, unknown unit or malformed payload | Quarantine with source/message identity and reason; no silent coercion to a valid observation. |
| Same key and content delivered twice | One logical curated observation; preserve delivery/acquisition evidence. |
| Same key with a source correction | Retain revision evidence; apply deterministic source/revision precedence before publishing. |
| Successfully fetched source with no new published observation | Accept only under documented source availability and age rules; retain the original observation date. |
| Failed request or an unexplained empty/incompatible response | Fail acquisition; a successful publish trigger or old queued message cannot substitute for it. |

The [Avro fixtures](../tests/unit/test_schemas.py) cover valid/nullable records,
incompatible measurement types and truncated binary input for every feed.
EIA fixtures additionally cover product/unit/date errors, explicit missing
values, pagination, non-finite prices and published-source disagreement; the
isolated cloud test exercises the real sink. Full feed-wide business-quality
publication gates remain checklist item 15. Cocoa date correction is recorded
in [item 4 evidence](../docs/operations/cocoa-date-correction.md); cross-feed
timestamp corrections remain item 5.

## Timestamp and availability provenance

Keep observation date, source publication/revision time when supplied,
producer acquisition instant and consumer receipt instant distinct. Record
unknown publisher timestamps as unknown; never manufacture them from the
current clock. Retain original timestamps in raw evidence and define timezone
normalization before adding fields to deployed schemas. A source publication
calendar is not proof that an observation was available at a past cutoff.

## Schema changes and compatibility

1. Identify every producer, topic/schema revision, consumer, staging table,
   transform, package and report affected by the proposed change.
2. Record the old/new contract version and writer schema identity. Preserve
   old input fixtures and immutable raw provenance.
3. Test old-writer/new-reader and any required new-writer/old-reader paths with
   actual binary payloads and explicit writer/reader schemas. A nullable field
   with a default alone is not proof that the current schemaless decoder can
   safely read a new writer layout.
4. Prepare compatible consumers/table changes first, or use an explicit
   versioned route/decoder. Use an isolated integration destination and verify
   replay before switching producers.
5. Verify self-contained function packages, rollout order and rollback. Retain
   the old decoder/raw inputs until the required replay window closes.
6. Treat changes to units, point/series identity, grain, timestamp meaning or
   business definitions as contract changes even if the Avro field types stay
   identical. Publish a reviewed migration and consumer notice.

## Source use and accepted oil acquisition policy

The repository's code license is not a license for third-party datasets.
Record attribution, publisher terms, retrieval date and permission evidence
for each acquisition route before claiming unrestricted redistribution.

- ICCO: retain publisher/column/date provenance. This review did not verify a
  blanket license for public redistribution of acquired history; public web
  access alone is not that permission. Confirm the intended external-report
  use before broad publication under the BI expansion.
- EIA: the [original daily Europe Brent series](https://www.eia.gov/dnav/pet/hist/RBRTED.htm)
  supplies the required product, frequency and units. [EIA reuse guidance](https://www.eia.gov/about/copyrights_reuse.php)
  permits reuse of its public-domain information with acknowledgement and
  distinguishes protected third-party materials. Full comparison found
  historical discrepancies between the API, published table and the earlier
  supplier archive. The owner selected published-table history, preservation
  of the prior archive with its real provenance, and daily API publication
  only when the interval agrees with the table. The 28 prior-only dates are
  absent published observations in the new history. See the
  [migration evidence](../docs/operations/eia-migration.md); complete equivalence
  or completed historical warehouse replacement is not claimed.
- NASA POWER: preserve dataset/parameter/community/point attribution, units
  and response metadata. The [NASA-managed POWER dataset entry](https://registry.opendata.aws/nasa-power/),
  checked 2026-10-27, states unrestricted use/access/download and requests
  project citation; its listed license is CC BY 4.0. NASA's
  [Earthdata acknowledgement guidance](https://forum.earthdata.nasa.gov/viewtopic.php?sid=5add98efddee206f660227f75298c45f&t=504)
  also requests notification of redistribution and references to resulting
  publications. Preserve the applicable terms/citation with acquired evidence;
  do not substitute unrelated commercial-satellite licensing rules.

Existing archives retain their original provenance. The accepted replacement
has fresh acquisition metadata and a recovered/checksummed held cloud object;
warehouse repair requires isolated candidates and measured validation.
