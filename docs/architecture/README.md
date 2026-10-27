# Architecture and design decisions

The [root README](../../README.md) contains the current rendered Mermaid flow.
The [wide](cocoa-architecture.xml) and [tall](cocoa-architecture-tall.xml)
editable diagram sources are preserved. The [Terraform graph](terraform-resource-graph.png)
is the exact previously selected export.

## Engineering boundaries

The analytical sequence is visible in the SQL folder structure:

[Curated data](../../sql/01_curated_data/README.md) ->
[feature engineering](../../sql/02_feature_engineering/README.md) ->
[model training](../../sql/03_model_training/README.md) ->
[prediction](../../sql/04_prediction/README.md) ->
[backtesting / evaluation](../../sql/05_evaluation/README.md).

Each stage documents its input/output contract and links directly to the
implementing statements. One canonical procedure retains the execution order.

- Separate historical bootstrap from daily operations. Protected snapshots and
  a provenance manifest make historical processing recoverable without claiming
  that the original acquisition was automated.
- Keep producer deployments self-contained while maintaining one editable Avro
  schema per feed. Generated schemas are copies, not independently edited contracts.
- Use Pub/Sub between producers and consumers. Consumers write staging; SQL
  owns deduplication/MERGE into curated history.
- Orchestrate source triggers, staging progress and SQL in Airflow. Systemd
  starts Beam with the VM; Cloud Scheduler owns the VM operating window.
- Keep source layout independent of installed filenames. /opt/beam and
  /opt/airflow paths remain stable while repository domains become visible.
- Represent existing infrastructure in Terraform state without taking ownership
  of function source deployments, secret payloads or SQL-created objects.
- Distinguish training/forecasting from evaluation. Current historical
  predictions and MAPE are not a chronological out-of-sample backtest.

The older editable diagram includes conceptual dead-letter arrows and a more
detailed task layout. Topic existence is not evidence of active DLQ routing;
the current DAG source and captured runtime mappings are authoritative.

See [GCP ownership](../infrastructure/gcp-inventory.md),
[feature definitions](../feature-engineering/README.md),
[runtime mappings](../operations/runtime-layout.md), and the
[engineering backlog](../operations/engineering-backlog.md).
