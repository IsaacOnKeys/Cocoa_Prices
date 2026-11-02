# Cost baseline and operating limits

Owner: project operator. Target below EUR 5/month; EUR 10 is the owner's maximum acceptable monthly spend, including development. These are operating constraints, not a guaranteed provider-enforced euro cap. Stop discretionary cloud builds, training and backfills at EUR 5 forecast/actual spend; investigate at EUR 8 and resolve before EUR 10. Do not disable billing or destroy storage/keys as an automatic reaction.

## October 2026 evidence

The owner's October 31 Billing Reports screenshot shows EUR 19.27 total: EUR 14.64 Container Registry Vulnerability Scanning, EUR 4.47 Compute Engine and EUR 0.15 KMS. Other displayed services net to zero after savings. The report total excluding scanning is EUR 4.63; rounded service rows need not sum exactly. This is owner-supplied billing evidence, not a billing-export reconciliation; the screenshot's project filter is collapsed. October exceeded the EUR 10 limit. Development costs count and cannot be dismissed as recurring operation.

The measured 31-day usage includes development: 83.89 VM hours, 77.13 GiB query bytes billed across actors, and about 193.5 MiB logs ingested. Normal earlier Airflow days billed 200–220 MiB of queries, with the slowest individual query about 14.3 seconds; recent development days reached 3.17 GiB for that identity. Query elapsed time is not end-to-end DAG duration. Recent daily source acquisitions include manual validation: October 30 had four acquisitions/feed and 89 cocoa, 82 oil and 125 weather physical rows. These figures describe the observed workload, not supplier limits or a load-test result.

The stopped e2-medium VM retains a 30 GB standard persistent disk. Function images occupy about 0.482 GiB in the regional gcf-artifacts repository. Raw and temporary application buckets were about 7.32 MiB and 2.05 MiB in the latest available daily metric; daily storage metrics lag, and runtime backup images add about 597 MiB per retained backup set. Do not sum daily storage gauge samples as current bytes or interpret table metadata as an invoice. Fixed disks, retained artifacts, backups, keys and networking can still accrue costs while a VM is stopped.

October's repeated deployment builds are consistent with the new scanning charge: automatic scanning was active on the function-image repository. It was explicitly disabled on that repository on October 31 and native metadata confirmed SCANNING_DISABLED. Existing/in-progress scan charges may arrive later; the setting does not reverse past charges. The project-wide scanning API remains enabled: broader disablement was rejected by automatic approval review. No workaround was attempted. Local/CI Gitleaks and Python dependency audits remain; these do not replace an OS/container vulnerability scan.

## Controls

- Keep the existing weekday 18:30 Berlin VM start and daily 20:30 stop; encrypted backups run at 20:10 on weekdays. November has 21 weekdays, so the planned window is 42 hours before maintenance. Never leave a maintenance VM running after work; record and restore its original power state. Do not move shutdown ahead of the existing backup without coordinating both.
- Keep zero minimum instances. Daily producers are capped at one instance each, ingestion at three and watchdog at one, with bounded requests/timeouts. No continuous Dataflow service or automatic retraining is added. Optional Beam backfills and model training require a separate bounded, manually reviewed run.
- Keep one active DAG run, its 60-minute run timeout, bounded task retries, deterministic BigQuery job IDs and existing per-job byte limits. No extra pipeline rerun is needed to validate a label/quota change.
- BigQuery query quotas: 64 GiB/project/day, 5 GiB/user/day across users/service accounts. Native API units are MiBy, so preferences are 65536 and 5120. A small five-day replay at the earlier 220 MiB/day rate is about 1.1 GiB, comfortably inside the per-user limit. This is an arithmetic capacity allowance, not a native backlog throughput benchmark. The API's recent aggregate usage was 45510 MiB during development, so the project allowance exceeds that peak. The explicit large-decrease acknowledgement replaces the enormous defaults; Google's below-recent-usage safety check remains enabled. Higher-cost model work is not priced like ordinary SELECT queries; review it separately.
- Resource labels attribute owned datasets, application buckets, producers and VM; existing ingestion/watchdog labels remain. Job metadata separates the Airflow service account from development even for older unlabeled queries.
- Existing project-only EUR 10 budget excludes credits and sends actual EUR 5/8/10 plus forecast EUR 10 notices only to the selected operator. Operational test delivery was confirmed earlier; the owner separately confirmed receipt of an actual October budget email on 2026-10-31. The specific threshold was not identified. Budgets notify and do not stop spending. Query quotas are approximate and do not cap compute/storage/build costs or guarantee the aggregate EUR 10 limit.

Before every cloud image build/deployment, inspect the destination repository:

```powershell
gcloud artifacts repositories describe gcf-artifacts --project=cocoa-prices-430315 --location=europe-west3 --format="value(vulnerabilityScanningConfig.enablementState)"
```

Proceed only with SCANNING_DISABLED for this CV project's repository. If it is active, stop and restore the approved repository exclusion before building. New repositories need an explicit cost review; do not enable paid scanners by default. The Google-managed repository is not imported into Terraform solely to own this one setting. Metadata-only labels and the query quotas add no always-on compute.

## Repeatable measurement and remaining acceptance

Run the read-only profiler into a new ignored receipt directory:

```powershell
.build/python311-ops/Scripts/python.exe scripts/operations/finops_snapshot.py --days 7 --output .build/finops-weekly.json
```

Add --query-usage only when needed: its one metadata/staging-identity query is capped at 128 MiB, with no source prices or model training. The October measurement processed 5.49 MiB and billed 50 MiB. Table sizes use metadata APIs. Partial unavailable metrics are explicit; failure is not a zero-cost result. Outputs contain private infrastructure/identity metadata and stay outside Git. Regional repository coverage is europe-west3 only. Review Billing Reports weekly during development and after a deployment; after stabilization, review monthly and retain a normal month's service/SKU totals separately from development.

Below EUR 5/month remains plausible given the former cost and the removed scanning source, but is not proven by the mixed development month. No price forecast is substituted for an actual invoice. Budget notification receipt is confirmed. Item 34's implementation and report workload measurement are complete; sustained recurring spend still requires an ordinary operating month under item 36.

November 1 BI development measured a mixed-cache two-page visit at 250 MiB billed and a date-filter change at 40 MiB, before correcting the historical chart sources. A cached first-page revisit issued no new jobs in its observed interval. These are query-byte measurements, not euro charges or final-report recurring costs. The final historical comparison was blocked by the configured QueryUsagePerUserPerDay quota; no quota was raised and no alternate identity was used on that date. Final-source measurements were still open at that point. Editing and validation count as development usage; a quota rejection must not be reported as a successful test.

On November 2 the owner explicitly authorized a temporary 5-to-6 GiB per-user allowance for verification, followed by restoration. The final report measured 390 MiB billed for a mixed-cache two-page visit (history 160 MiB; KPIs 230 MiB), six query-cache hits, a 70 MiB incremental seven-day filter query and a 140 MiB empty-range query. A cached revisit produced no new BigQuery jobs. These observed query shapes include billing minimums; they are not a fully cold worst-case forecast or a monetary bill. Public viewers share the owner's delegated query allowance, so repeated uncached visits can intentionally hit the daily limit. Keep that tradeoff explicit for the CV report; do not raise quotas automatically for traffic.

The native effective user limit was restored to 5120 MiB; the project cap remained 65536 MiB. A fresh three-row source-freshness query succeeded after restoration and billed 20 MiB. The final historical reconciliation billed 70 MiB and the scheduled-evidence lookup 20 MiB; these are development checks, separately labelled from report jobs. No builds, model training or VM startup were used. The read-only VM check reported TERMINATED. Terraform source limits are unchanged, and the temporary preference adjustment is not a new recurring allowance. Detailed receipts and job IDs are linked in [the report evidence](bi-report.md).

Validation on October 31: label changes and two quota preferences applied; native Service Usage reported effective 65536/5120 MiB limits, repository metadata reported SCANNING_DISABLED and the labeled VM remained TERMINATED. Final Terraform plan had no changes. Changed-file Ruff, Python 3.11 compilation, Terraform formatting/validation and whitespace checks passed. No supplier invocation, cloud image build, pipeline replay or training was used for these checks. The first quota attempt failed without changing limits because the consumer project was missing; the scoped provider now explicitly supplies it. Google's rejected 10 GiB aggregate proposal was replaced with the documented 64 GiB allowance after inspecting recent usage.

References: [repository scanning controls](https://docs.cloud.google.com/artifact-analysis/docs/enable-automatic-scanning), [BigQuery custom query quotas and their limits](https://docs.cloud.google.com/bigquery/docs/custom-quotas). Private receipts are in .build/item34; they are not public release artifacts.
