# Existing VM Ops Agent

`airflow-vm` has an enabled, active `google-cloud-ops-agent` service. Its
`/etc/google-cloud-ops-agent/config.yaml` override parsed as an empty mapping
on 2026-10-30; [config.yaml](config.yaml) records that semantic configuration.
The installed file's SHA256 was
`d0dc20b887a86367da927268b62f5ad69588c295315e6420720fe258c3df5dac`.
Comments/formatting are not reproduced as runtime settings.

The installed agent supplies its default host metrics and syslog configuration.
This feature adds no agent receivers, collection frequency, custom metrics or
service restart. The operational dashboard reads the existing CPU/memory data.
For restoration, install this override at the path above with root ownership
and mode 0644, then restart the already installed agent during maintenance.
Agent installation/version reconstruction remains the VM deployment/recovery
work, rather than a claim made by this configuration capture.

Application logs go directly to Cloud Logging; they do not require adding an
Airflow file receiver or exposing connection/secret configuration. The native
status/config receipt is `.build/operational-telemetry/ops-agent.json`.
