sudo tee /opt/airflow/bin/restart_with_secrets.sh >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
export AIRFLOW__CORE__FERNET_KEY=$(gcloud secrets versions access latest --secret=airflow-fernet-key)
cd /opt/airflow
exec sudo -E docker compose up -d
EOF
sudo chmod +x /opt/airflow/bin/restart_with_secrets.sh