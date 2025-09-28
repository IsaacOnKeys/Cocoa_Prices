sudo tee /opt/airflow/bin/setup_fernet_secret.sh >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

cd /opt/airflow

# 0) Ensure the Secret exists
if ! gcloud secrets describe airflow-fernet-key >/dev/null 2>&1; then
  echo "ERROR: Secret 'airflow-fernet-key' not found. Create it in Secret Manager first." >&2
  exit 1
fi

# 1) Backups
ts=$(date +%Y%m%d-%H%M%S)
[ -f docker-compose.yaml ] && sudo cp docker-compose.yaml docker-compose.yaml.bak.$ts
[ -f .env ] && sudo cp .env .env.bak.$ts

# 2) Make Compose read the env var (not a literal)
if grep -qE '^\s*AIRFLOW__CORE__FERNET_KEY:' docker-compose.yaml; then
  sudo sed -i 's#^\(\s*AIRFLOW__CORE__FERNET_KEY:\).*#\1 ${AIRFLOW__CORE__FERNET_KEY:-set_via_restart}#' docker-compose.yaml
else
  echo "WARNING: did not find 'AIRFLOW__CORE__FERNET_KEY:' in docker-compose.yaml. Add it under each Airflow service's 'environment:'." >&2
fi

# 3) Remove any plaintext key from .env
sudo sed -i '/^AIRFLOW__CORE__FERNET_KEY=/d' .env || true

# 4) Create a restart helper that pulls the key from Secret Manager
sudo mkdir -p /opt/airflow/bin
sudo tee /opt/airflow/bin/restart_with_secrets.sh >/dev/null <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
export AIRFLOW__CORE__FERNET_KEY=$(gcloud secrets versions access latest --secret=airflow-fernet-key)
cd /opt/airflow
exec sudo -E docker compose up -d
EOS
sudo chmod +x /opt/airflow/bin/restart_with_secrets.sh

# 5) Restart Airflow with the secret-loaded env
sudo /opt/airflow/bin/restart_with_secrets.sh

# 6) One-time re-encrypt existing creds (idempotent)
sudo -E docker compose exec airflow-webserver airflow rotate-fernet-key || true

# 7) Verify
sudo -E docker compose exec airflow-webserver bash -lc 'test -n "$AIRFLOW__CORE__FERNET_KEY" && echo "OK: key visible in container" || (echo "MISSING key in container"; exit 1)'
sudo -E docker compose exec airflow-webserver airflow info | grep -i fernet || true
echo "If you still see any 'empty cryptography key' warnings in the webserver logs, re-run step 5."
EOF

sudo chmod +x /opt/airflow/bin/setup_fernet_secret.sh
sudo /opt/airflow/bin/setup_fernet_secret.sh
