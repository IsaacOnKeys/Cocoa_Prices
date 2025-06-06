#!/bin/bash

# Submit clean_precipitation.py
python clean_precipitation.py \
    --runner=DataflowRunner \
    --job_name="clean-weather-data-$(date +%s)" \
    --project=cocoa-prices-430315 \
    --region=europe-west3 \
    --temp_location=gs://raw-historic-data/temp \
    --staging_location=gs://raw-historic-data/staging \
    --worker_machine_type=e2-standard-4 \
    --save_main_session \
    --worker_harness_container_image=europe-west3-docker.pkg.dev/cocoa-prices-430315/cocoa-code-project/dataflow-pipelines-batch:latest

# Submit clean_oil_fred.py
python clean_oil_fred.py \
    --runner=DataflowRunner \
    --job_name="clean-oil-data-$(date +%s)" \
    --project=cocoa-prices-430315 \
    --region=europe-west3 \
    --temp_location=gs://raw-historic-data/temp \
    --staging_location=gs://raw-historic-data/staging \
    --worker_machine_type=e2-standard-4 \
    --save_main_session \
    --worker_harness_container_image=europe-west3-docker.pkg.dev/cocoa-prices-430315/cocoa-code-project/dataflow-pipelines-batch:latest

# Submit clean_cocoa_prices.py
python clean_cocoa_prices.py \
    --runner=DataflowRunner \
    --job_name="clean-cocoa-data-$(date +%s)" \
    --project=cocoa-prices-430315 \
    --region=europe-west3 \
    --temp_location=gs://raw-historic-data/temp \
    --staging_location=gs://raw-historic-data/staging \
    --worker_machine_type=e2-standard-4 \
    --save_main_session \
    --worker_harness_container_image=europe-west3-docker.pkg.dev/cocoa-prices-430315/cocoa-code-project/dataflow-pipelines-batch:latest