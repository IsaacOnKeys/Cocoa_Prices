#!/usr/bin/env bash

zones=(europe-west3-b europe-west3-c europe-west1-b europe-west1-c europe-west4-a europe-central2-a europe-north1-a)

############################################################
# 1. Cocoa prices
############################################################
(
while true; do
  for zone in "${zones[@]}"; do
    echo "COCOA: trying $zone..."
    gcloud dataflow flex-template run cocoa-prices-stream-$zone-$(date +%s) \
      --project=cocoa-prices-430315 \
      --region=europe-west3 \
      --worker-zone=$zone \
      --num-workers=1 \
      --max-workers=3 \
      --worker-machine-type=e2-standard-2 \
      --enable-streaming-engine \
      --template-file-gcs-location=gs://dataflow-templates-europe-west3/latest/flex/PubSub_Avro_to_BigQuery \
      --service-account-email=494039866722-compute@developer.gserviceaccount.com \
      --parameters schemaPath=gs://cocoa-prices-temp-for-bq/schemas/cocoa_schema.avsc,inputSubscription=projects/cocoa-prices-430315/subscriptions/cocoa-prices-sub,outputTableSpec=cocoa-prices-430315:stream_staging.cocoa_prices,outputTopic=projects/cocoa-prices-430315/topics/cocoa-prices-dead-letter && break
  done
  sleep 900
done
) &

############################################################
# 2. Oil prices
############################################################
(
while true; do
  for zone in "${zones[@]}"; do
    echo "OIL: trying $zone..."
    gcloud dataflow flex-template run oil-prices-stream-$zone-$(date +%s) \
      --project=cocoa-prices-430315 \
      --region=europe-west3 \
      --worker-zone=$zone \
      --num-workers=1 \
      --max-workers=3 \
      --worker-machine-type=e2-standard-2 \
      --enable-streaming-engine \
      --template-file-gcs-location=gs://dataflow-templates-europe-west3/latest/flex/PubSub_Avro_to_BigQuery \
      --service-account-email=494039866722-compute@developer.gserviceaccount.com \
      --parameters schemaPath=gs://cocoa-prices-temp-for-bq/schemas/oil_schema.avsc,inputSubscription=projects/cocoa-prices-430315/subscriptions/oil-prices-sub,outputTableSpec=cocoa-prices-430315:stream_staging.oil_prices,outputTopic=projects/cocoa-prices-430315/topics/oil-prices-dead-letter && break
  done
  sleep 900
done
) &

############################################################
# 3. Weather data
############################################################
(
while true; do
  for zone in "${zones[@]}"; do
    echo "WEATHER: trying $zone..."
    gcloud dataflow flex-template run weather-data-stream-$zone-$(date +%s) \
      --project=cocoa-prices-430315 \
      --region=europe-west3 \
      --worker-zone=$zone \
      --num-workers=1 \
      --max-workers=3 \
      --worker-machine-type=e2-standard-2 \
      --enable-streaming-engine \
      --template-file-gcs-location=gs://dataflow-templates-europe-west3/latest/flex/PubSub_Avro_to_BigQuery \
      --service-account-email=494039866722-compute@developer.gserviceaccount.com \
      --parameters schemaPath=gs://cocoa-prices-temp-for-bq/schemas/weather_schema.avsc,inputSubscription=projects/cocoa-prices-430315/subscriptions/weather-sub,outputTableSpec=cocoa-prices-430315:stream_staging.precipitation_moisture,outputTopic=projects/cocoa-prices-430315/topics/weather-dead-letter && break
  done
  sleep 900
done
) &

wait
