from google.cloud import storage

client = storage.Client()
bucket = client.get_bucket('raw-historic-data')
print(f"Bucket '{bucket.name}' is accessible.")
