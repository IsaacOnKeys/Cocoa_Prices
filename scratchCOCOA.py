
RUNNER = os.getenv("RUNNER", "DirectRunner")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 4))
MAX_NUM_WORKERS = int(os.getenv("MAX_NUM_WORKERS", NUM_WORKERS))
PROJECT_ID = os.getenv("PROJECT_ID", "cocoa-prices-430315")
REGION = os.getenv("REGION", "europe-west3")
STAGING_LOCATION = os.getenv("STAGING_LOCATION", "gs://raw-historic-data/staging")
TEMP_LOCATION = os.getenv("TEMP_LOCATION", "gs://raw-historic-data/temp")
REGION = os.getenv("REGION", "europe-west3")
WORKER_MACHINE_TYPE = os.getenv("WORKER_MACHINE_TYPE", "e2-standard-4")

logging.info(f"Runner: {RUNNER}")

if RUNNER not in ["DirectRunner", "DataflowRunner"]:
    raise ValueError(f"Unsupported runner: {RUNNER}")

logging.info(f"Number of workers: {NUM_WORKERS}")

# Pipeline Options
PIPELINE_OPTIONS = PipelineOptions()

# Google Cloud Options
GCP_OPTIONS = PIPELINE_OPTIONS.view_as(GoogleCloudOptions)
GCP_OPTIONS.job_name = f"cleaning-cocoa-prices-data-{int(time.time()) % 100000}"

# Standard Options
STANDARD_OPTIONS = PIPELINE_OPTIONS.view_as(StandardOptions)
STANDARD_OPTIONS.runner = RUNNER

# Runner-specific configuration
if RUNNER == "DirectRunner":
    DIRECT_OPTIONS = PIPELINE_OPTIONS.view_as(
        beam.options.pipeline_options.DirectOptions
    )
    DIRECT_OPTIONS.direct_num_workers = NUM_WORKERS

elif RUNNER == "DataflowRunner":
    # Environment Validation for DataflowRunner
    required_vars = {
        "PROJECT_ID": PROJECT_ID,
        "STAGING_LOCATION": STAGING_LOCATION,
        "TEMP_LOCATION": TEMP_LOCATION,
        "REGION": REGION,
        "WORKER_MACHINE_TYPE": WORKER_MACHINE_TYPE,
        "NUM_WORKERS": NUM_WORKERS,
        "MAX_NUM_WORKERS": MAX_NUM_WORKERS,
    }
    logging.info(f"Project: {PROJECT_ID[:5]}***")
    logging.info(f"Staging location: {STAGING_LOCATION.split('/')[2]}***")
    logging.info(f"Temp location: {TEMP_LOCATION.split('/')[2]}***")
    logging.info(f"Region: {REGION}")
    logging.info(f"Worker machine type: {WORKER_MACHINE_TYPE}")

    for name, value in required_vars.items():
        if not value:
            logging.error(f"Missing required environment variable: {name}")
            raise ValueError(
                f"Environment variable {name} must be set for DataflowRunner."
            )
    # Setup Options
    SETUP_OPTIONS = PIPELINE_OPTIONS.view_as(SetupOptions)
    SETUP_OPTIONS.requirements_file = os.getenv("REQUIREMENTS_FILE", "requirements.txt")
    SETUP_OPTIONS.setup_file = os.getenv("SETUP_FILE", "./setup.py")
    GCP_OPTIONS.worker_machine_type = WORKER_MACHINE_TYPE
    GCP_OPTIONS.num_workers = NUM_WORKERS
    GCP_OPTIONS.max_num_workers = MAX_NUM_WORKERS
    GCP_OPTIONS.project = PROJECT_ID
    GCP_OPTIONS.staging_location = STAGING_LOCATION
    GCP_OPTIONS.temp_location = TEMP_LOCATION
    GCP_OPTIONS.region = REGION
