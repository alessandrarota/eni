import os
import json
import logging
import subprocess
import shlex
import sys
import sh
from dotenv import load_dotenv
from .configurations.environment import DevelopmentConfig, TestingConfig, ProductionConfig, BaseConfig

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.getLogger('sh').setLevel(logging.CRITICAL)


def load_env(env_folder="configurations"):
    env = os.getenv("ENV")
    data_product_name = os.getenv("DATA_PRODUCT_NAME")

    
    if env == 'sd':
        config_class = DevelopmentConfig
    elif env == 'st':
        config_class = TestingConfig
    elif env == 'pr':
        config_class = ProductionConfig
    else:
        config_class = BaseConfig

    os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = config_class.OTEL_EXPORTER_OTLP_PROTOCOL
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = config_class.OTEL_EXPORTER_OTLP_ENDPOINT
    logging.info(f"OTEL_EXPORTER_OTLP_ENDPOINT set to {os.environ['OTEL_EXPORTER_OTLP_ENDPOINT']}")

    os.environ["OTEL_SERVICE_NAME"] = f"{data_product_name}-quality_sidecar"
    logging.info(f"OTEL_SERVICE_NAME set to {os.environ['OTEL_SERVICE_NAME']}")

def send_metric(validation_results):
    logging.info("Setting environment variables...")
    load_env()

    python_file = os.path.join(os.path.dirname(__file__), "data_quality_otlp.py")
    command = (
        "opentelemetry-instrument --metrics_exporter otlp,console "
        "--logs_exporter otlp,console python "
        f"{shlex.quote(python_file)} "
        f"{shlex.quote(json.dumps(validation_results))}"
    )

    logging.info("Executing OpenTelemetry command...")
    try:
        result = sh.bash("-c", command)
        logging.info(f"Command executed successfully!")
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing command: {e}")
        raise

    
