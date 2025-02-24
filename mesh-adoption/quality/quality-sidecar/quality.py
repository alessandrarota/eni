import os
import sys
import logging
import time
import shlex
import json
from qualitysidecar.qualitysidecar.qualitysidecar_gx import validate_data_quality
from qualitysidecar.qualitysidecar.qualitysidecar_otlp import send_metric
import subprocess

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Main function to start the process
def main(json_file_path, data_product_name):
    try:
        logging.info("Running GX functions...")
        # Running GX module
        validation_results = validate_data_quality(json_file_path)
        logging.info(f"GX Validation Results: {validation_results}")

        # Running OTLP functions with subprocess
        logging.info("Running OTLP functions...")
        result = send_metric(validation_results)

    except Exception as e:
        logging.error(f"An error occurred during application execution: {e}")
        sys.exit(1)



if __name__ == "__main__":
    expectations_json_file_path = os.getenv("EXPECTATIONS_JSON_FILE_PATH")
    data_product_name = os.getenv("DATA_PRODUCT_NAME")

    main(expectations_json_file_path, data_product_name)