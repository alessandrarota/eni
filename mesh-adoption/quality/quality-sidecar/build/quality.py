import os
import sys
import logging
import time
import shlex
import json
from gx.data_quality_gx import validate_data_quality
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
        validation_results = validate_data_quality(json_file_path, data_product_name)
        logging.info(f"GX Validation Results: {validation_results}")

        # Running OTLP functions with subprocess
        logging.info("Running OTLP functions...")
        command = f"opentelemetry-instrument --metrics_exporter otlp,console --logs_exporter otlp,console python -m data_quality_otlp {shlex.quote(json.dumps(validation_results))} \"{data_product_name}\""
        result = subprocess.run(command, shell=True, check=True)

        logging.info(result)

    except Exception as e:
        logging.error(f"An error occurred during application execution: {e}")
        sys.exit(1)



if __name__ == "__main__":
    expectations_json_file_path = os.getenv("EXPECTATIONS_JSON_FILE_PATH")
    data_product_name = os.getenv("DATA_PRODUCT_NAME")

    if not expectations_json_file_path:
        logging.error("The environment variable EXPECTATIONS_JSON_FILE_PATH is not set or is empty!")
        sys.exit(1)

    if not data_product_name:
        logging.error("The environment variable DATA_PRODUCT_NAME is not set or is empty!")
        sys.exit(1)

    main(expectations_json_file_path, data_product_name)