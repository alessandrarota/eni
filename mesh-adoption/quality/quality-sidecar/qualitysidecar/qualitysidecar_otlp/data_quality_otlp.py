import sys
import logging
from opentelemetry import metrics
from opentelemetry.metrics import Observation
import json
import os

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Setup meter for metric creation
meter = metrics.get_meter(__name__)

def create_otlp_metric(validations_results, data_product_name):
    meter.create_observable_gauge(
        name="".join(data_product_name.split()),
        unit="%",
        callbacks=[create_observations_callback(validations_results)]
    )

    logging.info("OTLP metric created!")

# Function to create the observation callback
def create_observations_callback(validation_results):
    def callback(options):
        observations = []

        for validation_result in validation_results:
            result = validation_result["result"]
            meta = validation_result["expectation_config"]["meta"]

            observation = Observation(
                value=100 - result["unexpected_percent"],
                attributes={
                    "signal_type": "DATA_QUALITY",
                    "checked_elements_nbr": result["element_count"],
                    "errors_nbr": result["unexpected_count"],
                    "check_name":  meta["check_name"],
                    "data_product_name": meta["data_product_name"]
                }
            )
            observations.append(observation)

        return observations

    return callback 

def main(validation_results):
    try:
        logging.info("Creating OTLP Metric...")
        create_otlp_metric(validation_results, os.getenv("DATA_PRODUCT_NAME"))
        logging.info("Observations created!")

    except Exception as e:
        logging.error(f"An error occurred during application execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    validation_results = sys.argv[1]  
    main(json.loads(validation_results))