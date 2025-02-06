import os
import sys
import json
import logging
import pandas as pd
import hashlib
import time
import base64, re
from opentelemetry import metrics
from opentelemetry.metrics import Observation
import great_expectations as gx
from gx_setup.gx_dataframe import *
from connectors.SystemConnector import *

# Logging configuration
logging.basicConfig(level=logging.INFO)
logging.getLogger("great_expectations").setLevel(logging.WARNING)

# Get context and meter for metric creation
context = gx.get_context()
meter = metrics.get_meter(__name__)

def load_json_file(file_path):
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error reading or decoding the JSON file: {file_path} - {e}")
        sys.exit(1)

def configure_expectations_and_run_validations(json_file, data_product_name):
    validation_results = []

    for system in json_file:
        system_name = system["system_name"]
        system_type = system["system_type"]

        data_source = get_existing_data_source(context, system_name)
        if not data_source:
            data_source = add_data_source(context, system_name)

        for expectation in system["expectations"]:
            asset_name = expectation["asset_name"]
            check_name = expectation["check_name"]
            expectation_type = expectation["expectation_type"]

            data_asset = get_existing_data_asset(data_source, asset_name)
            if not data_asset:
                data_asset = add_data_asset(data_source, asset_name)

            batch_definition = data_asset.add_batch_definition_whole_dataframe(check_name)
            
            connector = get_connector(
                system_type=system_type,
                system_name=system_name,
                asset_name=asset_name,
                asset_kwargs=expectation["asset_kwargs"]
            )

            try:
                ExpectationClass = getattr(gx.expectations.core, expectation_type)

                if isinstance(ExpectationClass, type):
                    expectation_instance = ExpectationClass(**expectation["kwargs"], meta={"check_name": check_name, "data_product_name": data_product_name})
                    logging.info(f"Expectation instance created: {expectation_instance}")

                    batch = batch_definition.get_batch(batch_parameters={"dataframe": connector.get_dataframe()})
                    validation_result = batch.validate(expectation_instance)

                else:
                    logging.error(f"{expectation_type} is not a valid expectation class!")

            except Exception as e:
                logging.error(f"Error processing expectation {expectation_type}: {str(e)}")

            validation_results.append(validation_result) 

    return validation_results


def create_otlp_metric(validations_results, data_product_name):
    meter.create_observable_gauge(
        name=data_product_name,
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

        logging.info("Observations created!")
        return observations

    return callback 

# Main function to start the process
def main(json_file_path, data_product_name):
    try:
        logging.info("Starting the application...")

        logging.info("Reading the GreatExpectations JSON configuration file...")
        json_file = load_json_file(json_file_path)

        logging.info("Configuring Expectations and running Validations...")
        validation_results = configure_expectations_and_run_validations(json_file, data_product_name)

        logging.info("Creating OTLP Metric...")
        create_otlp_metric(validation_results, data_product_name)

        try:
            while True:
                time.sleep(5)  
        except:
            logging.info("Shutting down the application...")

    except Exception as e:
        logging.error(f"An error occurred during application execution: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("No file path provided as a parameter!")
        sys.exit(1)

    if not os.getenv("DATA_PRODUCT_NAME"):
        logging.error("The environment variable DATA_PRODUCT_NAME is not set or is empty!")
        sys.exit(1)

    main(sys.argv[1], os.getenv("DATA_PRODUCT_NAME"))