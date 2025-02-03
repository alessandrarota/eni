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

# Logging configuration
logging.basicConfig(level=logging.INFO)
logging.getLogger("great_expectations").setLevel(logging.WARNING)

# Get context and meter for metric creation
context = gx.get_context()
meter = metrics.get_meter(__name__)

# Function to load JSON file
def load_json_file(file_path):
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error reading or decoding the JSON file: {file_path} - {e}")
        sys.exit(1)

# Function to configure Great Expectations resources
def configure_gx_resources(gx_data, data_product_name, business_domain_name):
    gx_resources = []

    for suite_data in gx_data:
        physical_info = suite_data["physical_informations"]
        suite_name = f"{physical_info['data_source_name']}-{physical_info['data_asset_name']}"
        
        data_source = add_data_source(context, physical_info["data_source_name"])
        data_asset = add_data_asset(data_source, physical_info["data_asset_name"])
        batch_definition = add_whole_batch_definition(data_asset, suite_name)
        suite = add_suite(context, data_product_name, suite_name, suite_data["expectations"])

        gx_resources.append({
            "batch_definition": batch_definition,
            "suite_name": suite_name,
            "physical_informations": physical_info,
            "expectations": suite_data["expectations"],
            "suite": suite,
            "business_domain_name": business_domain_name,
            "data_product_name": data_product_name
        })

    return gx_resources

# Function to configure validation definitions
def configure_validation_definitions(resources):
    validation_definitions = []

    for resource in resources:
        batch_definition = resource["batch_definition"]
        suite = resource["suite"]
        validation_definition = add_validation_definition(context, batch_definition, suite)

        validation_definitions.append({
            "validation_definition": validation_definition,
            "metadata": resource
        })

    return validation_definitions

# Function to execute validations and create metrics
def execute_validations_and_create_metrics(validation_definitions):
    all_validation_results = []

    for validation_def in validation_definitions:
        metadata = validation_def["metadata"]
        physical_info = metadata["physical_informations"]

        validation_result = validation_run(
            df=pd.read_csv(physical_info["dataframe"], delimiter=','),
            validation_definition=validation_def["validation_definition"]
        )
        
        all_validation_results.append({
            "validation_result": validation_result,
            "metadata": validation_def["metadata"]
        })

    # Create metric name using SHA1 hash
    metric_name = f"{metadata['business_domain_name']}-{metadata['data_product_name']}"
    hashed_metric_name = re.sub(r'[^a-zA-Z0-9]', '', base64.urlsafe_b64encode(hashlib.sha1(metric_name.encode('utf-8')).digest()).decode('utf-8').rstrip('='))
    
    # Create observable gauge metric
    meter.create_observable_gauge(
        name=hashed_metric_name,
        unit="%",
        callbacks=[create_observations_callback(all_validation_results)]
    )

    logging.info("Validation metrics created!")

# Function to create the observation callback
def create_observations_callback(validation_results):
    def callback(options):
        observations = []

        for result_data in validation_results:
            validation_result = result_data["validation_result"]
            metadata = result_data["metadata"]
            physical_informations = metadata["physical_informations"]

            for result in validation_result["results"]:
                result_data = result["result"]
                expectation_config = result["expectation_config"]
                meta = expectation_config["meta"]
                kwargs = expectation_config["kwargs"]

                observation = Observation(
                    value=100 - result_data["unexpected_percent"],
                    attributes={
                        "signal_type": "DATA_QUALITY",
                        "business_domain_name": metadata["business_domain_name"],
                        "data_product_name": metadata["data_product_name"],
                        "expectation_name": meta["expectation_name"],
                        "data_source_name": physical_informations["data_source_name"],
                        "data_asset_name": physical_informations["data_asset_name"],
                        "column_name": kwargs["column"],
                        "checked_elements_nbr": result_data["element_count"],
                        "errors_nbr": result_data["unexpected_count"],
                        "gx_suite_name": metadata["suite_name"],
                        "data_quality_dimension_name":  meta["data_quality_dimension"]
                    }
                )

                observations.append(observation)

        logging.info("Observations created!")
        return observations

    return callback 

# Main function to start the process
def main(json_file_path, data_product_name, business_domain_name):
    try:
        logging.info("Starting the application...")

        logging.info("Reading the GreatExpectations JSON configuration file...")
        gx_data = load_json_file(json_file_path)

        logging.info("Configuring GreatExpectations resources...")
        gx_resources = configure_gx_resources(gx_data, data_product_name, business_domain_name)

        logging.info("Configuring validation definitions...")
        validation_defs = configure_validation_definitions(gx_resources)

        logging.info("Executing validations and creating metrics...")
        execute_validations_and_create_metrics(validation_defs)

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

    main(sys.argv[1], os.getenv("DATA_PRODUCT_NAME"), os.getenv("BUSINESS_DOMAIN_NAME", ""))