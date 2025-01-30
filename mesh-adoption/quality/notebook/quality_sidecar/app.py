import time
from opentelemetry.metrics import get_meter, Observation
import logging
import pandas as pd
import great_expectations as gx
from gx_setup.gx_dataframe import *
import logging
from opentelemetry import metrics
import os
import json
import sys
from opentelemetry.sdk.resources import SERVICE_NAME
from opentelemetry.metrics import (
    Observation
)

logging.basicConfig(level=logging.INFO)
logging.getLogger("great_expectations").setLevel(logging.WARNING)
context = gx.get_context()
meter = metrics.get_meter(__name__)


def read_json_file(json_file_path):
    try:
        with open(json_file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"File not found: {json_file_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON file: {json_file_path}")
        sys.exit(1)

def setup_gx_resources(gx_json_data, data_product_name):
    gx_resources = []

    for data_product_suite in gx_json_data:
        physical_informations = data_product_suite["physical_informations"]
        suite_name = physical_informations["data_source_name"] + "-" + physical_informations["data_asset_name"]

        data_source = add_data_source(context, physical_informations["data_source_name"])
        data_asset = add_data_asset(data_source, physical_informations["data_asset_name"])
        batch_definition = add_whole_batch_definition(data_asset, suite_name)
        suite = add_suite(context, data_product_name, suite_name, data_product_suite["expectations"])
        
        gx_resources.append({
            "batch_definition": batch_definition,
            "suite_name": suite_name,
            "physical_informations": physical_informations,
            "expectations": data_product_suite["expectations"],
            "suite": suite
        })

    return gx_resources

def setup_validation_definitions(data_resources):
    validation_defs = []

    for data_resource in data_resources:
        batch_definition = data_resource["batch_definition"]
        suite = data_resource["suite"]
        validation_def = add_validation_definition(context, batch_definition, suite)
        validation_defs.append(validation_def)

    return validation_defs

def run_validations_and_create_metric(validation_defs, gx_json_data, data_product_name):
    for index, data_product_suite in enumerate(gx_json_data):
        physical_informations = data_product_suite["physical_informations"]
        suite_name = physical_informations["data_source_name"] + "-" + physical_informations["data_asset_name"]

        validation_results = validation_run(df=pd.read_csv(physical_informations["dataframe"], delimiter=','), validation_definition=validation_defs[index])

        meter.create_observable_gauge(
            name=suite_name,
            unit="%",
            callbacks=[create_observations_callback(validation_results, data_product_name, suite_name, physical_informations["data_source_name"], physical_informations["data_asset_name"])]
        )

def create_observations_callback(validation_results, data_product_name, suite_name, data_source_name, data_asset_name):
    def callback(options):
        observations = []
        
        for validation_result in validation_results["results"]:
            result = validation_result["result"]
            expectation_config = validation_result["expectation_config"]
            meta = expectation_config["meta"]
            expectation_config = validation_result["expectation_config"]
            kwargs = expectation_config["kwargs"]

            observation = Observation(
                value=100-result["unexpected_percent"],
                attributes={
                    "signal_type": "DATA_QUALITY",
                    "element_count": result["element_count"],
                    "unexpected_count": result["unexpected_count"],
                    "expectation_name": meta["expectation_name"],
                    "data_product_name": data_product_name,
                    "suite_name": suite_name,
                    "data_source_name": data_source_name,
                    "data_asset_name": data_asset_name,
                    "column_name": kwargs["column"]
                }
            )

            observations.append(observation)

        logging.info("ValidationResults created!")
        return observations

    return callback 

def main(json_file_path, data_product_name):
    try:
        logging.info("Starting the application...")

        logging.info("Reading GreatExpectations json file...")
        gx_json_data = read_json_file(json_file_path)

        logging.info("Setting up GreatExpectations Resources...")
        gx_resources = setup_gx_resources(gx_json_data, data_product_name)

        logging.info("Setting up ValidationDefinitions...")
        validation_defs = setup_validation_definitions(gx_resources)

        logging.info("Running ValidationDefinitions and creating Metric...")
        results = run_validations_and_create_metric(validation_defs, gx_json_data, data_product_name)

        # try:
        #     while True:
        #         time.sleep(5)  
        # except:
        #     logging.info("Shutting down the application...")

    except Exception as e:
        logging.error(f"An error occurred during application execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("No file path provided as parameter!")
        sys.exit(1)

    if not os.getenv("DATA_PRODUCT_NAME"):
        logging.error("The environment variable DATA_PRODUCT_NAME is not set or is empty!")
        sys.exit(1)

    main(sys.argv[1], os.getenv("DATA_PRODUCT_NAME"))