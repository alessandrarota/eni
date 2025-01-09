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

logging.basicConfig(level=logging.INFO)
context = gx.get_context()
meter = metrics.get_meter(__name__)


def read_json_file(expectations_json_file_path=os.getenv("EXPECTATIONS_JSON_FILE_PATH")):
    if not expectations_json_file_path:
        raise ValueError("EXPECTATIONS_JSON_FILE_PATH variable not found. It must be provided.")
    
    with open(expectations_json_file_path, "r") as f:
        config_data = json.load(f)

        return config_data

def setup_gx(gx_json_data): 
    validation_defs = []
    data_product_name = gx_json_data["data_product_name"]
    data_product_suites = gx_json_data["data_product_suites"]

    for data_product_suite in data_product_suites:
        
        physical_informations = data_product_suite["physical_informations"]
        suite_name = data_product_name + "-" + physical_informations["data_source_name"] + "-" + physical_informations["data_asset_name"]

        data_source = add_data_source(context, physical_informations["data_source_name"])
        data_asset = add_data_asset(data_source, physical_informations["data_asset_name"])
        batch_definition = add_whole_batch_definition(data_asset, suite_name)

        suite = add_suite(context, data_product_name, suite_name, data_product_suite["expectations"])
        validation_def = add_validation_definition(context, batch_definition, suite)
        validation_defs.append(validation_def)

        # Create an ObservableGauge
        observable_gauge = meter.create_observable_gauge(
            name=suite_name,
            description=f"Validation results for suite: {suite_name}",
            unit="%",
            callbacks=[run_validation_callback(validation_def, data_product_name, suite_name, physical_informations["data_source_name"], physical_informations["data_asset_name"], pd.read_csv(physical_informations["dataframe"], delimiter=','))]
        )

    return validation_defs

def run_validation_callback(validation_def, data_product_name, suite_name, data_source_name, data_asset_name, df):
    def callback(options):
        validation_results = validation_run(df=df, validation_definition=validation_def)
        
        observations = []
        
        for validation_result in validation_results["results"]:
            result = validation_result["result"]
            expectation_config = validation_result["expectation_config"]
            meta = expectation_config["meta"]
            print(validation_result)

            #print(f"Validation result: {validation_result}")
            observation = Observation(
                value=100-result["unexpected_percent"],
                attributes={
                    "element_count": result["element_count"],
                    "unexpected_count": result["unexpected_count"],
                    "expectation_name": meta["expectation_name"],
                    "data_product_name": data_product_name,
                    "suite_name": suite_name,
                    "data_source_name": data_source_name,
                    "data_asset_name": data_asset_name#,
                    #"timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            )

            observations.append(observation)

        return observations

    return callback 

if __name__ == "__main__":
    logging.info("Starting the application...")

    logging.info("Reading GreatExpectations json file...")
    gx_json_data = read_json_file()

    logging.info("Setting up GreatExpectations...")
    validation_defs = setup_gx(gx_json_data)