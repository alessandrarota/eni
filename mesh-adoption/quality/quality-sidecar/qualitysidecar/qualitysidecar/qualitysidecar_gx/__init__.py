import sys
import json
import os
import great_expectations as gx
#from .setup.dataframe import *
from .connectors.SystemConnector import *
from .setup.data import SparkDataSource, PandasDataSource
from .setup.expectation import get_expectation_class

# Logging configuration
logging.getLogger("great_expectations").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_json_file(file_path):
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error reading or decoding the JSON file: {file_path} - {e}")
        sys.exit(1)

def configure_expectations_and_run_validations(json_file, data_product_name):
    # setup gx context
    context = gx.get_context()
    #data_source_manager = SparkDataSource(context)
    data_source_manager = PandasDataSource(context)
    
    validation_results = []

    for system in json_file:
        system_name = system["system_name"]
        system_type = system["system_type"]

        data_source = data_source_manager.add_data_source(system_name)

        for expectation in system["expectations"]:
            asset_name = expectation["asset_name"]
            check_name = expectation["check_name"]
            expectation_type = expectation["expectation_type"]

            data_asset = data_source_manager.add_data_asset(data_source, asset_name)

            batch_definition = data_source_manager.add_whole_batch_definition(data_asset, check_name)
            
            connector = get_connector(
                system_type=system_type,
                system_name=system_name,
                asset_name=asset_name,
                asset_kwargs=expectation.get("asset_kwargs", {})
            )

            ExpectationClass = get_expectation_class(expectation_type)

            if ExpectationClass is not None:
                expectation_instance = ExpectationClass(**expectation["kwargs"], meta={"check_name": check_name, "data_product_name": data_product_name})
                logging.info(f"Expectation instance created: {expectation_instance}")

                batch = data_source_manager.add_batch_to_batch_definition(batch_definition, connector.get_dataframe())
                validation_result = data_source_manager.validate_expectation_on_batch(batch, expectation_instance)

            validation_results.append(validation_result.to_json_dict()) 

        logging.info("Validation Results created!")

    return validation_results

def validate_data_quality(json_file_path):
    try:
        env = os.getenv("ENV")
        if not env:
            logging.error(f"Environment variable {env} not found. Using system environment variables.")
            raise ValueError("ENV is required.")

        data_product_name = os.getenv("DATA_PRODUCT_NAME")
        if not data_product_name:
            logging.error(f"Environment variable {data_product_name} not found. Using system environment variables.")
            raise ValueError("DATA_PRODUCT_NAME is required.")
        
        logging.info("Reading the JSON configuration file...")
        json_file = load_json_file(json_file_path)

        logging.info("Configuring Expectations and running Validations...")
        validation_results = configure_expectations_and_run_validations(json_file, os.getenv("DATA_PRODUCT_NAME"))

        return validation_results

    except Exception as e:
        logging.error(f"An error occurred during application execution: {e}")
        sys.exit(1)

