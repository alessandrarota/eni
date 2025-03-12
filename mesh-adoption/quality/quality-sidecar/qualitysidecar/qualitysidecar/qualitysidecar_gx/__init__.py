import sys
import json
import os
import great_expectations as gx
from .connectors.SystemConnector import *
from .setup.data import SparkDataSource, PandasDataSource
from .setup.expectation import get_expectation_class
from .utils.utils import clean_kwargs_columns

# Logging configuration
logging.getLogger("great_expectations").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_configurations(file_path):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            
            if not isinstance(data, list) or not data:
                logging.error("Invalid JSON format: Expected a list with at least one element.")
                sys.exit(1)
            
            system_name = data[0].get("system_name", "Unknown")
            system_type = data[0].get("system_type", "Unknown")
            
            expectations = data[0].get("expectations", [])
            for exp in expectations:
                exp["system_name"] = system_name
                exp["system_type"] = system_type
            
            return expectations
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error reading or decoding the JSON file: {file_path} - {e}")
        sys.exit(1)

def retrieve_dataframe_from_configuration(configuration):
    system_name = configuration.get("system_name", "unknown")
    system_type = configuration.get("system_type", "unknown")

    asset_name = configuration.get("asset_name", "unknown")
    check_name = configuration.get("check_name", "unknown")
    asset_kwargs = configuration.get("asset_kwargs", {})

    try:
        connector = get_connector(
            system_type=system_type,
            system_name=system_name,
            asset_name=asset_name,
            asset_kwargs=asset_kwargs
        )

        return connector.extract()            
    except Exception as e:
        logging.error(f"Error processing expectation {check_name} for system {system_name}: {e}")

def run_validations(configuration, dataframe):
    data_product_name = os.getenv("DATA_PRODUCT_NAME")
    if not data_product_name:
        logging.error(f"Environment variable {data_product_name} not found.")
        raise ValueError("DATA_PRODUCT_NAME is required.")
    
    context = gx.get_context()
    data_source_manager = SparkDataSource(context)

    system_name = configuration.get("system_name", "unknown")
    expectation_type = configuration.get("expectation_type", "unknown")
    kwargs = configuration.get("kwargs", {})
    asset_name = configuration.get("asset_name", "unknown")
    check_name = configuration.get("check_name", "unknown")

    try:
        data_source = data_source_manager.add_data_source(system_name)
    except Exception as e:
        logging.error(f"Error adding data source for system {system_name}: {e}")

    try:
        data_asset = data_source_manager.add_data_asset(data_source, asset_name)
        batch_definition = data_source_manager.add_whole_batch_definition(data_asset, check_name)
        ExpectationClass = get_expectation_class(expectation_type)
        
        if ExpectationClass is not None:
            expectation_instance = ExpectationClass(
                **clean_kwargs_columns(kwargs),
                meta={"check_name": check_name, "data_product_name": data_product_name}
            )
            logging.info(f"Expectation instance created: {expectation_instance}")
            
            batch = data_source_manager.add_batch_to_batch_definition(batch_definition, dataframe)
            validation_result = data_source_manager.validate_expectation_on_batch(batch, expectation_instance)
            return validation_result.to_json_dict()
        
    except Exception as e:
        logging.error(f"Error processing expectation {check_name} for system {system_name}: {e}")
