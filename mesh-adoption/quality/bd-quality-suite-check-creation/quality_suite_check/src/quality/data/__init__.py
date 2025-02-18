import json
import os.path
import logging
import pdb
import pandas as pd
import numpy as np

from pathlib import Path
from datetime import datetime
from jsonschema import validate
from quality.configurations.ConfigurationProperties import BaseConfig


def read_configuration_schema(file_path: str | Path):
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            config_schema = json.load(file)
        
        logging.info('Configuration data schema file read correctly.')
        return config_schema
    else:
        logging.error(f'Could not find configuration schema file in "{file_path}"')
        return {}
    

def read_configuration(file_path: str | Path):
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            config_data = json.load(file)

        logging.info('Configuration data file read correctly.')
        return config_data
    else:
        logging.error(f'Could not find configuration file in "{file_path}"')
        return []


def get_configuration(configuration: BaseConfig,
                      configuration_schema_file_path: str | Path = None,
                      configuration_file_path: str | Path = None):
    data_schema = read_configuration_schema(configuration.CONFIGURATION_SCHEMA_FILE_PATH if configuration_schema_file_path is None else configuration_schema_file_path)
    data = read_configuration(configuration.CONFIGURATION_FILE_PATH if configuration_file_path is None else configuration_file_path)

    try:
        validate(instance=data, schema=data_schema)
        logging.info('Configuration data file validated succesfully: the schema is correct.')
        return data
    except Exception as e:
        logging.error(f'Configuration data file not valid: {e}')
        return []


def convert_excel_to_json(configuration: BaseConfig,
                          excel_file_path: str | Path = None):
    input_file = configuration.CONFIGURATION_FILE_PATH if excel_file_path is None else excel_file_path
    output_file = f"{os.path.dirname(input_file)}/{datetime.today().strftime('%Y%m%d%H%M%S')}.json"

    in_suite = pd.read_excel(input_file, sheet_name='Data Quality Suite')
    in_check = pd.read_excel(input_file, sheet_name='Data Quality Check')
    in_assoc = pd.read_excel(input_file, sheet_name='Physical Association')

    in_suite.drop_duplicates(subset=['Business Domain', 
                                     'Data Product'], 
                             inplace=True, 
                             ignore_index=True)
    in_check.drop_duplicates(subset=['Data Product',
                                     'Name'],
                             inplace=True, 
                             ignore_index=True)
    in_assoc.replace({r'^\s*$': None, np.nan: None}, regex=True, inplace=True)

    data = [
        {
            "business_domain_name": suite_row['Business Domain'].strip(),
            "data_product_name": suite_row['Data Product'].strip(),
            "data_quality_checks": [
                {
                    "data_quality_check_name": str(check_row['Name']).strip(),
                    "data_quality_check_description": str(check_row['Description']).strip(),
                    "data_quality_dimension": str(check_row['Dimension']).strip(),
                    "score_strategy": str(check_row['Score Strategy']).strip(),
                    "score_warning_threshold": check_row['Warning Threshold'],
                    "score_success_threshold": check_row['Success Threshold'],
                    "physical_associations": [
                        {
                            "system": str(assoc_row['Physical System']).strip(),
                            "physical_entity_schema": str(assoc_row['Physical Entity Schema']).strip(),
                            "physical_entity_name": str(assoc_row['Physical Entity Name']).strip(),
                            
                        } if assoc_row['Physical Field'] is None else {
                            "system": str(assoc_row['Physical System']).strip(),
                            "physical_entity_schema": str(assoc_row['Physical Entity Schema']).strip(),
                            "physical_entity_name": str(assoc_row['Physical Entity Name']).strip(),
                            "physical_field_name": str(assoc_row['Physical Field']).strip()
                        } for _, assoc_row in in_assoc[(in_assoc['Data Product'] == check_row['Data Product']) & (in_assoc['Data Quality Check'] == check_row['Name'])].iterrows()
                    ]
                } for _, check_row in in_check[in_check['Data Product'] == suite_row['Data Product']].iterrows()
            ]
        } for _, suite_row in in_suite.iterrows()
    ]

    with open(output_file, "w") as file:
        file.write(json.dumps(data))

    logging.info(f'Excel configuration file succesfully converted to JSON as {os.path.basename(output_file)}.')
    return output_file