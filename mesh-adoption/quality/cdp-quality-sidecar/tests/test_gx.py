import pytest
from datetime import datetime
import logging
from app import read_json_file, setup_gx_resources, setup_validation_definitions
import os
import pandas as pd
from gx_setup.gx_dataframe import *

def execute_main_without_loop(path, data_product_name):
    gx_json_data = read_json_file(path)
    gx_resources = setup_gx_resources(gx_json_data, data_product_name)
    validation_defs = setup_validation_definitions(gx_resources)
    
    results = [
        validation_run(
            df=pd.read_csv(gx_json_data[i]["physical_informations"]["dataframe"], delimiter=','),
            validation_definition=validation_def
        )
        for i, validation_def in enumerate(validation_defs)
    ]
    
    return results

def test_gx_v1():
    validation_results = execute_main_without_loop(path='/app/tests/resources/v1/gx_v1.json', data_product_name='v1')

    results = validation_results[0]

    assert results["success"] == False
    assert len(results["results"]) == 4

    result = results["results"][0]
    assert result["success"] is False
    assert result["result"]["unexpected_count"] == 1
    assert result["result"]["unexpected_percent"] == 14.285714285714285
    assert "Paris" in result["result"]["partial_unexpected_list"]
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_in_set"

    result = results["results"][1]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_between"

    result = results["results"][2]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_not_be_null"

    result = results["results"][3]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_unique"

def test_gx_v2():
    validation_results = execute_main_without_loop(path='/app/tests/resources/v2/gx_v2.json', data_product_name='v2')
    results = validation_results[0]

    assert results["success"] == True
    assert len(results["results"]) == 3

    result = results["results"][0]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 6
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_in_set"

    result = results["results"][1]
    assert result["success"] is True
    assert result["result"]["observed_value"] == "float64"
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_of_type"

    result = results["results"][2]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 6
    assert result["expectation_config"]["type"] == "expect_column_values_to_match_regex"