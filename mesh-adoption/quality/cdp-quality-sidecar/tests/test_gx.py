import pytest
from datetime import datetime
import logging
from app import read_json_file, setup_gx
import os
import pandas as pd
from gx_setup.gx_dataframe import *

def test_gx_v1():
    gx_json_data = read_json_file('/app/tests/resources/v1/gx_v1.json')
    data_product_name = 'v1'

    validation_defs = setup_gx(gx_json_data, data_product_name)

    data_product_suite = gx_json_data["data_product_suites"][0]
    physical_informations = data_product_suite["physical_informations"]

    results = validation_run(df=pd.read_csv(physical_informations["dataframe"], delimiter=','), validation_definition=validation_defs[0])

    assert results["success"] == False
    assert len(results["results"]) == 4

    # expectCityValuesToBeInSet
    result = results["results"][0]
    assert result["success"] is False
    assert result["result"]["unexpected_count"] == 1
    assert result["result"]["unexpected_percent"] == 14.285714285714285
    assert "Paris" in result["result"]["partial_unexpected_list"]
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_in_set"

    # expectUnitPriceValuesToBeBetween
    result = results["results"][1]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_between"

    # expectSalespersonValuesToNotBeNull
    result = results["results"][2]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_not_be_null"

    # expectCityValuesToBeInSet
    result = results["results"][3]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_unique"

def test_gx_v2():
    gx_json_data = read_json_file('/app/tests/resources/v2/gx_v2.json')
    data_product_name = 'v2'

    validation_defs = setup_gx(gx_json_data, data_product_name)

    data_product_suite = gx_json_data["data_product_suites"][0]
    physical_informations = data_product_suite["physical_informations"]

    results = validation_run(df=pd.read_csv(physical_informations["dataframe"], delimiter=','), validation_definition=validation_defs[0])

    assert results["success"] == True
    assert len(results["results"]) == 3

    # expectGenderToBeInSet
    result = results["results"][0]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 6
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_in_set"

    # expectPurchaseAmountValuesToBeOfType
    result = results["results"][1]
    assert result["success"] is True
    assert result["result"]["observed_value"] == "float64"
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_of_type"

    # expectPurchaseDateValuesToMatchRegex
    result = results["results"][2]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 6
    assert result["expectation_config"]["type"] == "expect_column_values_to_match_regex"