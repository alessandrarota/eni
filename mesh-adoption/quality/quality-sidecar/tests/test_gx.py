import pytest
from datetime import datetime
import logging
from data_quality_gx import load_json_file, configure_expectations_and_run_validations
import os
import pandas as pd

def execute_main_without_loop(path, data_product_name):
    gx_json_data = load_json_file(path)

    return configure_expectations_and_run_validations(gx_json_data, data_product_name)

def test_gx_v1():
    validation_results = execute_main_without_loop('/app/tests/resources/v1/csv_v1.json', "v1")

    print(validation_results)

    assert len(validation_results) == 4

    result = validation_results[0]
    assert result["success"] is False
    assert result["result"]["unexpected_count"] == 1
    assert result["result"]["unexpected_percent"] == 14.285714285714285
    assert "Paris" in result["result"]["partial_unexpected_list"]
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_in_set"
    assert result["expectation_config"]["meta"]["check_name"] == "check1"

    result = validation_results[1]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_between"
    assert result["expectation_config"]["meta"]["check_name"] == "check2"

    result = validation_results[2]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_not_be_null"
    assert result["expectation_config"]["meta"]["check_name"] == "check3"

    result = validation_results[3]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_unique"
    assert result["expectation_config"]["meta"]["check_name"] == "check4"

def test_gx_v2():
    validation_results = execute_main_without_loop('/app/tests/resources/v2/csv_v2.json', "v2")

    assert len(validation_results) == 3

    result = validation_results[0]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 6
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_in_set"
    assert result["expectation_config"]["meta"]["check_name"] == "check1"

    result = validation_results[1]
    assert result["success"] is True
    assert result["result"]["observed_value"] == "float64"
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_of_type"
    assert result["expectation_config"]["meta"]["check_name"] == "check2"

    result = validation_results[2]
    assert result["success"] is True
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 6
    assert result["expectation_config"]["type"] == "expect_column_values_to_match_regex"
    assert result["expectation_config"]["meta"]["check_name"] == "check3"