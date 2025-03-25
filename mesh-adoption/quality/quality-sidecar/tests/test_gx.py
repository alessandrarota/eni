from qualitysidecar.qualitysidecar_gx import extract_configurations, retrieve_dataframe_from_configuration, run_validations
import os
import pandas as pd
import pytest
from datetime import datetime
import logging

def execute_main_without_loop(path):
    configurations = extract_configurations(path)
    results = []

    for configuration in configurations:
        dataframe = retrieve_dataframe_from_configuration(configuration)
        results.append(run_validations(configuration, dataframe))

    return results

def test_gx_v1():
    validation_results = execute_main_without_loop('/app/tests/resources/csv_v1.json')

    assert len(validation_results) == 4

    result = validation_results[0]
    assert result["success"] is False
    assert len(result["result"]) == 4
    assert result["result"]["unexpected_count"] == 1
    assert result["result"]["unexpected_percent"] == 14.285714285714285
    assert result["result"]["expectation_output_metric"] == None
    assert result["result"]["element_count"] == 7
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_in_set"
    assert result["expectation_config"]["meta"]["check_name"] == "check1"

    result = validation_results[1]
    assert result["success"] is True
    assert len(result["result"]) == 4
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["result"]["expectation_output_metric"] == None
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_between"
    assert result["expectation_config"]["meta"]["check_name"] == "check2"

    result = validation_results[2]
    assert result["success"] is True
    assert len(result["result"]) == 4
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["result"]["expectation_output_metric"] == None
    assert result["expectation_config"]["type"] == "expect_column_values_to_not_be_null"
    assert result["expectation_config"]["meta"]["check_name"] == "check3"

    result = validation_results[3]
    assert result["success"] is True
    assert len(result["result"]) == 4
    assert result["result"]["unexpected_count"] == 0
    assert result["result"]["unexpected_percent"] == 0.0
    assert result["result"]["element_count"] == 7
    assert result["result"]["expectation_output_metric"] == None
    assert result["expectation_config"]["type"] == "expect_column_values_to_be_unique"
    assert result["expectation_config"]["meta"]["check_name"] == "check4"