import pytest
import os
import great_expectations as gx
from pyspark.sql import SparkSession
from utils.ResultFormatter import ResultFormatter
from typing import Any, Dict, cast
from expectations.query_expectation.ExpectQueriedTableRowCountToBe import ExpectQueriedTableRowCountToBe
from expectations.column_aggregate_expectation.ExpectColumnMinToBeBetween import ExpectColumnMinToBeBetween
from expectations.column_aggregate_expectation.ExpectColumnMaxToBeBetween import ExpectColumnMaxToBeBetween
from expectations.column_aggregate_expectation.ExpectColumnMeanToBeBetween import ExpectColumnMeanToBeBetween
from expectations.column_aggregate_expectation.ExpectColumnMedianToBeBetween import ExpectColumnMedianToBeBetween
from expectations.ExpectTableRowCountToBeBetween import ExpectTableRowCountToBeBetween
from expectations.ExpectTableRowCountToEqual import ExpectTableRowCountToEqual
from expectations.ExpectTableColumnsToMatchOrderedList import ExpectTableColumnsToMatchOrderedList
from expectations.column_aggregate_expectation.ExpectColumnDistinctValuesToBeInSet import ExpectColumnDistinctValuesToBeInSet
from expectations.column_aggregate_expectation.ExpectColumnDistinctValuesToContainSet import ExpectColumnDistinctValuesToContainSet
from expectations.column_aggregate_expectation.ExpectColumnDistinctValuesToEqualSet import ExpectColumnDistinctValuesToEqualSet
from expectations.column_map_expectation.ExpectColumnValuesToBeInTypeList import ExpectColumnValuesToBeInTypeList


@pytest.fixture(scope="session")
def setup_spark_and_data():
    result_formatter = ResultFormatter()
    
    spark = SparkSession.builder.appName("ParquetReader").getOrCreate()
    
    current_dir = os.path.dirname(__file__)
    parquet_file_path = os.path.join(current_dir, 'resources', 'yellow_tripdata_2024-01.parquet')
    df = spark.read.parquet(parquet_file_path)
    print(df.show(5)) 
    print(df.columns)

    context = gx.get_context()
    data_source = context.data_sources.add_spark("data_source")
    data_asset = data_source.add_dataframe_asset("data_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")

    return {
        "df": df,
        "batch_definition": batch_definition,
        "result_formatter": result_formatter
    }

def validate(setup_spark_and_data, expectation_instance):
    batch_definition = setup_spark_and_data["batch_definition"]
    df = setup_spark_and_data["df"]
    result_formatter = setup_spark_and_data["result_formatter"]

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df.limit(5)})
    results = batch.validate(expectation_instance)
    formatted_results = result_formatter.format_result(results)
    print(formatted_results)
    return formatted_results

def test_ExpectColumnMaxToBeBetween(setup_spark_and_data): 
    expectation_instance = ExpectColumnMaxToBeBetween(
        column="extra",
        min_value=0,
        max_value=3,
        strict_max=False
    )
    
    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] == 3.5
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectColumnMinToBeBetween(setup_spark_and_data): 
    expectation_instance = ExpectColumnMinToBeBetween(
        column="extra",
        min_value=0,
        max_value=3,
        strict_max=False
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] == 1.0
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectColumnMeanToBeBetween(setup_spark_and_data): 
    expectation_instance = ExpectColumnMeanToBeBetween(
        column="extra",
        min_value=0,
        max_value=3,
        strict_max=False
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] == 3.0
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectColumnMedianToBeBetween(setup_spark_and_data): 
    expectation_instance = ExpectColumnMedianToBeBetween(
        column="extra",
        min_value=0,
        max_value=3,
        strict_max=False
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] == 3.5
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectTableRowCountToBeBetween(setup_spark_and_data): 
    expectation_instance = ExpectTableRowCountToBeBetween(
        min_value=5,
        max_value=10,
        strict_min=True
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] == 5
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectTableRowCountToEqual(setup_spark_and_data): 
    expectation_instance = ExpectTableRowCountToEqual(
        value=5
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] == 5
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectTableColumnsToMatchOrderedList(setup_spark_and_data): 
    expectation_instance = ExpectTableColumnsToMatchOrderedList(
        column_list=['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] is None
    assert result["expectation_output_metric"] is False
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectColumnDistinctValuesToBeInSet(setup_spark_and_data): 
    expectation_instance = ExpectColumnDistinctValuesToBeInSet(
        column="extra",
        value_set=[1, 2, 3, 4, 5]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 4
    assert result["unexpected_percent"] == 80.0


    expectation_instance = ExpectColumnDistinctValuesToBeInSet(
        column="extra",
        value_set=[1, 3.5]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0

def test_ExpectColumnDistinctValuesToContainSet(setup_spark_and_data): 
    expectation_instance = ExpectColumnDistinctValuesToContainSet(
        column="extra",
        value_set=[1]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is True
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None


    expectation_instance = ExpectColumnDistinctValuesToContainSet(
        column="extra",
        value_set=[1, 3.5]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is True
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None


    expectation_instance = ExpectColumnDistinctValuesToContainSet(
        column="extra",
        value_set=[1, 2, 3.5]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is False
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectColumnDistinctValuesToEqualSet(setup_spark_and_data): 
    expectation_instance = ExpectColumnDistinctValuesToEqualSet(
        column="extra",
        value_set=[1, 2, 3.5]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is False
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None


    expectation_instance = ExpectColumnDistinctValuesToEqualSet(
        column="extra",
        value_set=[1, 3.5]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is True
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectColumnPairValuesAToBeGreaterThanB(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="extra",
        column_B="Airport_fee",
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0


    expectation_instance = gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="extra",
        column_B="payment_type",
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 1
    assert result["unexpected_percent"] == 20.0

def test_ExpectColumnPairValuesToBeEqual(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnPairValuesToBeEqual(
        column_A="extra",
        column_B="Airport_fee",
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 5
    assert result["unexpected_percent"] == 100.0


    expectation_instance = gx.expectations.ExpectColumnPairValuesToBeEqual(
        column_A="passenger_count",
        column_B="RatecodeID",
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0

def test_ExpectColumnPairValuesToBeInSet(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnPairValuesToBeInSet(
        column_A="VendorID",
        column_B="passenger_count",
        value_pairs_set=[(2,1), (1,1)],
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0


    expectation_instance = gx.expectations.ExpectColumnPairValuesToBeInSet(
        column_A="extra",
        column_B="RatecodeID",
        value_pairs_set=[(1,1)],
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 4
    assert result["unexpected_percent"] == 80.0

def test_ExpectColumnValueLengthsToBeBetween(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnValueLengthsToBeBetween(
        column="tpep_pickup_datetime",
        min_value=1,
        max_value=30
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0


    expectation_instance = gx.expectations.ExpectColumnValueLengthsToBeBetween(
        column="tpep_pickup_datetime",
        min_value=1,
        max_value=5
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 5
    assert result["unexpected_percent"] == 100.0

def test_ExpectColumnValueLengthsToEqual(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnValueLengthsToEqual(
        column="tpep_pickup_datetime",
        value=19
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0


    expectation_instance = gx.expectations.ExpectColumnValueLengthsToEqual(
        column="tpep_pickup_datetime",
        value=20
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 5
    assert result["unexpected_percent"] == 100.0

def test_ExpectColumnValuesToBeBetween(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnValuesToBeBetween(
        column="extra",
        min_value=1,
        max_value=20
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0


    expectation_instance = gx.expectations.ExpectColumnValuesToBeBetween(
        column="extra",
        min_value=1,
        max_value=3
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 4
    assert result["unexpected_percent"] == 80.0

def test_ExpectColumnValuesToBeInTypeList(setup_spark_and_data): 
    expectation_instance = ExpectColumnValuesToBeInTypeList(
        column="extra",
        type_list=["DoubleType", "StringType"]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is True
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None


    expectation_instance = ExpectColumnValuesToBeInTypeList(
        column="extra",
        type_list=["StringType"]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is False
    assert result["unexpected_count"] is None
    assert result["unexpected_percent"] is None

def test_ExpectColumnValuesToMatchRegexList(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnValuesToMatchRegexList(
        column="tpep_dropoff_datetime",
        regex_list =["^2024-01-01 00:\d{2}:[0-5]?[0-9]$"]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 1
    assert result["unexpected_percent"] == 20.0


    expectation_instance = gx.expectations.ExpectColumnValuesToMatchRegexList(
        column="tpep_dropoff_datetime",
        regex_list =["^2024-01-01 \d{2}:\d{2}:[0-5]?[0-9]$"]
    )
    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0.0

def test_ExpectColumnValuesToNotBeInSet(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnValuesToNotBeInSet(
        column="extra",
       value_set=[2, 4],
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0.0


    expectation_instance = gx.expectations.ExpectColumnValuesToNotBeInSet(
        column="extra",
       value_set=[2, 3.5],
    )
    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 4
    assert result["unexpected_percent"] == 80.0

def test_ExpectColumnValuesToBeNull(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnValuesToBeNull(
        column="extra",
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 5
    assert result["unexpected_percent"] == 100.0

def test_ExpectColumnValuesToNotBeNull(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="extra",
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is True
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 0
    assert result["unexpected_percent"] == 0

def test_ExpectColumnValuesToNotMatchRegexList(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectColumnValuesToNotMatchRegexList(
        column="tpep_dropoff_datetime",
        regex_list =["^2024-01-01 00:\d{2}:[0-5]?[0-9]$"]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 4
    assert result["unexpected_percent"] == 80.0

def test_ExpectSelectColumnValuesToBeUniqueWithinRecord(setup_spark_and_data): 
    expectation_instance = gx.expectations.ExpectSelectColumnValuesToBeUniqueWithinRecord(
        column_list=["RatecodeID", "payment_type"]
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] is None
    assert result["unexpected_count"] == 4
    assert result["unexpected_percent"] == 80.0


def test_ExpectQueriedTableRowCountToBe(setup_spark_and_data):    
    expectation_instance = ExpectQueriedTableRowCountToBe(
        value=4
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] == 5


    expectation_instance = ExpectQueriedTableRowCountToBe(
        value=3,
        query="SELECT COUNT(*) FROM {batch} WHERE extra = 3.5"
    )

    results = validate(setup_spark_and_data, expectation_instance)
    assert results["success"] is False
    result = cast(Dict[str, Any], results["result"])
    assert result["element_count"] == 5
    assert result["expectation_output_metric"] == 4
