import great_expectations as gx
#from gx.expectactions import ExpectColumnValuesToBeBetween
from expectations.query.ExpectQueriedTableRowCountToBe import ExpectQueriedTableRowCountToBe
from expectations.gx_gallery.ExpectColumnMaxToBeBetween import ExpectColumnMaxToBeBetween

#from great_expectations.expectations import ExpectColumnMaxToBeBetween
from expectations.gx_gallery.complete.ExpectColumnValuesToBeBetween import ExpectColumnValuesToBeBetween
from pyspark.sql import SparkSession

import requests
import os


file_path = "resources/yellow_tripdata_2024-01.parquet"
spark = SparkSession.builder.appName("ParquetReader").getOrCreate()
df = spark.read.parquet(file_path)
#print(list(df.columns))
#print(df.show(5))
context = gx.get_context()
data_source = context.data_sources.add_spark("data_source")
data_asset = data_source.add_dataframe_asset("data_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")

def test_ExpectColumnValuesToBeBetween():  
    expectation_instance = ExpectColumnValuesToBeBetween(
        column="passenger_count",
        min_value=4,
        max_value=5
        )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validation_results = batch.validate(expectation_instance)

    #assert validation_results["result"]["observed_value"] == df.count()
    #print(df.count())
    return validation_results

def test_ExpectColumnMaxToBeBetween(): 
    expectation_instance = ExpectColumnMaxToBeBetween(
        column="extra",
        min_value=0,
        max_value=3,
        strict_max=False
        )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df.limit(5)})
    validation_results = batch.validate(expectation_instance)
    return validation_results

def test_ExpectColumnMinToBeBetween(): 
    expectation_instance = ExpectColumnMaxToBeBetween(
        column="extra",
        min_value=0,
        max_value=3,
        strict_max=False
        )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df.limit(5)})
    validation_results = batch.validate(expectation_instance)
    return validation_results


def ExpectQueriedTableRowCountToBe_1(df, expected_count=5):    
    expectation_instance = ExpectQueriedTableRowCountToBe(value=expected_count)
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validation_results = batch.validate(expectation_instance)

    #assert validation_results["result"]["observed_value"] == df.count()
    print(df.count())
    return validation_results


def ExpectQueriedTableRowCountToBe_2(df, expected_count=96):    
    expectation_instance = ExpectQueriedTableRowCountToBe(
        value=expected_count,
        query="SELECT COUNT(*) FROM {batch} WHERE MACROZONA = 'SUD'")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validation_results = batch.validate(expectation_instance)
    print(df.count())
    
    #assert validation_results["result"]["observed_value"] == df.filter(df.Macrozona == 'SUD').count()
    return validation_results



# print(ExpectQueriedTableRowCountToBe_1(df))
# print(ExpectQueriedTableRowCountToBe_2(df))
#print("test_ExpectColumnValuesToBeBetween")
#test_ExpectColumnValuesToBeBetween()
print("\n\n")
print(f"ExpectColumnMaxToBeBetween: {test_ExpectColumnMaxToBeBetween()}")

#print(test_ExpectColumnValuesToBeBetween())
#print(test_ExpectColumnMaxToBeBetween())

