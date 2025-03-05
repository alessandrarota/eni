import great_expectations as gx
from plugins.ExpectQueriedTableRowCountToBe import ExpectQueriedTableRowCountToBe  # Modifica il path se necessario
from pyspark.sql import SparkSession
import requests
import os


file_path = "resources/segnogiornalieroquartooroario.parquet"

spark = SparkSession.builder.appName("ParquetReader").getOrCreate()
df = spark.read.parquet(file_path)
#print(df.head(5))

context = gx.get_context()
data_source = context.data_sources.add_spark("data_source")
data_asset = data_source.add_dataframe_asset("data_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")


def test_1(df, expected_count=5):    
    expectation_instance = ExpectQueriedTableRowCountToBe(value=expected_count)
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validation_results = batch.validate(expectation_instance)

    #assert validation_results["result"]["observed_value"] == df.count()
    print(df.count())
    return validation_results


def test_2(df, expected_count=96):    
    expectation_instance = ExpectQueriedTableRowCountToBe(
        value=expected_count,
        query="SELECT COUNT(*) FROM {batch} WHERE MACROZONA = 'SUD'")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validation_results = batch.validate(expectation_instance)
    print(df.count())
    
    #assert validation_results["result"]["observed_value"] == df.filter(df.Macrozona == 'SUD').count()
    return validation_results

# def test_3(df, expected_count=10, row_condition='col("Macrozona") == "NORD"'):
#     expectation_instance = ExpectQueriedTableRowCountToBe(
#         value=expected_count,
#         row_condition=row_condition)
#     batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
#     validation_results = batch.validate(expectation_instance)
    
#     print(validation_results["result"]["observed_value"] == df.filter(row_condition).count())
#     return validation_results

# def test_4(df, expected_count=100, mostly=0.9):    
#     expectation_instance = ExpectQueriedTableRowCountToBe(
#         value=expected_count,
#         mostly=mostly)
#     batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
#     validation_results = batch.validate(expectation_instance)
    
#     print(validation_results["result"]["observed_value"] <= expected_count * mostly)
#     return validation_results



print(test_1(df))
print(test_2(df))
#print(test_3(df))
#print(test_4(df))
