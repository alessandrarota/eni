import pandas as pd
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
import logging
import os
import io
import re
from ..utils.utils import clean_df_columns

class SystemConnector(ABC):
    
    def __init__(self, system_type, system_name, asset_name, **kwargs):
        self.system_type = system_type
        self.system_name = system_name
        self.asset_name = asset_name
        self.kwargs = kwargs if kwargs else {}
    
    @abstractmethod
    def extract(self):
        """
        Extracts data and returns a Spark DataFrame.
        """
        pass

    @staticmethod
    def get_or_create_spark_session(app_name="SparkSession"):
        try:
            return spark
        except NameError:
            logging.info("Creating new SparkSession...")
            return SparkSession.builder.appName(app_name).getOrCreate()


class CSVConnector(SystemConnector):
    def extract(self):
        try:
            path = self.kwargs.get('path')
            if not path:
                raise ValueError("The 'path' parameter is required.")
            
            spark = self.get_or_create_spark_session("CSVConnector")
            df_spark = spark.read.csv(path, header=True, inferSchema=True)
            
            return clean_df_columns(df_spark)
        except Exception as e:
            logging.error(f"Error extracting CSV: {e}")
            return None


class HiveConnector(SystemConnector):
    def extract(self):
        try:
            spark = self.get_or_create_spark_session("HiveConnector")
            query = f"SELECT * FROM {self.system_name}.{self.asset_name}"
            df_spark = spark.sql(query)
            
            return clean_df_columns(df_spark)
        except Exception as e:
            logging.error(f"Error extracting data from Hive: {e}")
            return None


class UnityConnector(SystemConnector):
    def extract(self):
        try:
            spark = self.get_or_create_spark_session("UnityConnector")
            schema, table = self.asset_name.split(".")
            query = f"SELECT * FROM `{self.system_name}`.`{schema}`.`{table}`"
            df_spark = spark.sql(query)
            
            return clean_df_columns(df_spark)
        except Exception as e:
            logging.error(f"Error extracting data from Unity: {e}")
            return None


class ADLSConnector(SystemConnector):
    def extract(self):
        try:
            spark = self.get_or_create_spark_session("ADLSConnector")
            file_path = self.kwargs.get('path')
            file_type = self.kwargs.get('type')
            
            if not file_path or not file_type:
                raise ValueError("Both 'path' and 'type' parameters are required.")
            
            adls_path = f"abfss://{self.asset_name}@{self.system_name}.dfs.core.windows.net/{file_path}"
            
            if file_type == "delta":
                df = spark.read.format("delta").load(adls_path)
            elif file_type == "parquet":
                df = spark.read.format("parquet").load(adls_path)
            elif file_type == "csv":
                df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(adls_path)
            elif file_type == "json":
                df = spark.read.format("json").load(adls_path)
            else:
                raise ValueError("Unsupported file type. Use 'delta', 'parquet', 'csv', or 'json'.")
            
            return clean_df_columns(df)
        except ValueError as ve:
            logging.error(f"ValueError: {ve}")
            return None
        except Exception as e:
            logging.error(f"Error during data extraction from ADLS: {e}")
            return None


class SystemConnectorFactory:
    @staticmethod
    def get_connector(system_type, system_name, asset_name, **kwargs):
        if system_type == 'CSV':
            return CSVConnector(system_type, system_name, asset_name, **kwargs)
        elif system_type == 'HIVE':
            return HiveConnector(system_type, system_name, asset_name, **kwargs)
        elif system_type == 'UNITY':
            return UnityConnector(system_type, system_name, asset_name, **kwargs)
        elif system_type == 'ADLS':
            return ADLSConnector(system_type, system_name, asset_name, **kwargs)
        else:
            raise ValueError(f"Unsupported system type: {system_type}")


connectors = {}
def get_connector(system_type, system_name, asset_name, asset_kwargs):
    connector_key = (system_type, system_name, asset_name)
    
    if connector_key in connectors:
        logging.info(f"Reusing connector for {connector_key}")
        return connectors[connector_key]

    connector = SystemConnectorFactory.get_connector(
        system_type=system_type,
        system_name=system_name,
        asset_name=asset_name,
        **asset_kwargs
    )

    connectors[connector_key] = connector
    logging.info(f"Created new connector for {connector_key}")
    return connector
