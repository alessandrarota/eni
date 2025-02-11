import pandas as pd
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
import logging

class SystemConnector(ABC):
    
    def __init__(self, system_type, system_name, asset_name, **kwargs):
        self.system_type = system_type
        self.system_name = system_name
        self.data_asset_name = asset_name
        self.kwargs = kwargs if kwargs else {} 
        self.dataframe= None
    
    @abstractmethod
    def extract(self):
        pass

    def get_dataframe(self):
        if self.dataframe is None:
            self.extract()
        return self.dataframe

    def set_dataframe(self, df):
        self.dataframe = df


class CSVConnector(SystemConnector):
    def extract(self):
        try:
            path = self.kwargs.get('path')
            if not path:
                raise ValueError("The 'path' parameter is required.")
            df = pd.read_csv(path)
            self.set_dataframe(df)
        except Exception as e:
            (f"Error extracting CSV: {e}")
            self.set_dataframe(None)


class HiveConnector(SystemConnector):
    def extract(self):
        try:
            spark = SparkSession.builder.appName("HiveConnector").getOrCreate()

            schema, table = self.data_asset_name.split(".")

            if not schema or not table:
                raise ValueError("Both schema and table should be specified in 'data_asset_name'.")
            
            query = f"SELECT * FROM {schema}.{table}"
            df_spark = spark.sql(query)

            df = df_spark.toPandas()
            self.set_dataframe(df)
        
        except Exception as e:
            logging.info(f"Error extracting data from Hive: {e}")
            self.set_dataframe(None)


class UnityConnector(SystemConnector):
    def extract(self):
        try:
            spark = SparkSession.builder.appName("UnityConnector").getOrCreate()

            # Parsing schema and table name
            schema, table = self.data_asset_name.split(".")

            if not schema or not table:
                raise ValueError("Both schema and table should be specified in 'data_asset_name'.")

            # Running the query on Unity Catalog
            query = f"SELECT * FROM {self.system_name}.{schema}.{table}"
            df_spark = spark.sql(query)

            df = df_spark.toPandas()
            self.set_dataframe(df)
        
        except Exception as e:
            logging.info(f"Error extracting data from Unity: {e}")
            self.set_dataframe(None)


class SystemConnectorFactory:
    
    @staticmethod
    def get_connector(system_type, system_name, asset_name, **kwargs):
        if system_type == 'CSV':
            return CSVConnector(system_type, system_name, asset_name, **kwargs)
        elif system_type == 'HIVE':
            return HiveConnector(system_type, system_name, asset_name, **kwargs)
        elif system_type == 'UNITY':
            return UnityConnector(system_type, system_name, asset_name, **kwargs)
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