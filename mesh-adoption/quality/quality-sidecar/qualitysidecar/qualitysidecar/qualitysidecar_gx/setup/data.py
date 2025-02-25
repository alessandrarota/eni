import logging
import great_expectations as gx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataSource:
    def __init__(self, context):
        self.context = context
    
    def add_data_source(self):
        raise NotImplementedError("Subclasses should implement this method.")
    
    def get_existing_data_source(self):
        raise NotImplementedError("Subclasses should implement this method.")
        
    def add_data_asset(self, data_source, data_asset_name):
        data_asset = self.get_existing_data_asset(data_source, data_asset_name)
        return data_asset if data_asset else data_source.add_dataframe_asset(name=data_asset_name)
    
    def get_existing_data_asset(self, data_source, data_asset_name):
        try:
            return data_source.get_asset(data_asset_name)
        except Exception:
            return None
    
    def add_whole_batch_definition(self, data_asset, batch_definition_name):
        return data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

    def add_batch_to_batch_definition(self, batch_definition, dataframe):
        return batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

    def validate_expectation_on_batch(self, batch, expectation):
        return batch.validate(expectation)


class PandasDataSource(DataSource):
    def add_data_source(self, data_source_name):
        data_source = self.get_existing_data_source(data_source_name)
        return data_source if data_source else self.context.data_sources.add_pandas(data_source_name)
    
    def get_existing_data_source(self, data_source_name):
        try:
            return self.context.get_data_source(data_source_name)
        except Exception:
            return None


class SparkDataSource(DataSource):
    def add_data_source(self, data_source_name):
        data_source = self.get_existing_data_source(data_source_name)
        return data_source if data_source else self.context.data_sources.add_spark(data_source_name)

    def get_existing_data_source(self, data_source_name):
        try:
            return self.context.get_data_source(data_source_name)
        except Exception:
            return None
