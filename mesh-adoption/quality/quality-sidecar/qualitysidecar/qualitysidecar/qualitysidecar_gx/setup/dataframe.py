import logging
import great_expectations as gx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def add_data_source(context, data_source_name):
    data_source = get_existing_data_source(context, data_source_name)
    return data_source if data_source else context.data_sources.add_pandas(data_source_name)

def get_existing_data_source(context, data_source_name):
    try:
        return context.get_data_source(data_source_name)
    except Exception:
        return None

def add_data_asset_pandas(data_source, data_asset_name):
    data_asset = get_existing_data_asset_pandas(data_source, data_asset_name)
    return data_asset if data_asset else data_source.add_dataframe_asset(name=data_asset_name)

def get_existing_data_asset_pandas(data_source, data_asset_name):
    try:
        return data_source.get_asset(data_asset_name)  
    except Exception:
        return None
    
def add_data_asset_spark(data_source, data_asset_name):
    data_asset = get_existing_data_asset_spark(data_source, data_asset_name)
    return data_asset if data_asset else data_source.add_dataframe_asset(name=data_asset_name)

def get_existing_data_asset_spark(data_source, data_asset_name):
    try:
        return data_source.get_asset(data_asset_name)  
    except Exception:
        return None

def add_whole_batch_definition(data_asset, batch_definition_name):
    return data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

def get_expectation_class(expectation_type):
    try:
        ExpectationClass = getattr(gx.expectations.core, expectation_type)

        if isinstance(ExpectationClass, type):
            return ExpectationClass
        else:
            logging.error(f"{expectation_type} is not a valid expectation class!")
    except Exception as e:
        logging.error(f"Error processing expectation {expectation_type}: {str(e)}")

def add_batch_to_batch_definition(batch_definition, dataframe):
    return batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

def validate_expectation_on_batch(batch, expectation):
    return batch.validate(expectation)

