from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
import logging
import great_expectations as gx

logging.basicConfig(level=logging.INFO)

def add_data_source(context, data_source_name):
    return context.data_sources.add_pandas(data_source_name)

def get_existing_data_source(context, data_source_name):
    try:
        return context.get_data_source(data_source_name)  
    except AttributeError:
        return None  
    except Exception as e:
        #print(f"Error retrieving data source: {e}")
        return None

def add_data_asset(data_source, data_asset_name):
    return data_source.add_dataframe_asset(name=data_asset_name)

def get_existing_data_asset(data_source, data_asset_name):
    try:
        return data_source.get_asset(data_asset_name)  
    except AttributeError:
        return None  
    except Exception as e:
        #print(f"Error retrieving data asset: {e}")
        return None

def add_whole_batch_definition(data_asset, batch_definition_name):
    return data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

def get_existing_batch_definition(data_asset, batch_definition_name):
    try:
        return data_asset.get_batch_definition(batch_definition_name) 
    except AttributeError:
        return None  
    except Exception as e:
        print(f"Error retrieving batch definition: {e}")
        return None


def add_suite(context, suite_name, suite_expectations):
    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)
    for exp in suite_expectations:
        ec = ExpectationConfiguration(
            type=exp["expectation_type"], 
            kwargs=exp["kwargs"], 
            meta={
                "check_name": exp["check_name"],
                "asset_name": exp["asset_name"],
                "asset_kwargs": exp["asset_kwargs"]
            }
        )
        suite.add_expectation_configuration(ec)
    suite.save()

    return suite

def add_validation_definition(context, batch_definition, suite):
    validation = gx.ValidationDefinition(data=batch_definition, suite=suite, name=suite.name)
    return context.validation_definitions.add(validation)

def validation_run(df, validation_definition):
    return validation_definition.run(batch_parameters={"dataframe": df})
    

