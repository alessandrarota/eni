from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
import logging
import great_expectations as gx

logging.basicConfig(level=logging.INFO)

def data_source_definition(context, data_source_name):
    return context.data_sources.add_pandas(data_source_name)

def data_asset_definition(data_source, data_asset_name):
    return data_source.add_dataframe_asset(name=data_asset_name)

def whole_batch_definition(data_asset, batch_definition_name):
    return data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

def suite_definition(context, data_product_name, suite_name, suite_expectations):
    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)
    for exp in suite_expectations:
        ec = ExpectationConfiguration(
            type=exp["expectation_type"], 
            kwargs=exp["kwargs"], 
            meta={
                "expectation_name": exp["expectation_name"],
                "data_product_name": data_product_name
            }
        )
        suite.add_expectation_configuration(ec)
    suite.save()

    return suite

def validation_definition(context, batch_definition, suite):
    print()
    validation = gx.ValidationDefinition(data=batch_definition, suite=suite, name=suite.name)
    return context.validation_definitions.add(validation)

def validation_run(df, validation_definition):
    return validation_definition.run(batch_parameters={"dataframe": df})
    

