import logging
import great_expectations as gx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_expectation_class(expectation_type):
    try:
        ExpectationClass = getattr(gx.expectations.core, expectation_type)

        if isinstance(ExpectationClass, type):
            return ExpectationClass
        else:
            logging.error(f"{expectation_type} is not a valid expectation class!")
    except Exception as e:
        logging.error(f"Error processing expectation {expectation_type}: {str(e)}")