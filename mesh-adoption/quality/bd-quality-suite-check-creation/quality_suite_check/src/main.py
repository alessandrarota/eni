import sys
import logging
import os
import pdb

from quality.exceptions.exception_handler import handle_exceptions
from quality import create_processor
from quality import utils
from quality import data
from quality.blindata import business_domain
from quality.blindata import data_product
from quality.blindata import data_quality_suite
from quality.blindata import data_quality_check


def main():
    sys.excepthook = handle_exceptions
    configuration = create_processor()
    logging.info(f"Quality Suites & Checks entities setup started on {os.getenv('HOSTNAME')}...")
    
    if configuration.CONFIGURATION_FILE_PATH.endswith('.xlsx'):
        configuration_file_path = data.convert_excel_to_json(configuration, excel_file_path=configuration.CONFIGURATION_FILE_PATH)
        configuration_data = data.get_configuration(configuration, configuration_file_path=configuration_file_path)
    elif configuration.CONFIGURATION_FILE_PATH.endswith('.json'):
        configuration_data = data.get_configuration(configuration, configuration_file_path=configuration.CONFIGURATION_FILE_PATH)
    else:
        configuration_data = []
        logging.error(f'Configuration file extension "{configuration.CONFIGURATION_FILE_PATH.split(".")[-1]}" not supported.')

    business_domains = business_domain.get_business_domains(configuration)
    data_products = data_product.get_data_products(configuration)

    for curr_idx, curr_configuration_data in enumerate(configuration_data):
        logging.info(f'Processing object {curr_idx:02d} in configuration.')

        curr_business_domain_name = curr_configuration_data['business_domain_name']
        curr_data_product_name = curr_configuration_data['data_product_name']

        if not utils.clean_name_string(curr_business_domain_name) in business_domains:
            curr_business_domain = None
            logging.error(f'Object {curr_idx:02d} - Business Domain "{curr_business_domain_name}" not found.')
        else:
            curr_business_domain = business_domains[utils.clean_name_string(curr_business_domain_name)]
            logging.info(f'Object {curr_idx:02d} - Business Domain "{curr_business_domain["displayName"]}" successfully found.')
        
        if not utils.clean_name_string(curr_data_product_name) in data_products:
            curr_data_product = None
            logging.error(f'Object {curr_idx:02d} - Data Product "{curr_data_product_name}" not found.')
        else:
            curr_data_product = data_products[utils.clean_name_string(curr_data_product_name)]
            logging.info(f'Object {curr_idx:02d} - Data Product "{curr_data_product["displayName"]}" successfully found.')
        
        if curr_business_domain is not None and curr_data_product is not None:
            data_quality_suite_uuid = data_quality_suite.get_data_quality_suite_uuid(configuration, curr_business_domain, curr_data_product)
            if data_quality_suite_uuid is not None:
                logging.info(f'Object {curr_idx:02d} - Processing Data Quality Suite [{data_quality_suite_uuid}].')
                for curr_check_idx, curr_check_configuration_data in enumerate(curr_configuration_data['data_quality_checks']):
                    logging.info(f'Object {curr_idx:02d} - Processing Data Quality Check {curr_check_idx:03d}')
                    data_quality_check.process_data_quality_check(configuration, curr_check_configuration_data, data_quality_suite_uuid, curr_data_product)
            else:
                logging.error(f'Object {curr_idx:02d} - Data Quality Suite not found.')


if __name__ == '__main__':
    main()
