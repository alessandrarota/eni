import requests
import logging

from quality.configurations.ConfigurationProperties import BaseConfig


def compose_data_quality_suite_code(business_domain_name: str, 
                                    data_product_name: str):
    return f'{business_domain_name}-{data_product_name}'


def compose_data_quality_suite_name(business_domain_display_name: str, 
                                    data_product_display_name: str):
    return f'{business_domain_display_name}: {data_product_display_name}'


def check_data_quality_suite_existence(configuration: BaseConfig,
                                       data_quality_suite_code: str):
    response = requests.get(
        f'{configuration.BLINDATA_QUALITY_SUITES_ENDPOINT}?size=1000',
        headers=configuration.BLINDATA_HEADER
    )
    if response.status_code == 200:
        data = response.json()
        uuids = [suite['uuid'] for suite in data['content'] if suite['code'] == data_quality_suite_code]
        if len(uuids) == 0:
            return None
        elif len(uuids) == 1:
            return uuids[0]
        else:
            logging.error(f"Expected [0, 1] Data Quality Suites, found {len(uuids)}.")
            return None
    else:
        logging.error(f"Failed to fetch data. Status code: {response.status_code}")
        return None


def create_data_quality_suite(configuration: BaseConfig,
                              data_quality_suite_code: str,
                              data_quality_suite_name: str):
    response = requests.post(
        configuration.BLINDATA_QUALITY_SUITES_ENDPOINT, 
        headers=configuration.BLINDATA_HEADER,
        json={
            'code': data_quality_suite_code,
            'name': data_quality_suite_name,
            'published': True,
        }
    )
    if response.status_code == 201:
        logging.info(f"Data Quality Suite [{data_quality_suite_code}] correctly created.")
        return response.json()['uuid']
    else:
        logging.error(f"Failed to create Data Quality Suite. Status code: {response.status_code}")
        return None


def get_data_quality_suite_uuid(configuration: BaseConfig,
                                business_domain: dict,
                                data_product: dict):
    data_quality_suite_code = compose_data_quality_suite_code(
        business_domain_name=business_domain['name'],
        data_product_name=data_product['name']
    )
    data_quality_suite_name = compose_data_quality_suite_name(
        business_domain_display_name=business_domain['displayName'],
        data_product_display_name=data_product['displayName']
    )

    data_quality_suite_uuid = check_data_quality_suite_existence(
        configuration=configuration,
        data_quality_suite_code=data_quality_suite_code
    )
    
    if data_quality_suite_uuid is None:
        return create_data_quality_suite(
            configuration=configuration,
            data_quality_suite_code=data_quality_suite_code,
            data_quality_suite_name=data_quality_suite_name
        )
    else:
        return data_quality_suite_uuid