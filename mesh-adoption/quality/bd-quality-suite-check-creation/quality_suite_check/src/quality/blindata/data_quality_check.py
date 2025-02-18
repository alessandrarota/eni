import requests
import logging
import pdb
import json

from quality.configurations.ConfigurationProperties import BaseConfig
from quality.blindata import physical_entities
from quality.blindata import physical_fields


def compose_data_quality_check_code(data_product_name: str, 
                                    data_quality_check_name: str):
    return f'{data_product_name}-{data_quality_check_name}'


def compose_data_quality_check_name(data_product_name: str, 
                                    data_quality_check_name: str):
    return f'{data_quality_check_name} ({data_product_name})'


def check_data_quality_check_existence(configuration: BaseConfig,
                                       data_quality_check_code: str):
    response = requests.get(
        configuration.BLINDATA_QUALITY_CHECKS_ENDPOINT,
        headers=configuration.BLINDATA_HEADER,
        params={'code': data_quality_check_code},
    )
    if response.status_code == 200:
        data = response.json()
        if data['totalElements'] == 1:
            return data['content'][0]['uuid']
        else:
            return None
    else:
        logging.error(f"Failed to fetch data. Status code: {response.status_code}")
        return None


def send_data_quality_check(configuration: BaseConfig,
                            data_quality_suite_uuid: str,
                            data_quality_check_uuid: str,
                            data_quality_check_code: str,
                            data_quality_check_name: str,
                            data_quality_check_description: str,
                            data_quality_check_warning_threshold: float,
                            data_quality_check_success_threshold: float,
                            data_quality_check_score_strategy: str,
                            data_quality_check_dimension: str,
                            data_quality_check_physical_fields: list,
                            data_quality_check_physical_entities: list):
    request_body = {
        'code': data_quality_check_code,
        'name': data_quality_check_name,
        'description': data_quality_check_description,
        'enabled': True,
        'warningThreshold': data_quality_check_warning_threshold,
        'successThreshold': data_quality_check_success_threshold,
        'scoreStrategy': data_quality_check_score_strategy,
        'qualitySuite': {
            'uuid': data_quality_suite_uuid
        },
        'additionalProperties': [{
            'name': configuration.DATA_QUALITY_CHECK_DIMENSION_NAME,
            'value': data_quality_check_dimension
        }]
    }

    if len(data_quality_check_physical_fields) > 0:
        request_body['physicalFields'] = [{'uuid': uuid} for uuid in data_quality_check_physical_fields]
    if len(data_quality_check_physical_entities) > 0:
        request_body['physicalEntities'] = [{'uuid': uuid} for uuid in data_quality_check_physical_entities]
    
    if data_quality_check_uuid is not None:
        request_body['uuid'] = data_quality_check_uuid
        response = requests.put(
            f'{configuration.BLINDATA_QUALITY_CHECKS_ENDPOINT}/{data_quality_check_uuid}',
            headers=configuration.BLINDATA_HEADER | {"Content-Type": "application/json"},
            json=request_body
        )
        success_status_code = 200
    else:
        response = requests.post(
            configuration.BLINDATA_QUALITY_CHECKS_ENDPOINT,
            headers=configuration.BLINDATA_HEADER | {"Content-Type": "application/json"},
            json=request_body
        )
        success_status_code = 201
    
    return response.status_code == success_status_code, response.status_code, response.text


def process_data_quality_check(configuration: BaseConfig,
                               data_quality_check_configuration: dict,
                               data_quality_suite_uuid: str,
                               data_product: dict):    
    data_quality_check_code = compose_data_quality_check_code(
        data_product_name=data_product['name'], 
        data_quality_check_name=data_quality_check_configuration['data_quality_check_name']
    )
    data_quality_check_name = compose_data_quality_check_name(
        data_product_name=data_product['name'], 
        data_quality_check_name=data_quality_check_configuration['data_quality_check_name']
    )

    data_quality_check_uuid = check_data_quality_check_existence(
        configuration=configuration,
        data_quality_check_code=data_quality_check_code
    )

    physical_fields_uuids, physical_entities_uuids = [], []
    for physical_association_configuration in data_quality_check_configuration['physical_associations']:
        if 'physical_field_name' in physical_association_configuration: 
            physical_field_uuid = physical_fields.get_physical_field_uuid(
                configuration=configuration,
                system_name=physical_association_configuration['system'],
                physical_entity_schema=physical_association_configuration['physical_entity_schema'],
                physical_entity_name=physical_association_configuration['physical_entity_name'],
                physical_field_name=physical_association_configuration['physical_field_name']
            )
            if physical_field_uuid is not None: 
                physical_fields_uuids.append(physical_field_uuid)
            else:
                logging.error(f"Physical Field {physical_association_configuration['physical_entity_schema']}.{physical_association_configuration['physical_entity_name']}.{physical_association_configuration['physical_field_name']} not found.")
        else: 
            physical_entity_uuid = physical_entities.get_physical_entity_uuid(
                configuration=configuration,
                system_name=physical_association_configuration['system'],
                physical_entity_schema=physical_association_configuration['physical_entity_schema'],
                physical_entity_name=physical_association_configuration['physical_entity_name']
            )
            if physical_entity_uuid is not None: 
                physical_entities_uuids.append(physical_entity_uuid)
            else:
                logging.error(f"Physical Entity {physical_association_configuration['physical_entity_schema']}.{physical_association_configuration['physical_entity_name']} not found.")

    return send_data_quality_check(
        configuration=configuration,
        data_quality_suite_uuid=data_quality_suite_uuid,
        data_quality_check_uuid=data_quality_check_uuid,
        data_quality_check_code=data_quality_check_code,
        data_quality_check_name=data_quality_check_name,
        data_quality_check_description=data_quality_check_configuration['data_quality_check_description'],
        data_quality_check_warning_threshold=data_quality_check_configuration['score_warning_threshold'],
        data_quality_check_success_threshold=data_quality_check_configuration['score_success_threshold'],
        data_quality_check_score_strategy=data_quality_check_configuration['score_strategy'],
        data_quality_check_dimension=data_quality_check_configuration['data_quality_dimension'],
        data_quality_check_physical_fields=physical_fields_uuids,
        data_quality_check_physical_entities=physical_entities_uuids
    )
