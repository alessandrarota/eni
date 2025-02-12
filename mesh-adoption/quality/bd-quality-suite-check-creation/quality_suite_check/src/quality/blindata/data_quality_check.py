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


def get_data_quality_check(configuration: BaseConfig,
                           data_quality_check_uuid: str):
    response = requests.get(
        f'{configuration.BLINDATA_QUALITY_CHECKS_ENDPOINT}/{data_quality_check_uuid}',
        headers=configuration.BLINDATA_HEADER,
    )
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to fetch data. Status code: {response.status_code}")
        return None


def create_new_data_quality_check(configuration: BaseConfig,
                                  data_quality_suite_uuid: str,
                                  data_quality_check_code: str,
                                  data_quality_check_name: str,
                                  data_quality_check_warning_threshold: float,
                                  data_quality_check_success_threshold: float,
                                  data_quality_check_score_strategy: str,
                                  data_quality_check_dimension: str,
                                  data_quality_check_physical_fields: list,
                                  data_quality_check_physical_entities: list):
    request_body = {
        'code': data_quality_check_code,
        'name': data_quality_check_name,
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
    
    response = requests.post(
        configuration.BLINDATA_QUALITY_CHECKS_ENDPOINT,
        headers=configuration.BLINDATA_HEADER | {"Content-Type": "application/json"},
        json=request_body
    )
    if response.status_code == 201:
        logging.info(f"Data Quality Check succesfully created.")
        return True
    else:
        logging.error(f"Failed to create data quality check. Status code: {response.status_code} - {response.text}.")
        return False


def update_existing_data_quality_check(configuration: BaseConfig,
                                       data_quality_check: object,
                                       data_quality_suite_uuid: str,
                                       data_quality_check_uuid: str,
                                       data_quality_check_code: str,
                                       data_quality_check_name: str,
                                       data_quality_check_warning_threshold: float,
                                       data_quality_check_success_threshold: float,
                                       data_quality_check_score_strategy: str,
                                       data_quality_check_dimension: str,
                                       data_quality_check_physical_fields: list,
                                       data_quality_check_physical_entities: list):
    to_be_updated = False
    request_body = {
        'uuid': data_quality_check_uuid,
        'code': data_quality_check_code,
        'name': data_quality_check_name,
        'enabled': True,
        'description': data_quality_check['description'] if 'description' in data_quality_check else '',
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
    
    if data_quality_check['code'] != data_quality_check_code or \
        data_quality_check['name'] != data_quality_check_name or \
        data_quality_check['warningThreshold'] != data_quality_check_warning_threshold or \
        data_quality_check['successThreshold'] != data_quality_check_success_threshold or \
        data_quality_check['qualitySuite']['uuid'] != data_quality_suite_uuid:
        to_be_updated = True
    
    if 'scoreStrategy' not in data_quality_check or data_quality_check['scoreStrategy'] != data_quality_check_score_strategy:
        request_body['scoreStrategy'] = data_quality_check_score_strategy
        to_be_updated = True
    
    if 'additionalProperties' not in data_quality_check or not any([(pr['name'] == configuration.DATA_QUALITY_CHECK_DIMENSION_NAME and pr['value'] == data_quality_check_dimension) for pr in data_quality_check['additionalProperties']]):
        if 'additionalProperties' in data_quality_check:
            request_body['additionalProperties'] = data_quality_check['additionalProperties']
        
        if any([pr['name'] == configuration.DATA_QUALITY_CHECK_DIMENSION_NAME for pr in request_body['additionalProperties']]):
            request_body['additionalProperties'] = [
                pr for pr in request_body['additionalProperties'] if pr['name'] != configuration.DATA_QUALITY_CHECK_DIMENSION_NAME
            ] + [{
                'name': configuration.DATA_QUALITY_CHECK_DIMENSION_NAME,
                'value': data_quality_check_dimension
            }]
        else:
            request_body['additionalProperties'].append({
                'name': configuration.DATA_QUALITY_CHECK_DIMENSION_NAME,
                'value': data_quality_check_dimension
            })
        to_be_updated = True
    
    if len(data_quality_check_physical_fields) > 0:
        for physical_field in data_quality_check_physical_fields:
            if not any([pe['uuid'] == physical_field for pe in data_quality_check['physicalFields']]):
                request_body['physicalFields'] = [{'uuid': uuid} for uuid in data_quality_check_physical_fields]
                to_be_updated = True
    
    if len(data_quality_check_physical_entities) > 0:
        for physical_entity in data_quality_check_physical_entities:
            if not any([pe['uuid'] == physical_entity for pe in data_quality_check['physicalEntities']]):
                request_body['physicalEntities'] = [{'uuid': uuid} for uuid in data_quality_check_physical_entities]
                to_be_updated = True
    
    if to_be_updated:
        response = requests.put(
            f'{configuration.BLINDATA_QUALITY_CHECKS_ENDPOINT}/{data_quality_check["uuid"]}',
            headers=configuration.BLINDATA_HEADER | {"Content-Type": "application/json"},
            json=request_body
        )
        if response.status_code == 200:
            logging.info(f"Data Quality Check succesfully updated.")
            return True
        else:
            logging.error(f"Failed to update. Status code: {response.status_code} - {response.text}.")
            return False
    else:
        logging.info(f"Data Quality Check already updated; does not need to send request.")
        return False


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

    if data_quality_check_uuid is None:
        return create_new_data_quality_check(
            configuration=configuration,
            data_quality_suite_uuid=data_quality_suite_uuid,
            data_quality_check_code=data_quality_check_code,
            data_quality_check_name=data_quality_check_name,
            data_quality_check_warning_threshold=data_quality_check_configuration['score_warning_threshold'],
            data_quality_check_success_threshold=data_quality_check_configuration['score_success_threshold'],
            data_quality_check_score_strategy=data_quality_check_configuration['score_strategy'],
            data_quality_check_dimension=data_quality_check_configuration['data_quality_dimension'],
            data_quality_check_physical_fields=physical_fields_uuids,
            data_quality_check_physical_entities=physical_entities_uuids
        )
    else: 
        data_quality_check = get_data_quality_check(
            configuration=configuration,
            data_quality_check_uuid=data_quality_check_uuid
        )
        
        return update_existing_data_quality_check(
            configuration=configuration,
            data_quality_check=data_quality_check,
            data_quality_suite_uuid=data_quality_suite_uuid,
            data_quality_check_uuid=data_quality_check_uuid,
            data_quality_check_code=data_quality_check_code,
            data_quality_check_name=data_quality_check_name,
            data_quality_check_warning_threshold=data_quality_check_configuration['score_warning_threshold'],
            data_quality_check_success_threshold=data_quality_check_configuration['score_success_threshold'],
            data_quality_check_score_strategy=data_quality_check_configuration['score_strategy'],
            data_quality_check_dimension=data_quality_check_configuration['data_quality_dimension'],
            data_quality_check_physical_fields=physical_fields_uuids,
            data_quality_check_physical_entities=physical_entities_uuids
        )
