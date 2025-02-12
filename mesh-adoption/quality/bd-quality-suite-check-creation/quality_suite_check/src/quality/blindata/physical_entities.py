import logging
import requests

from quality.configurations.ConfigurationProperties import BaseConfig
from quality.blindata import system


def get_physical_entity_uuid(configuration: BaseConfig,
                             system_name: str,
                             physical_entity_schema: str,
                             physical_entity_name: str):
    system_uuid = system.get_system_uuid(configuration, system_name)
    if system_uuid is None:
        logging.error(f'System {system_name} not found.')
        return None
    else:
        response = requests.get(
            configuration.BLINDATA_PHYSICAL_ENTITIES_ENDPOINT,
            headers=configuration.BLINDATA_HEADER,
            params={
                'systemUuid': system_uuid,
                'search': f'{physical_entity_schema}.{physical_entity_name}',
            }
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
