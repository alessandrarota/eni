import logging
import requests

from quality.configurations.ConfigurationProperties import BaseConfig


def get_system_uuid(configuration: BaseConfig,
                    system_name: str):
    response = requests.get(
        configuration.BLINDATA_SYSTEM_ENDPOINT,
        headers=configuration.BLINDATA_HEADER,
        params={'search': system_name},
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
