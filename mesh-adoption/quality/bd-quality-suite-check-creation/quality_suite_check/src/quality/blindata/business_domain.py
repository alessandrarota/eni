import logging
import requests

from quality.configurations.ConfigurationProperties import BaseConfig


def get_business_domains(configuration: BaseConfig):
    response = requests.get(
        f'{configuration.BLINDATA_BUSINESS_DOMAINS_ENDPOINT}?size=100', 
        headers=configuration.BLINDATA_HEADER
    )

    if response.status_code == 200:
        data = response.json()
        business_domains =  {
            bd['name'].upper(): {
                'name': bd['name'],
                'displayName': bd['displayName'],
                'uuid': bd['uuid'],
                'identifier': bd['identifier'],
            } for bd in data['content']
        }

        logging.info(f'Succesfully retrieved {len(business_domains)} business domains.')
        return business_domains
    else:
        logging.error(f'Failed to fetch data. Status code: {response.status_code}')
        return {}
