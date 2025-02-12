import logging
import requests

from quality.configurations.ConfigurationProperties import BaseConfig


def get_data_products(configuration: BaseConfig):
    response = requests.get(
        f'{configuration.BLINDATA_DATA_PRODUCTS_ENDPOINT}?size=1000', 
        headers=configuration.BLINDATA_HEADER
    )

    if response.status_code == 200:
        data = response.json()
        data_products =  {
            dp['name'].upper(): {
                'name': dp['name'],
                'displayName': dp['displayName'],
                'uuid': dp['uuid'],
                'identifier': dp['identifier'],
            } for dp in data['content']
        }

        logging.info(f'Succesfully retrieved {len(data_products)} data products.')
        return data_products
    else:
        logging.error(f'Failed to fetch data. Status code: {response.status_code}')
        return {}
