import requests
import logging

from quality.configurations.ConfigurationProperties import BaseConfig


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_bearer_token(configuration: BaseConfig):
    response = requests.post(
        configuration.BLINDATA_LOGIN_ENDPOINT,
        json={
            "username": configuration.BLINDATA_CREDENTIAL_USERNAME,
            "password": configuration.BLINDATA_CREDENTIAL_PASSWORD,
        }
    )

    if response.status_code == 200:
        logging.info('Authorization Bearer Token retrieved successfully.')
        return response.json().get("access_token") 
    else:
        raise Exception(f"Login failed. Status code: {response.status_code}, Response: {response.text}")


def set_header_for_requests(configuration: BaseConfig, bearer_token: str):
    configuration.BLINDATA_HEADER = {
        'X-Bd-Tenant': configuration.BLINDATA_TENANT_ID,
        'Authorization': f'Bearer {bearer_token}'
    }


def login(configuration: BaseConfig):
    set_header_for_requests(
        configuration=configuration,
        bearer_token=get_bearer_token(configuration)
    )