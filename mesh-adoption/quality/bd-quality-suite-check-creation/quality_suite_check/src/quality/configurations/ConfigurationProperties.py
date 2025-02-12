import os

from datetime import datetime


class BaseConfig(object):
    DEBUG = True
    LOGGER = 'sd'
    LOG_CONSOLE_LEVEL = 'INFO' # 'DEBUG'
    LOG_FILE_LEVEL = 'INFO' # 'DEBUG'
    LOG_FILE_PATH = f'./logs/{datetime.today().strftime("%Y%m%d%H%M%S")}.log'
    FLOW_NAME = 'blindata-quality-pytest'

    BLINDATA_BASE_URL = os.getenv('BLINDATA_BASE_URL', 'https://app.blindata.io')
    BLINDATA_CREDENTIAL_USERNAME = os.getenv('BLINDATA_CREDENTIAL_USERNAME')
    BLINDATA_CREDENTIAL_PASSWORD = os.getenv('BLINDATA_CREDENTIAL_PASSWORD')
    BLINDATA_TENANT_ID = os.getenv('BLINDATA_TENANT_ID')
    
    BLINDATA_LOGIN_ENDPOINT = f'{BLINDATA_BASE_URL}/auth/login'
    BLINDATA_DATA_PRODUCTS_ENDPOINT = f'{BLINDATA_BASE_URL}/api/v1/dataproducts'
    BLINDATA_BUSINESS_DOMAINS_ENDPOINT = f'{BLINDATA_BASE_URL}/api/v1/dataproductsdomains'
    BLINDATA_QUALITY_CHECKS_ENDPOINT = f'{BLINDATA_BASE_URL}/api/v1/data-quality/checks'
    BLINDATA_QUALITY_SUITES_ENDPOINT = f'{BLINDATA_BASE_URL}/api/v1/data-quality/suites'
    BLINDATA_SYSTEM_ENDPOINT = f'{BLINDATA_BASE_URL}/api/v1/systems'
    BLINDATA_PHYSICAL_ENTITIES_ENDPOINT = f'{BLINDATA_BASE_URL}/api/v1/physical_entities'
    BLINDATA_PHYSICAL_FIELDS_ENDPOINT = f'{BLINDATA_BASE_URL}/api/v1/physical_fields'
    
    CONFIGURATION_SCHEMA_FILE_PATH = os.getenv('QUALITY_CONFIGURATION_SCHEMA_FILE_PATH', './quality_suite_check/resources/quality-config-schema_v01.json')
    CONFIGURATION_FILE_PATH = os.getenv('QUALITY_CONFIGURATION_FILE_PATH', './quality_suite_check/resources/quality-config.xlsx')

    BLINDATA_HEADER = {}

    DATA_QUALITY_CHECK_DIMENSION_NAME = os.getenv('DATA_QUALITY_CHECK_DIMENSION_NAME', 'Data Quality Dimension')


class DevelopmentConfig(BaseConfig):
    DEBUG = True
    LOGGER = 'sd'
    LOG_CONSOLE_LEVEL = 'INFO'
    LOG_FILE_LEVEL = 'INFO'
    LOG_FILE_PATH = f'./logs/{datetime.today().strftime("%Y%m%d%H%M%S")}.log'
    FLOW_NAME = 'blindata-quality-sd'


class TestingConfig(BaseConfig):
    DEBUG = True
    LOGGER = 'st'
    LOG_CONSOLE_LEVEL = 'INFO'
    LOG_FILE_LEVEL = 'INFO'
    LOG_FILE_PATH = f'./logs/{datetime.today().strftime("%Y%m%d%H%M%S")}.log'
    FLOW_NAME = 'blindata-quality-st'


class ProductionConfig(BaseConfig):
    DEBUG = False
    LOGGER = 'pr'
    LOG_CONSOLE_LEVEL = 'INFO'
    LOG_FILE_LEVEL = 'INFO'
    LOG_FILE_PATH = f'./logs/{datetime.today().strftime("%Y%m%d%H%M%S")}.log'
    FLOW_NAME = 'blindata-quality-pr'
