import os

class BaseConfig(object):
    ENGINE_ISOLATION_LEVEL = "READ UNCOMMITTED"
    DEBUG = True
    LOGGER = 'sd'
    LOG_FILE_PATH = './src/forwarder/log/forwarder.log'
    CONSOLE_LOG_LEVEL = "DEBUG"
    FILE_LOG_LEVEL = "DEBUG"
    FLOW_NAME = 'blindata-forwarder-pytest'
    ENVIRONMENT = 'sd'

    DATABASE_URL = f"sqlite:///:memory:"

    BLINDATA_LOGIN_ENDPOINT = "https://app.blindata.io/auth/login"
    BLINDATA_FORWARDER_USERNAME = os.getenv('BLINDATA_FORWARDER_USERNAME')
    BLINDATA_FORWARDER_PASSWORD = os.getenv('BLINDATA_FORWARDER_PASSWORD')
    BLINDATA_POST_QUALITY_RESULTS_CSV_ENDPOINT = "https://app.blindata.io/api/v1/upload/quality-results"
    BLINDATA_POST_QUALITY_RESULT_ENDPOINT = "https://app.blindata.io/api/v1/data-quality/results"
    BLINDATA_QUALITY_CHECK_ENDPOINT = "https://app.blindata.io/api/v1/data-quality/checks"
    BLINDATA_GET_QUALITY_SUITE_ENDPOINT = "https://app.blindata.io/api/v1/data-quality/suites"
    BLINDATA_TENANT_ID = os.getenv('BLINDATA_TENANT_ID') 

class DevelopmentConfig(BaseConfig):
    DEBUG = True
    LOGGER = 'sd'
    LOG_FILE_PATH = './src/forwarder/log/forwarder.log'
    CONSOLE_LOG_LEVEL = "DEBUG"
    FILE_LOG_LEVEL = "DEBUG"
    ENVIRONMENT = 'sd'

    DATABASE_URL = os.getenv('DATABASE_URL')

    BLINDATA_LOGIN_ENDPOINT = "https://app.blindata.io/auth/login"
    BLINDATA_FORWARDER_USERNAME = os.getenv('BLINDATA_FORWARDER_USERNAME')
    BLINDATA_FORWARDER_PASSWORD = os.getenv('BLINDATA_FORWARDER_PASSWORD')
    BLINDATA_POST_QUALITY_RESULTS_CSV_ENDPOINT = "https://app.blindata.io/api/v1/upload/quality-results"
    BLINDATA_POST_QUALITY_RESULT_ENDPOINT = "https://app.blindata.io/api/v1/data-quality/results"
    BLINDATA_QUALITY_CHECK_ENDPOINT = "https://app.blindata.io/api/v1/data-quality/checks"
    BLINDATA_GET_QUALITY_SUITE_ENDPOINT = "https://app.blindata.io/api/v1/data-quality/suites"
    BLINDATA_TENANT_ID = os.getenv('BLINDATA_TENANT_ID') 

class TestingConfig(BaseConfig):
    DEBUG = True
    ENVIRONMENT = 'st'

class ProductionConfig(BaseConfig):
    DEBUG = True
    ENVIRONMENT = 'pr'