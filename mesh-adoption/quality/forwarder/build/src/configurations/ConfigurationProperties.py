class BaseConfig(object):
    DEBUG = False
    ENGINE_ISOLATION_LEVEL = "READ UNCOMMITTED"

class DevelopmentConfig(BaseConfig):
    DEBUG = True
    LOGGER = 'development'
    LOG_FILE_PATH = './build/src/log/forwarder.log'
    CONSOLE_LOG_LEVEL = "DEBUG"
    FILE_LOG_LEVEL = "DEBUG"
    SQLALCHEMY_DATABASE_URI = f"sqlite:///:memory:"
    FLOW_NAME = 'forwarder-development'
    BLINDATA_LOGIN_ENDPOINT = "https://app.blindata.io/auth/login"
    BLINDATA_FORWARDER_USERNAME = "quality-forwarder-user-test@blindata.eni"
    BLINDATA_FORWARDER_PASSWORD = "0?VErp{-}}NiaFC@lNr2"
    BLINDATA_QUALITY_CHECK_ENDPOINT = "https://app.blindata.io/api/v1/upload/quality-results"
    BLINDATA_TENANT_ID = "63f43791-5d98-4943-8423-a03f45e6328c"
    BLINDATA_ACTIVATE=False

class TestingConfig(BaseConfig):
    DEBUG = True
    LOGGER = 'testing'
    LOG_FILE_PATH = './build/src/log/forwarder.log'
    CONSOLE_LOG_LEVEL = "DEBUG"
    FILE_LOG_LEVEL = "DEBUG"
    SQLALCHEMY_DATABASE_URI = f"mssql+pymssql://sa:yourStrongPassword123@host.docker.internal:1433/quality"
    FLOW_NAME = 'forwarder-testing'
    BLINDATA_LOGIN_ENDPOINT = "https://app.blindata.io/auth/login"
    BLINDATA_FORWARDER_USERNAME = "quality-forwarder-user-test@blindata.eni"
    BLINDATA_FORWARDER_PASSWORD = "0?VErp{-}}NiaFC@lNr2"
    BLINDATA_QUALITY_CHECK_ENDPOINT = "https://app.blindata.io/api/v1/upload/quality-results"
    BLINDATA_TENANT_ID = "63f43791-5d98-4943-8423-a03f45e6328c"
    BLINDATA_ACTIVATE=True


class ProductionConfig(BaseConfig):
    DEBUG = True
    LOGGER = 'production'
    SQLALCHEMY_DATABASE_URI = f"mssql+pymssql://sa:yourStrongPassword123@host.docker.internal:1433/quality"