import logging
import os

from logging import StreamHandler, FileHandler
from quality.configurations.ConfigurationProperties import *
from quality.blindata import login


def create_processor(env='ENV'):
    config_name = os.getenv(env, 'base')
    config = init_configuration(config_name)

    init_logging(config)

    logging.info(f'Environment: {config_name}')
    
    login.login(config)
    return config


def init_configuration(env):
    logging.info(f'Loading configuration for {env}')
    if env.__eq__('development'):
        return DevelopmentConfig
    elif env.__eq__('testing'):
        return TestingConfig
    elif env.__eq__('production'):
        return ProductionConfig
    else:
        return BaseConfig


def init_logging(configuration):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG)
    # formatter = logging.Formatter("%(asctime)s - "
    #                               "level=%(levelname)s - "
    #                               "%(name)s - "
    #                               "%(threadName)s - "
    #                               "%(message)s")

    formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

    handler = FileHandler(configuration.LOG_FILE_PATH)
    handler.setLevel(configuration.LOG_FILE_LEVEL)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    console_handler = StreamHandler()
    console_handler.setLevel(configuration.LOG_CONSOLE_LEVEL)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    logging.info('Logger formatters and handlers setup completed.')
