from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from logging.handlers import TimedRotatingFileHandler
from logging import StreamHandler
from src.configurations.ConfigurationProperties import *
from datetime import datetime
import logging
import os
import socket


def create_processor(env='ENV'):
    config_name = os.getenv(env, "development")
    configurations = init_configurations(config_name)
    init_logging(configurations)
    logging.info(f"Environment: {config_name}")
    init_database(configurations)
    return configurations


def init_configurations(env):
    logging.info(f"Loading configurations for {env}")
    if env.__eq__('development'):
        return DevelopmentConfig
    if env.__eq__('testing'):
        return TestingConfig
    if env.__eq__('production'):
        return ProductionConfig


def init_database(configurations):
    logging.info(f"Setting up the database: {configurations.SQLALCHEMY_DATABASE_URI}")
    engine = create_engine(configurations.SQLALCHEMY_DATABASE_URI, isolation_level=configurations.ENGINE_ISOLATION_LEVEL)
    logging.info(f"Setting up the session maker: started")
    configurations.SESSION_MAKER = sessionmaker(engine)
    logging.info(f"Setting up the session maker: done")
    if not database_exists(engine.url):
        create_database(engine.url)
        logging.info(f"New database created {database_exists(engine.url)}")
    else:
        logging.info(f"Database already exists")

        


def init_logging(configurations):
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - "
                                  "level=%(levelname)s - "
                                  "%(name)s - "
                                  "%(threadName)s - "
                                  "%(message)s")

    handler = TimedRotatingFileHandler(configurations.LOG_FILE_PATH, when='midnight')
    handler.setLevel(configurations.FILE_LOG_LEVEL)
    handler.setFormatter(formatter)
    handler.suffix = '%Y%m%d'
    root_logger.addHandler(handler)

    # if not configurations.LOGGER.__eq__('production'):
    console_handler = StreamHandler()
    console_handler.setLevel(configurations.CONSOLE_LOG_LEVEL)
    console_handler.setFormatter(formatter)
    console_handler.suffix = '%Y%m%d'
    root_logger.addHandler(console_handler)
    logging.debug("Logger setup completed")
