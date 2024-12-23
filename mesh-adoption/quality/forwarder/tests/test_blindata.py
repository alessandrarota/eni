import pytest
from datetime import datetime
from src import init_configurations
from src.blindata.blindata import get_blindata_token, post_quality_results
import logging
import re

logging.basicConfig(level=logging.INFO)

def test_login_blindata():
    configurations = init_configurations("development")
    bearer_token = get_blindata_token(configurations)

    assert bearer_token != None

def test_post_empty_quality_results():
    configurations = init_configurations("development")
    response = post_quality_results(configurations)

    logging.info(response)
    assert response.status_code != 200

    