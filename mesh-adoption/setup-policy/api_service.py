from constants import *
import requests
import logging
from typing import Dict, Any
import json

logging.basicConfig(level=logging.INFO)

def handle_response(response: requests.Response) -> requests.Response:
    #assert response.status_code == 201, f"Error: {response.status_code} - {response.text}"
    return response.json()

def build_url(base_url: str, api_url: str, context_path: str, resource: str = "") -> str:
    return f"{base_url}{api_url}{context_path}/{resource}"

def load_json_as_string(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as json_file:
        json_string = json_file.read()
    json_data = json.loads(json_string)
    return json_data

def create_policy_engine(file_path: str) -> requests.Response:
    response = requests.post(
        build_url(ENDPOINT, POLICY_BASE_API_URL, POLICY_CONTEXT_PATH, "policy-engines"),
        json = load_json_as_string(file_path)
    )
    return handle_response(response)

def create_policy(file_path: str) -> requests.Response:
    response = requests.post(
        build_url(ENDPOINT, POLICY_BASE_API_URL, POLICY_CONTEXT_PATH, "policies"),
        json = load_json_as_string(file_path)
    )
    return handle_response(response)