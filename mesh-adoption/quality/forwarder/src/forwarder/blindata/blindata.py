import requests
import logging
import csv
import traceback
import re
from datetime import datetime
import threading
import time

logging.basicConfig(level=logging.INFO)

# Variabili globali per memorizzare il token e la sua scadenza
token_lock = threading.Lock()
cached_token = None
token_expiry = 0  # Timestamp di scadenza del token

def refresh_token(config):
    """Funzione per ottenere e aggiornare il token ogni 30 minuti."""
    global cached_token, token_expiry

    while True:
        try:
            login_response = requests.post(
                config.BLINDATA_LOGIN_ENDPOINT,
                json={
                    "username": config.BLINDATA_FORWARDER_USERNAME,
                    "password": config.BLINDATA_FORWARDER_PASSWORD
                },
                headers={"Content-Type": "application/json"}
            )

            if login_response.status_code == 200:
                new_token = login_response.json().get("access_token")
                if not new_token:
                    raise ValueError("Access token not found in the response")

                with token_lock:
                    cached_token = f"Bearer {new_token}"
                    token_expiry = time.time() + 1800  # 30 minuti in secondi

                logging.info("Successfully refreshed access token.")
            else:
                logging.error(f"Login failed with status code {login_response.status_code}: {login_response.text}")

        except Exception as e:
            logging.error(f"Error during token refresh: {e}")
            logging.error(traceback.format_exc())

        # Attendi 30 minuti prima di aggiornare il token
        time.sleep(1800)

def get_blindata_token():
    """Restituisce il token memorizzato se non scaduto, altrimenti lancia un'eccezione."""
    global cached_token, token_expiry

    with token_lock:
        # Verifica se il token è valido
        if cached_token and time.time() < token_expiry:
            return cached_token

    raise Exception("Token not available or expired")

def start_blindata_token_refresh_thread(config):
    token_thread = threading.Thread(target=refresh_token, args=(config,), daemon=True)
    token_thread.start()

def fill_csv_file(current_metrics):
    file_name = "quality_results.csv"
    csv_header = [
        "qualityResult.qualityCheckCode",
        "qualityResult.metric",
        "qualityResult.totalElements",
        "qualityResult.startedAt"
    ]

    try:
        with open(file_name, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(csv_header)

            for current_metric in current_metrics:
                csv_row = [
                    f"{current_metric.expectation_name}_{current_metric.data_source_name}-{current_metric.data_asset_name}-{current_metric.column_name}",
                    current_metric.errors_nbr,
                    current_metric.checked_elements_nbr,
                    datetime.strptime(current_metric.otlp_sending_datetime[:current_metric.otlp_sending_datetime.index('.') + 7], "%Y-%m-%d %H:%M:%S.%f")
                        .strftime("%Y-%m-%dT%H:%M:%S.") +
                    str(datetime.strptime(current_metric.otlp_sending_datetime[:current_metric.otlp_sending_datetime.index('.') + 7], "%Y-%m-%d %H:%M:%S.%f").microsecond // 1000).zfill(3) + 'Z'
                ]
                writer.writerow(csv_row)

        logging.info(f"CSV file '{file_name}' created successfully!")
        return file_name
    except Exception as e:
        logging.error(f"Error while creating the CSV file: {e}")
        logging.error(traceback.format_exc())
        raise

def post_quality_results_on_blindata_with_csv(config, current_metrics):
    try:
        bearer_token = get_blindata_token()
        file_name = fill_csv_file(current_metrics)

        with open(file_name, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                logging.info(f"CSV Row: {row}")

        with open(file_name, "rb") as file:
            files = {"file": (file_name, file, "text/csv")}
            response = requests.post(
                config.BLINDATA_QUALITY_CHECK_ENDPOINT,
                headers={"Authorization": bearer_token, "Accept": "application/json"},
                files=files
            )

        if response.status_code == 200 and response.json().get('errors', []) == []:
            logging.info("File uploaded successfully!")
            logging.info(f"API Response: {response.json()}")
            return response
        else:
            logging.error(f"Error during file upload: {response.status_code} - {response.text}")
            raise Exception(f"File upload failed: {response.status_code}")

    except Exception as e:
        logging.error(f"Error during file upload process: {e}")
        logging.error(traceback.format_exc())
        raise

def post_single_quality_result_on_blindata(config, quality_check, current_metric):
    try:
        bearer_token = get_blindata_token()

        result = {
            "qualityCheck": quality_check,
            "metric": current_metric.errors_nbr,
            "totalElements": current_metric.checked_elements_nbr,
            "startedAt": (current_metric.otlp_sending_datetime.strftime("%Y-%m-%dT%H:%M:%S.") + str(current_metric.otlp_sending_datetime.microsecond // 1000).zfill(3) + 'Z')
        }

        response = requests.post(
            config.BLINDATA_POST_QUALITY_RESULT_ENDPOINT,
            json=result,
            headers={"Authorization": bearer_token, "Content-Type": "application/json"}
        )

        if response.status_code == 201:
            logging.info(f"Result for quality check {quality_check['code']} updated successfully!")
        else:
            logging.error(f"Error during results upload: {response.status_code} - {response.text}")
        
        return response.status_code

    except Exception as e:
        logging.error(f"Error during API request to create quality check: {e}")
        logging.error(traceback.format_exc())
        raise

def get_quality_check(config, current_metric):
    try:
        bearer_token = get_blindata_token()
        quality_check_code = f"{current_metric.check_name}"

        check_response = requests.get(
            config.BLINDATA_QUALITY_CHECK_ENDPOINT,
            params={"code": quality_check_code},
            headers={"Authorization": bearer_token, "Content-Type": "application/json"}
        )

        if check_response.status_code == 200 and check_response.json().get("totalElements") == 1:
            logging.info(f"Quality check found for {quality_check_code}.")
            return check_response.json()['content'][0]
        else:
            logging.info(f"No quality check found for {quality_check_code}.")
            return False

    except Exception as e:
        logging.error(f"Error during API request for quality check: {e}")
        logging.error(traceback.format_exc())
        raise

def get_quality_suite(config, current_metric):
    try:
        bearer_token = get_blindata_token()

        suite_response = requests.get(
            config.BLINDATA_GET_QUALITY_SUITE_ENDPOINT,
            params={"password": config.BLINDATA_FORWARDER_PASSWORD},
            headers={"Authorization": bearer_token, "Content-Type": "application/json"}
        )

        if suite_response.status_code == 200:
            content = suite_response.json().get("content")
            for item in content:
                if item.get("code") == current_metric.blindata_suite_name:
                    logging.info(f"Found quality suite: {current_metric.blindata_suite_name}")
                    return item
            logging.info(f"No quality suite found for {current_metric.blindata_suite_name}.")
            return None
        else:
            logging.error(f"Failed to retrieve quality suite: {suite_response.status_code} - {suite_response.text}")
            return None

    except Exception as e:
        logging.error(f"Error during API request to get quality suite: {e}")
        logging.error(traceback.format_exc())
        raise
