import requests
import logging
import csv
import traceback

logging.basicConfig(level=logging.INFO)

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
                    current_metric.metric_name + "-" + current_metric.expectation_name,
                    current_metric.unexpected_count,
                    current_metric.element_count,
                    current_metric.timestamp
                ]           
                writer.writerow(csv_row)
        logging.info(f"CSV file created successfully!")
        return file_name
    except Exception as e:
        logging.error(f"Error while creating the CSV file: {e}")
        raise


def get_blindata_token(config):
    login_response = requests.post(
        config.BLINDATA_LOGIN_ENDPOINT, 
        json={
            "username": config.BLINDATA_FORWARDER_USERNAME,
            "password": config.BLINDATA_FORWARDER_PASSWORD
        }, 
        headers={
            "Content-Type": "application/json"
        }
    )

    if login_response.status_code == 200:
        token = login_response.json().get("access_token")
        if not token:
            raise ValueError("Access token not found in the response")
    else:
        raise Exception(f"Login failed with status code {login_response.status_code}: {login_response.text}")

    bearer_token = f"Bearer {token}"
    print(bearer_token)

    return bearer_token

def post_quality_results(config, current_metrics):
    bearer_token = get_blindata_token(config)
    file_name = fill_csv_file(current_metrics)

    with open(file_name, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            logging.info(row)  

    try:
        with open(file_name, "rb") as file:
            files = {
                "file": (file_name, file, "text/csv"),
            }

            response = requests.post(
                config.BLINDATA_QUALITY_CHECK_ENDPOINT, 
                headers={
                    "Authorization": bearer_token,
                    "Accept": "application/json"
                },
                files=files
            )
        
        
        if response.status_code == 200 and response.json()['errors'] == []:
            logging.info("File uploaded successfully!")
            logging.info(f"API Response: {response.json()}")
            return response
        else:
            logging.error(f"Error during file upload: {response.status_code} - {response.text}")
            logging.error(f"Full traceback: {traceback.format_exc()}")
    except Exception as e:
        logging.error(f"Error during API request: {e}")
        logging.error(f"Test failed: {e}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
        raise