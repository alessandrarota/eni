import requests 
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

HEADER = {
    'X-Bd-Tenant': "63f43791-5d98-4943-8423-a03f45e6328c",
    "Content-Type": "application/json"
}
TENANT = "63f43791-5d98-4943-8423-a03f45e6328c"

def get_token():
    try:
        login_response = requests.post(
            'https://app.blindata.io/auth/login',
            json={
                "username": 'quality-forwarder-user-test@blindata.eni',
                "password": '0?VErp{-}}NiaFC@lNr2'
            },
            headers={"Content-Type": "application/json"}
        )

        if login_response.status_code == 200:
            logging.info("Token created!")
            return f"Bearer {login_response.json().get('access_token')}"

    except Exception as e:
        logging.error(f"Error during token refresh: {e}")

def create_minimum_quality_check(
                            data_quality_check_code: str,
                            data_quality_check_name: str,
                            data_quality_check_warning_threshold: float,
                            data_quality_check_success_threshold: float,
                            data_quality_check_expected_value: int):
    
    try:
        request_body = {
            'code': data_quality_check_code,
            'name': data_quality_check_name,
            'enabled': True,
            'warningThreshold': data_quality_check_warning_threshold,
            'successThreshold': data_quality_check_success_threshold,
            'expected_value': data_quality_check_expected_value,
            'scoreStrategy': "MINIMUM",
            'qualitySuite': {
                'uuid': "b73d096c-5a3c-4908-b799-7b8f5d3bfa0f"  #testSuiteForGXCustomExpectations
            }
        }
        response = requests.post(
            'https://app.blindata.io/api/v1/data-quality/checks',
            headers=HEADER | {"Authorization": get_token()},
            json=request_body
        )
        
        logging.info(f"Response_code: {response.status_code}")
        logging.info(f"Response_text: {response.text}")
        return response.status_code == 201, response.status_code, response.text
    except Exception as e:
        logging.error(f"Error during creating check: {e}")

def get_quality_check(quality_check_name):
    try:

        check_response = requests.get(
            'https://app.blindata.io/api/v1/data-quality/checks',
            params={"code": quality_check_name},
            headers=HEADER | {"Authorization": get_token()},
        )

        if check_response.status_code == 200 and check_response.json().get("totalElements") == 1:
            logging.info(f"Quality check found for {quality_check_name}.")
            return check_response.json()['content'][0]
        else:
            logging.info(f"No quality check found for {quality_check_name}.")
            return False

    except Exception as e:
        logging.error(f"Error during API request for quality check: {e}")

def post_single_quality_result_on_blindata(quality_check, metric):
    now = datetime.utcnow()
    try:
        result = {
            "qualityCheck": quality_check,
            "metric": metric,
            "totalElements": "100",
            "startedAt": (now.strftime("%Y-%m-%dT%H:%M:%S.") + str(now.microsecond // 1000).zfill(3) + 'Z')
        }

        response = requests.post(
            "https://app.blindata.io/api/v1/data-quality/results",
            json=result,
            headers=HEADER | {"Authorization": get_token()},
        )

        if response.status_code == 201:
            logging.info(f"Result for quality check {quality_check['code']} updated successfully!")
        else:
            logging.error(f"Error during results upload: {response.status_code} - {response.text}")
        
        return response.status_code

    except Exception as e:
        logging.error(f"Error during API request to create quality check: {e}")


# create_minimum_quality_check(
#     data_quality_check_code="minimumQualityCheck",
#     data_quality_check_name="minimumQualityCheck",
#     data_quality_check_warning_threshold=90,
#     data_quality_check_success_threshold=100,
#     data_quality_check_expected_value=3
# )

#post_single_quality_result_on_blindata(quality_check=get_quality_check("minimumQualityCheck"), metric=25)
#post_single_quality_result_on_blindata(quality_check=get_quality_check("maximumQualityCheck"), metric=75)
# post_single_quality_result_on_blindata(quality_check=get_quality_check("distanceQualityCheck"), metric=50)
# post_single_quality_result_on_blindata(quality_check=get_quality_check("distanceQualityCheck"), metric=25)
# post_single_quality_result_on_blindata(quality_check=get_quality_check("distanceQualityCheck"), metric=75)


# true/false
post_single_quality_result_on_blindata(quality_check=get_quality_check("distanceTrueFalseQualityCheck"), metric=1)
post_single_quality_result_on_blindata(quality_check=get_quality_check("distanceTrueFalseQualityCheck"), metric=0)
