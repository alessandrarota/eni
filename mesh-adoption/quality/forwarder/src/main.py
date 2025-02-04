from forwarder.exceptions.exception_handler import handle_exceptions
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from http.server import HTTPServer
from forwarder import create_processor
from forwarder.data.entities.MetricCurrent import MetricCurrent
from forwarder.data.entities.MetricHistory import MetricHistory
from forwarder.data.enum.MetricStatusCode import MetricStatusCode
from forwarder.blindata.blindata import *
import sys
import logging
import time
import schedule
import os
import threading
from forwarder.configurations.health_check_handler import *

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(level=logging.WARNING)

def validate_business_domain_name(config, current_metric):
    if not current_metric.business_domain_name:
        MetricHistory.save(config, current_metric, MetricStatusCode.ERR_EMPTY_BUSINESS_DOMAIN.value)
        MetricCurrent.delete(config, current_metric)
        return False
    return True

def validate_blindata_suite_name(config, current_metric):
    if current_metric.blindata_suite_name is None:
        MetricHistory.save(config, current_metric, MetricStatusCode.ERR_WRONG_BUSINESS_DOMAIN.value)
        MetricCurrent.delete(config, current_metric)
        return False
    return True

def handle_quality_check_creation(config, current_metric):
    blindata_suite = get_quality_suite(config, current_metric)

    if not blindata_suite:
        MetricHistory.save(config, current_metric, MetricStatusCode.ERR_BLINDATA_SUITE_NOT_FOUND.value)
        MetricCurrent.delete(config, current_metric)
        return False
    else:
        quality_check = create_quality_check(config, current_metric, blindata_suite)

        if not quality_check:
            MetricHistory.save(config, current_metric, MetricStatusCode.ERR_FAILED_BLINDATA_CHECK_CREATION.value)
            MetricCurrent.delete(config, current_metric)
            return False
        else:
            return quality_check

def process_quality_result_upload(config, current_metric, status_code):
    if status_code == 201:
        MetricHistory.save(config, current_metric, MetricStatusCode.SUCCESS.value)
        MetricCurrent.delete(config, current_metric)
    else:
        MetricHistory.save(config, current_metric, f"{MetricStatusCode.ERR_BLINDATA.value}_{status_code}")
        MetricCurrent.delete(config, current_metric)

def handle_locked_metrics(config, current_metric):
    if not validate_business_domain_name(config, current_metric):
        return

    if not validate_blindata_suite_name(config, current_metric):
        return
    
    quality_check = get_quality_check(config, current_metric)

    if not quality_check:
        quality_check = handle_quality_check_creation(config, current_metric)
        if not quality_check:
            return 
        
    status_code = post_single_quality_result_on_blindata(config, quality_check, current_metric)
    process_quality_result_upload(config, current_metric, status_code)

def lock_new_metrics(config):
    try:
        return MetricCurrent.lock_new_current_metrics(config, os.getenv('HOSTNAME'))
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving new metrics: {e}")

def process_new_metrics(config):
    metrics = lock_new_metrics(config)

    if metrics and len(metrics) > 0:
        logging.info(f"Found {len(metrics)} metrics to process, starting processing.")
        for metric in metrics:
            handle_locked_metrics(config, metric)
    else:
        logging.debug("No metrics available for processing.")

def start_http_server():
    logging.info("Starting server on port 5000")
    server = HTTPServer(('0.0.0.0', 5000), RequestHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    while True:
        schedule.run_pending()
        time.sleep(1)

def main():
    sys.excepthook = handle_exceptions
    configuration = create_processor()
    start_blindata_token_refresh_thread(configuration)
    logging.info(f"Forwarder setup completed on {os.getenv('HOSTNAME')}")

    schedule_interval = int(os.getenv('SCHEDULE_INTERVAL'))
    schedule.every(schedule_interval).seconds.do(process_new_metrics, configuration)

    start_http_server()


if __name__ == '__main__':
    main()
