from src.exceptions.exception_handler import handle_exceptions
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
from datetime import datetime
from src import create_processor
from src.data.entities.MetricCurrent import MetricCurrent
from src.data.entities.MetricHistory import MetricHistory
from datetime import datetime, timezone
from src.blindata.blindata import *
import sys
import logging
import schedule
import time
import requests

def handle_metrics(config, current_metrics):
    try:
        history_metrics = [
            MetricHistory(
                data_product_name=metric.data_product_name,
                app_name=metric.app_name,
                metric_name=metric.metric_name,
                expectation_name=metric.expectation_name,
                metric_description=metric.metric_description,
                value=metric.value,
                unit_of_measure=metric.unit_of_measure,
                element_count=metric.element_count,
                unexpected_count=metric.unexpected_count,
                timestamp=metric.timestamp,
                flow_name=config.FLOW_NAME,
                insert_datetime=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            )
            for metric in current_metrics
        ]
        
        MetricHistory.save_history_metrics(config, history_metrics)
        MetricCurrent.delete_current_metrics(config, current_metrics)

        logging.info(
            f"Successfully processed and transferred {len(current_metrics)} metrics from {MetricCurrent.__tablename__} to {MetricHistory.__tablename__}."
        )
    except SQLAlchemyError as e:
        logging.error(f"Error during job: {e}")

def metric_processor_job(config, current_metrics):
    try:
        blindata_response = post_quality_results_on_blindata(config, current_metrics)

        if blindata_response and blindata_response.status_code == 200 and blindata_response.json()['errors'] == []:
            handle_metrics(config, current_metrics)
        else:
            logging.error(f"Blindata response failed with status: {blindata_response.status_code if blindata_response else 'No response'}")
    except Exception as e:
        logging.error(f"Error during job: {e}")

def checking_for_new_metrics(config):
    try:
        return MetricCurrent.get_all_current_metrics(config)
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving new metric from {MetricCurrent.__tablename__}.\n {e}")

def elaborate_request(config):
    current_metrics = checking_for_new_metrics(config)

    if current_metrics is not None and len(current_metrics) != 0:
        logging.info(f"{len(current_metrics)} metrics to send to {MetricHistory.__tablename__} - I'm starting")
        metric_processor_job(config, current_metrics)
    else:
        logging.debug(f"No metrics from {MetricCurrent.__tablename__} available, I'm done")


def main():
    while 1:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    sys.excepthook = handle_exceptions
    config = create_processor()
    logging.info("forwarder setup completed")
    threads = []
    schedule.every(20).seconds.do(elaborate_request, config)

    main()
