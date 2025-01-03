import pytest
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from src import create_processor, init_configurations
from app import elaborate_request
from src.data.entities.MetricCurrent import MetricCurrent
from src.data.entities.MetricHistory import MetricHistory
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from src.blindata.blindata import get_blindata_token, post_quality_results
import logging
import re

def init_database(configurations):
    logging.info(f"Setting up the database: {configurations.SQLALCHEMY_DATABASE_URI}")
    engine = create_engine(configurations.SQLALCHEMY_DATABASE_URI, isolation_level=configurations.ENGINE_ISOLATION_LEVEL)
    configurations.SESSION_MAKER = sessionmaker(bind=engine)

    MetricCurrent.metadata.create_all(engine)
    MetricHistory.metadata.create_all(engine)

def destroy_database(configurations):
    engine = create_engine(configurations.SQLALCHEMY_DATABASE_URI, isolation_level=configurations.ENGINE_ISOLATION_LEVEL)
    session = configurations.SESSION_MAKER()
    session.query(MetricCurrent).delete()
    session.query(MetricHistory).delete()
    session.commit()
    session.close()
    engine.dispose()

def populate_metric_current(session, data):
    for record in data:
        print(record)
        metric = MetricCurrent(
            data_product_name=record["data_product_name"],
            app_name=record["app_name"],
            expectation_name=record["expectation_name"],
            metric_name=record["metric_name"],
            metric_description=record["metric_description"],
            value=record["value"],
            unit_of_measure=record["unit_of_measure"],
            element_count=record["element_count"],
            unexpected_count=record["unexpected_count"],
            timestamp=record["timestamp"]
        )
        session.add(metric)
    session.commit()

def populate_metric_history(session, data):
    for record in data:
        metric = MetricHistory(
            data_product_name=record["data_product_name"],
            app_name=record["app_name"],
            expectation_name=record["expectation_name"],
            metric_name=record["metric_name"],
            metric_description=record["metric_description"],
            value=record["value"],
            unit_of_measure=record["unit_of_measure"],
            element_count=record["element_count"],
            unexpected_count=record["unexpected_count"],
            timestamp=record["timestamp"],
            insert_datetime=record["insert_datetime"],
            flow_name=record["flow_name"]
        )
        session.add(metric)
    session.commit()

def test_migration_with_empty_database():
    configurations = init_configurations("development")
    init_database(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    elaborate_request(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    destroy_database(configurations)

def test_migration_with_no_metric_current_and_existing_metric_history():
    configurations = init_configurations("development")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    history_metrics = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "expectPassengerCountValuesToBeBetween",
            "metric_name": "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2",
            "metric_description": "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2",
            "value": 93.33,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 667,
            "timestamp": "2025-01-03T10:46:47.191770889Z",
            "insert_datetime": "2025-01-03T10:47:47.191770889Z",
            "flow_name": "forwarder-development"
        }
    ]
    populate_metric_history(session, history_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    elaborate_request(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    history_metrics = MetricHistory.get_all_history_metrics(configurations)

    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "expectPassengerCountValuesToBeBetween"
    assert history_metrics[0].metric_name == "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2"
    assert history_metrics[0].metric_description == "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2"
    assert history_metrics[0].value == 93.33
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].element_count == 10000
    assert history_metrics[0].unexpected_count == 667
    assert history_metrics[0].timestamp == "2025-01-03T10:46:47.191770889Z"
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$", history_metrics[0].insert_datetime)
    assert history_metrics[0].flow_name == "forwarder-development"

    destroy_database(configurations)

def test_migration_with_existing_metric_current_and_no_existing_metric_history():
    configurations = init_configurations("development")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    current_metrics = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "expectPassengerCountValuesToBeBetween",
            "metric_name": "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2",
            "metric_description": "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2",
            "value": 93.33,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 667,
            "timestamp": "2025-01-03T10:46:47.191770889Z"
        },
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "expectPickupDatetimeValuesToMatchRegex",
            "metric_name": "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2",
            "metric_description": "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2",
            "value": 100.0,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 10000,
            "timestamp": "2025-01-03T10:46:47.191770889Z"
        }
    ]
    populate_metric_current(session, current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 2
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    elaborate_request(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 2

    history_metrics = MetricHistory.get_all_history_metrics(configurations)

    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "expectPassengerCountValuesToBeBetween"
    assert history_metrics[0].metric_name == "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2"
    assert history_metrics[0].metric_description == "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2"
    assert history_metrics[0].value == 93.33
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].element_count == 10000
    assert history_metrics[0].unexpected_count == 667
    assert history_metrics[0].timestamp == "2025-01-03T10:46:47.191770889Z"
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$", history_metrics[0].insert_datetime)
    assert history_metrics[0].flow_name == "forwarder-development"

    assert history_metrics[1].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[1].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[1].expectation_name == "expectPickupDatetimeValuesToMatchRegex"
    assert history_metrics[1].metric_name == "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2"
    assert history_metrics[1].metric_description == "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2"
    assert history_metrics[1].value == 100.0
    assert history_metrics[1].unit_of_measure == "%"
    assert history_metrics[1].element_count == 10000
    assert history_metrics[1].unexpected_count == 10000
    assert history_metrics[1].timestamp == "2025-01-03T10:46:47.191770889Z"
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$", history_metrics[1].insert_datetime)
    assert history_metrics[1].flow_name == "forwarder-development"

    destroy_database(configurations)

def test_migration_with_existng_metric_current_and_existing_metric_history():
    configurations = init_configurations("development")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    current_metrics = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "expectPassengerCountValuesToBeBetween",
            "metric_name": "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2",
            "metric_description": "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2",
            "value": 93.33,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 667,
            "timestamp": "2025-01-03T10:46:47.191770889Z"
        }
    ]
    history_metrics =[
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "expectPickupDatetimeValuesToMatchRegex",
            "metric_name": "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2",
            "metric_description": "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2",
            "value": 100.0,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 10000,
            "timestamp": "2025-01-03T10:46:47.191770889Z",
            "insert_datetime": "2025-01-03T12:04:55.792280507Z",
            "flow_name": "forwarder-development"
        }
    ]
    populate_metric_current(session, current_metrics)
    populate_metric_history(session, history_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 1
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    elaborate_request(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 2

    history_metrics = MetricHistory.get_all_history_metrics(configurations)

    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "expectPickupDatetimeValuesToMatchRegex"
    assert history_metrics[0].metric_name == "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2"
    assert history_metrics[0].metric_description == "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2"
    assert history_metrics[0].value == 100.0
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].element_count == 10000
    assert history_metrics[0].unexpected_count == 10000
    assert history_metrics[0].timestamp == "2025-01-03T10:46:47.191770889Z"
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$", history_metrics[0].insert_datetime)
    assert history_metrics[0].flow_name == "forwarder-development"

    assert history_metrics[1].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[1].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[1].expectation_name == "expectPassengerCountValuesToBeBetween"
    assert history_metrics[1].metric_name == "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2"
    assert history_metrics[1].metric_description == "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2"
    assert history_metrics[1].value == 93.33
    assert history_metrics[1].unit_of_measure == "%"
    assert history_metrics[1].element_count == 10000
    assert history_metrics[1].unexpected_count == 667
    assert history_metrics[1].timestamp == "2025-01-03T10:46:47.191770889Z"
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$", history_metrics[1].insert_datetime)
    assert history_metrics[1].flow_name == "forwarder-development"

def test_migration_with_duplicates_in_metric_current_and_existing_records_in_metric_history():
    configurations = init_configurations("development")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    existing_metric_history = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "expectPickupDatetimeValuesToMatchRegex",
            "metric_name": "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2",
            "metric_description": "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2",
            "value": 100.0,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 10000,
            "timestamp": "2025-01-03T10:46:47.191770889Z",
            "insert_datetime": "2025-01-03T12:04:55.792280507Z",
            "flow_name": "forwarder-development"
        }
    ]
    populate_metric_history(session, existing_metric_history)

    duplicate_metric_current = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "expectPickupDatetimeValuesToMatchRegex",
            "metric_name": "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2",
            "metric_description": "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2",
            "value": 100.0,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 10000,
            "timestamp": "2025-01-03T10:46:47.191770889Z",
            "insert_datetime": "2025-01-03T12:04:55.792280507Z",
            "flow_name": "forwarder-development"
        }
    ]
    populate_metric_current(session, duplicate_metric_current)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 1
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    elaborate_request(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    history_metrics = MetricHistory.get_all_history_metrics(configurations)
    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "expectPickupDatetimeValuesToMatchRegex"
    assert history_metrics[0].metric_name == "consuntividiproduzione-cdpdatasourcesample2-cdpdataassetsample2"
    assert history_metrics[0].metric_description == "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2"
    assert history_metrics[0].value == 100.0
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].element_count == 10000
    assert history_metrics[0].unexpected_count == 10000
    assert history_metrics[0].timestamp == "2025-01-03T10:46:47.191770889Z"
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$", history_metrics[0].insert_datetime)
    assert history_metrics[0].flow_name == "forwarder-development"

    destroy_database(configurations)

def test_login_blindata():
    configurations = init_configurations("development")
    bearer_token = get_blindata_token(configurations)

    assert bearer_token != None

def test_migration_with_error_blindata():
    configurations = init_configurations("development")
    configurations.BLINDATA_ACTIVATE = True

    init_database(configurations)

    session = configurations.SESSION_MAKER()

    current_metrics = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "expectPassengerCountValuesToBeBetween",
            "metric_name": "metric-that-does-not-exist-on-blindata", #error
            "metric_description": "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2",
            "value": 93.33,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 667,
            "timestamp": "2025-01-03T10:46:47.191770889Z"
        }
    ]
    populate_metric_current(session, current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 1
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    elaborate_request(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 1
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    current_metrics = MetricCurrent.get_all_current_metrics(configurations)

    assert current_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert current_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert current_metrics[0].expectation_name == "expectPassengerCountValuesToBeBetween"
    assert current_metrics[0].metric_name == "metric-that-does-not-exist-on-blindata"
    assert current_metrics[0].metric_description == "Validation results for suite: consuntiviDiProduzione-cdpDataSourceSample2-cdpDataAssetSample2"
    assert current_metrics[0].value == 93.33
    assert current_metrics[0].unit_of_measure == "%"
    assert current_metrics[0].element_count == 10000
    assert current_metrics[0].unexpected_count == 667
    assert current_metrics[0].timestamp == "2025-01-03T10:46:47.191770889Z"

    destroy_database(configurations)