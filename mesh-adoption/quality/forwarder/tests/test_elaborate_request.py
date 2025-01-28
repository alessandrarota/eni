import pytest
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from forwarder import create_processor, init_configurations
from main import elaborate_request, checking_for_new_metrics, handle_metrics
from forwarder.data.entities.MetricCurrent import MetricCurrent
from forwarder.data.entities.MetricHistory import MetricHistory
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from forwarder.blindata.blindata import get_blindata_token
import logging
import re

def init_database(configurations):
    logging.info(f"Setting up the database: {configurations.DATABASE_URL}")
    engine = create_engine(configurations.DATABASE_URL, isolation_level=configurations.ENGINE_ISOLATION_LEVEL)
    configurations.SESSION_MAKER = sessionmaker(bind=engine)

    MetricCurrent.metadata.create_all(engine)
    MetricHistory.metadata.create_all(engine)

def destroy_database(configurations):
    engine = create_engine(configurations.DATABASE_URL, isolation_level=configurations.ENGINE_ISOLATION_LEVEL)
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
            metric_value=record["value"],
            unit_of_measure=record["unit_of_measure"],
            element_count=record["element_count"],
            unexpected_count=record["unexpected_count"],
            timestamp=record["timestamp"],
            data_source_name = record["data_source_name"],
            data_asset_name = record["data_asset_name"],
            column_name = record["column_name"]
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
            metric_value=record["value"],
            unit_of_measure=record["unit_of_measure"],
            element_count=record["element_count"],
            unexpected_count=record["unexpected_count"],
            timestamp=record["timestamp"],
            data_source_name = record["data_source_name"],
            data_asset_name = record["data_asset_name"],
            column_name = record["column_name"],
            insert_datetime=record["insert_datetime"],
            flow_name=record["flow_name"]
        )
        session.add(metric)
    session.commit()

def job_without_blindata(configurations):
    metrics = checking_for_new_metrics(configurations)

    if metrics is not None and len(metrics) != 0:
        handle_metrics(configurations, metrics)

def test_migration_with_empty_database():
    configurations = init_configurations("base")
    init_database(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    job_without_blindata(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    destroy_database(configurations)

def test_migration_with_no_metric_current_and_existing_metric_history():
    configurations = init_configurations("base")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    history_metrics = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "metric_name": "datasourcesample-dataassetsample",
            "metric_description": "Validation results for suite: dataSourceSample-dataAssetSample",
            "value": 93.33,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 667,
            "timestamp": "2025-01-28 16:00:44.565",
            "insert_datetime": "2025-01-27T15:55:49.933437+00",
            "flow_name": "blindata-forwarder-pytest",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "passenger_count"
        }
    ]
    populate_metric_history(session, history_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    job_without_blindata(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    history_metrics = MetricHistory.get_all_history_metrics(configurations)

    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "checkAcceptedValues"
    assert history_metrics[0].metric_name == "datasourcesample-dataassetsample"
    assert history_metrics[0].metric_description == "Validation results for suite: dataSourceSample-dataAssetSample"
    assert history_metrics[0].metric_value == 93.33
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].element_count == 10000
    assert history_metrics[0].unexpected_count == 667
    assert history_metrics[0].flow_name == "blindata-forwarder-pytest"

    destroy_database(configurations)

def test_migration_with_existing_metric_current_and_no_existing_metric_history():
    configurations = init_configurations("base")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    current_metrics = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "metric_name": "datasourcesample-dataassetsample",
            "metric_description": "Validation results for suite: dataSourceSample-dataAssetSample",
            "value": 93.33,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 667,
            "timestamp": "2025-01-28 16:00:44.565",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "passenger_count"
        },
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "metric_name": "datasourcesample-dataassetsample",
            "metric_description": "Validation results for suite: dataSourceSample-dataAssetSample",
            "value": 100.00,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 10000,
            "timestamp": "2025-01-28 16:00:44.565",
            "insert_datetime": "2025-01-27T15:55:49.933437+00",
            "flow_name": "blindata-forwarder-pytest",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "pickup_datetime"
        }
    ]
    populate_metric_current(session, current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 2
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    job_without_blindata(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 2

    history_metrics = MetricHistory.get_all_history_metrics(configurations)

    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "checkAcceptedValues"
    assert history_metrics[0].metric_name == "datasourcesample-dataassetsample"
    assert history_metrics[0].metric_description == "Validation results for suite: dataSourceSample-dataAssetSample"
    assert history_metrics[0].metric_value == 93.33
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].element_count == 10000
    assert history_metrics[0].unexpected_count == 667
    assert history_metrics[0].flow_name == "blindata-forwarder-pytest"

    assert history_metrics[1].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[1].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[1].expectation_name == "checkAcceptedValues"
    assert history_metrics[1].metric_name == "datasourcesample-dataassetsample"
    assert history_metrics[1].metric_description == "Validation results for suite: dataSourceSample-dataAssetSample"
    assert history_metrics[1].metric_value == 100.0
    assert history_metrics[1].unit_of_measure == "%"
    assert history_metrics[1].element_count == 10000
    assert history_metrics[1].unexpected_count == 10000
    assert history_metrics[1].flow_name == "blindata-forwarder-pytest"

    destroy_database(configurations)

def test_migration_with_existng_metric_current_and_existing_metric_history():
    configurations = init_configurations("base")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    current_metrics = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "metric_name": "datasourcesample-dataassetsample",
            "metric_description": "Validation results for suite: dataSourceSample-dataAssetSample",
            "value": 93.33,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 667,
            "timestamp": "2025-01-03T10:46:47.191770889Z",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "passenger_count"
        }
    ]
    history_metrics =[
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "metric_name": "datasourcesample-dataassetsample",
            "metric_description": "Validation results for suite: dataSourceSample-dataAssetSample",
            "value": 100.0,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 10000,
            "timestamp": "2025-01-03T10:46:47.191770889Z",
            "insert_datetime": "2025-01-03T12:04:55.792280507Z",
            "flow_name": "blindata-forwarder-pytest",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "pickup_datetime"
        }
    ]
    populate_metric_current(session, current_metrics)
    populate_metric_history(session, history_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 1
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    job_without_blindata(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 2

    history_metrics = MetricHistory.get_all_history_metrics(configurations)

    assert history_metrics[1].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[1].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[1].expectation_name == "checkAcceptedValues"
    assert history_metrics[1].metric_name == "datasourcesample-dataassetsample"
    assert history_metrics[1].metric_description == "Validation results for suite: dataSourceSample-dataAssetSample"
    assert history_metrics[1].metric_value == 93.33
    assert history_metrics[1].unit_of_measure == "%"
    assert history_metrics[1].element_count == 10000
    assert history_metrics[1].unexpected_count == 667
    assert history_metrics[1].flow_name == "blindata-forwarder-pytest"
    assert history_metrics[1].data_source_name == "dataSourceSample"
    assert history_metrics[1].data_asset_name == "dataAssetSample"
    assert history_metrics[1].column_name == "passenger_count"


    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "checkAcceptedValues"
    assert history_metrics[0].metric_name == "datasourcesample-dataassetsample"
    assert history_metrics[0].metric_description == "Validation results for suite: dataSourceSample-dataAssetSample"
    assert history_metrics[0].metric_value == 100.00
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].element_count == 10000
    assert history_metrics[0].unexpected_count == 10000
    assert history_metrics[0].flow_name == "blindata-forwarder-pytest"
    assert history_metrics[0].data_source_name == "dataSourceSample"
    assert history_metrics[0].data_asset_name == "dataAssetSample"
    assert history_metrics[0].column_name == "pickup_datetime"

def test_migration_with_duplicates_in_metric_current_and_existing_records_in_metric_history():
    configurations = init_configurations("base")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    existing_metric_history = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "metric_name": "datasourcesample-dataassetsample",
            "metric_description": "Validation results for suite: dataSourceSample-dataAssetSample",
            "value": 100.00,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 10000,
            "timestamp": "2025-01-28 16:00:44.565",
            "insert_datetime": "2025-01-27T15:55:49.933437+00",
            "flow_name": "blindata-forwarder-pytest",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "pickup_datetime"
        }
    ]
    populate_metric_history(session, existing_metric_history)

    duplicate_metric_current = [
        {
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "metric_name": "datasourcesample-dataassetsample",
            "metric_description": "Validation results for suite: dataSourceSample-dataAssetSample",
            "value": 100.00,
            "unit_of_measure": "%",
            "element_count": 10000,
            "unexpected_count": 10000,
            "timestamp": "2025-01-28 16:00:44.565",
            "insert_datetime": "2025-01-27T15:55:49.933437+00",
            "flow_name": "blindata-forwarder-pytest",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "pickup_datetime"
        }
    ]
    populate_metric_current(session, duplicate_metric_current)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 1
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    job_without_blindata(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    history_metrics = MetricHistory.get_all_history_metrics(configurations)
    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "checkAcceptedValues"
    assert history_metrics[0].metric_name == "datasourcesample-dataassetsample"
    assert history_metrics[0].metric_description == "Validation results for suite: dataSourceSample-dataAssetSample"
    assert history_metrics[0].metric_value == 100.00
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].element_count == 10000
    assert history_metrics[0].unexpected_count == 10000
    assert history_metrics[0].flow_name == "blindata-forwarder-pytest"
    assert history_metrics[0].data_source_name == "dataSourceSample"
    assert history_metrics[0].data_asset_name == "dataAssetSample"
    assert history_metrics[0].column_name == "pickup_datetime"


    destroy_database(configurations)