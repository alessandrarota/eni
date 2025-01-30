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
            business_domain_name=record["business_domain_name"],
            data_product_name=record["data_product_name"],
            expectation_name=record["expectation_name"],
            data_source_name=record["data_source_name"],
            data_asset_name=record["data_asset_name"],
            column_name=record["column_name"],
            blindata_suite_name=record["blindata_suite_name"],
            gx_suite_name=record["gx_suite_name"],
            metric_value=record["metric_value"],
            unit_of_measure=record["unit_of_measure"],
            checked_elements_nbr=record["checked_elements_nbr"],
            errors_nbr=record["errors_nbr"],
            app_name=record["app_name"],
            otlp_sending_datetime=record["otlp_sending_datetime"],
            status_code=record.get("status_code"),
            locking_service_code=record.get("locking_service_code"),
            insert_datetime=record.get("insert_datetime"),
            update_datetime=record.get("update_datetime")
        )
        session.add(metric)
    session.commit()

def populate_metric_history(session, data):
    for record in data:
        metric = MetricHistory(
            business_domain_name=record["business_domain_name"],
            data_product_name=record["data_product_name"],
            expectation_name=record["expectation_name"],
            data_source_name=record["data_source_name"],
            data_asset_name=record["data_asset_name"],
            column_name=record["column_name"],
            blindata_suite_name=record["blindata_suite_name"],
            gx_suite_name=record["gx_suite_name"],
            metric_value=record["metric_value"],
            unit_of_measure=record["unit_of_measure"],
            checked_elements_nbr=record["checked_elements_nbr"],
            errors_nbr=record["errors_nbr"],
            app_name=record["app_name"],
            otlp_sending_datetime=record["otlp_sending_datetime"],
            status_code=record.get("status_code"),
            insert_datetime=record["insert_datetime"],
            source_service_code=record.get("source_service_code")
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
            "business_domain_name": "eniPowerProduzione",
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "blindata_suite_name": "dataSourceSample-dataAssetSample",
            "gx_suite_name": "dataSourceSample-dataAssetSample",
            "metric_value": 93.33,
            "unit_of_measure": "%",
            "checked_elements_nbr": 10000,
            "errors_nbr": 667,
            "otlp_sending_datetime": "2025-01-28 16:00:44.565",
            "insert_datetime": "2025-01-27T15:55:49.933437+00",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "passenger_count",
            "status_code": "SUCCESS",
            "source_service_code": "blindata-forwarder-pytest"
        }
    ]
    populate_metric_history(session, history_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    job_without_blindata(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 1

    history_metrics = MetricHistory.get_all_history_metrics(configurations)
    metric = history_metrics[0]

    assert metric.business_domain_name == "eniPowerProduzione"
    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.app_name == "consuntiviDiProduzione-quality_sidecar"
    assert metric.expectation_name == "checkAcceptedValues"
    assert metric.blindata_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.gx_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.metric_value == 93.33
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 667
    assert metric.data_source_name == "dataSourceSample"
    assert metric.data_asset_name == "dataAssetSample"
    assert metric.column_name == "passenger_count"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert metric.insert_datetime == "2025-01-27T15:55:49.933437+00"
    assert metric.status_code == "SUCCESS"
    assert metric.source_service_code == "blindata-forwarder-pytest"

    destroy_database(configurations)

def test_migration_with_existing_metric_current_and_no_existing_metric_history():
    configurations = init_configurations("base")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    
    current_metrics = [
        {
            "business_domain_name": "eniPowerProduzione", 
            "data_product_name": "consuntiviDiProduzione",
            "expectation_name": "checkAcceptedValues",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "passenger_count",
            "blindata_suite_name": "suite_example",  
            "gx_suite_name": "suite_example",  
            "metric_value": 93.33,
            "unit_of_measure": "%",
            "checked_elements_nbr": 10000,
            "errors_nbr": 667,
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "otlp_sending_datetime": "2025-01-28 16:00:44.565",
            "status_code": "200",  
            "locking_service_code": "blindata-forwarder-pytest",  
            "insert_datetime": "2025-01-28 15:00:00",
            "update_datetime": "2025-01-28 16:00:00"
        },
        {
            "business_domain_name": "eniPowerProduzione",  
            "data_product_name": "consuntiviDiProduzione",
            "expectation_name": "checkAcceptedValues",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "pickup_datetime",
            "blindata_suite_name": "suite_example", 
            "gx_suite_name": "Suite for validation of pickup datetime",  
            "metric_value": 100.00,
            "unit_of_measure": "%",
            "checked_elements_nbr": 10000,
            "errors_nbr": 10000,
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "otlp_sending_datetime": "2025-01-28 16:00:44.565",
            "status_code": "200",  
            "locking_service_code": "blindata-forwarder-pytest", 
            "insert_datetime": "2025-01-27T15:55:49.933437+00",
            "update_datetime": "2025-01-28T16:00:00+00"
        }
    ]
    populate_metric_current(session, current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 2
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 0

    job_without_blindata(configurations)

    assert len(MetricCurrent.get_all_current_metrics(configurations)) == 0
    assert len(MetricHistory.get_all_history_metrics(configurations)) == 2

    history_metrics = MetricHistory.get_all_history_metrics(configurations)

    assert history_metrics[0].business_domain_name == "eniPowerProduzione"  
    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].expectation_name == "checkAcceptedValues"
    assert history_metrics[0].data_source_name == "dataSourceSample"
    assert history_metrics[0].data_asset_name == "dataAssetSample"
    assert history_metrics[0].column_name == "passenger_count"
    assert history_metrics[0].blindata_suite_name == "suite_example"
    assert history_metrics[0].gx_suite_name == "suite_example"
    assert history_metrics[0].metric_value == 93.33
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].checked_elements_nbr == 10000
    assert history_metrics[0].errors_nbr == 667
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert history_metrics[0].status_code == "200"
    assert history_metrics[0].source_service_code == "blindata-forwarder-pytest"
    
    assert history_metrics[1].business_domain_name == "eniPowerProduzione" 
    assert history_metrics[1].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[1].expectation_name == "checkAcceptedValues"
    assert history_metrics[1].data_source_name == "dataSourceSample"
    assert history_metrics[1].data_asset_name == "dataAssetSample"
    assert history_metrics[1].column_name == "pickup_datetime"
    assert history_metrics[1].blindata_suite_name == "suite_example"
    assert history_metrics[1].gx_suite_name == "Suite for validation of pickup datetime"
    assert history_metrics[1].metric_value == 100.0
    assert history_metrics[1].unit_of_measure == "%"
    assert history_metrics[1].checked_elements_nbr == 10000
    assert history_metrics[1].errors_nbr == 10000
    assert history_metrics[1].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[1].otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert history_metrics[1].status_code == "200"
    assert history_metrics[1].source_service_code == "blindata-forwarder-pytest"

    destroy_database(configurations)


def test_migration_with_existng_metric_current_and_existing_metric_history():
    configurations = init_configurations("base")
    init_database(configurations)

    session = configurations.SESSION_MAKER()
    current_metrics = [
        {
            "business_domain_name": "eniPowerProduzione",  
            "data_product_name": "consuntiviDiProduzione",
            "expectation_name": "checkAcceptedValues",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "pickup_datetime",
            "blindata_suite_name": "suite_example", 
            "gx_suite_name": "Suite for validation of pickup datetime",  
            "metric_value": 100.00,
            "unit_of_measure": "%",
            "checked_elements_nbr": 10000,
            "errors_nbr": 10000,
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "otlp_sending_datetime": "2025-01-28 16:00:44.565",
            "status_code": "200",  
            "locking_service_code": "blindata-forwarder-pytest", 
            "insert_datetime": "2025-01-27T15:55:49.933437+00",
            "update_datetime": "2025-01-28T16:00:00+00"
        }
    ]
    history_metrics =[
        {
            "business_domain_name": "eniPowerProduzione",
            "data_product_name": "consuntiviDiProduzione",
            "app_name": "consuntiviDiProduzione-quality_sidecar",
            "expectation_name": "checkAcceptedValues",
            "blindata_suite_name": "dataSourceSample-dataAssetSample",
            "gx_suite_name": "dataSourceSample-dataAssetSample",
            "metric_value": 93.33,
            "unit_of_measure": "%",
            "checked_elements_nbr": 10000,
            "errors_nbr": 667,
            "otlp_sending_datetime": "2025-01-28 16:00:44.565",
            "insert_datetime": "2025-01-27T15:55:49.933437+00",
            "data_source_name": "dataSourceSample",
            "data_asset_name": "dataAssetSample",
            "column_name": "passenger_count",
            "status_code": "SUCCESS",
            "source_service_code": "blindata-forwarder-pytest"
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

    assert history_metrics[0].business_domain_name == "eniPowerProduzione"
    assert history_metrics[0].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[0].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[0].expectation_name == "checkAcceptedValues"
    assert history_metrics[0].blindata_suite_name == "dataSourceSample-dataAssetSample"
    assert history_metrics[0].gx_suite_name == "dataSourceSample-dataAssetSample"
    assert history_metrics[0].metric_value == 93.33
    assert history_metrics[0].unit_of_measure == "%"
    assert history_metrics[0].checked_elements_nbr == 10000
    assert history_metrics[0].errors_nbr == 667
    assert history_metrics[0].data_source_name == "dataSourceSample"
    assert history_metrics[0].data_asset_name == "dataAssetSample"
    assert history_metrics[0].column_name == "passenger_count"
    assert history_metrics[0].otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert history_metrics[0].insert_datetime == "2025-01-27T15:55:49.933437+00"
    assert history_metrics[0].status_code == "SUCCESS"
    assert history_metrics[0].source_service_code == "blindata-forwarder-pytest"
    
    assert history_metrics[1].business_domain_name == "eniPowerProduzione" 
    assert history_metrics[1].data_product_name == "consuntiviDiProduzione"
    assert history_metrics[1].expectation_name == "checkAcceptedValues"
    assert history_metrics[1].data_source_name == "dataSourceSample"
    assert history_metrics[1].data_asset_name == "dataAssetSample"
    assert history_metrics[1].column_name == "pickup_datetime"
    assert history_metrics[1].blindata_suite_name == "suite_example"
    assert history_metrics[1].gx_suite_name == "Suite for validation of pickup datetime"
    assert history_metrics[1].metric_value == 100.0
    assert history_metrics[1].unit_of_measure == "%"
    assert history_metrics[1].checked_elements_nbr == 10000
    assert history_metrics[1].errors_nbr == 10000
    assert history_metrics[1].app_name == "consuntiviDiProduzione-quality_sidecar"
    assert history_metrics[1].otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert history_metrics[1].status_code == "200"
    assert history_metrics[1].source_service_code == "blindata-forwarder-pytest"

    destroy_database(configurations)

