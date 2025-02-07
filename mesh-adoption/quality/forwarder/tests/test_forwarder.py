from TestHandler import TestHandler
from forwarder.data.entities.MetricCurrent import MetricCurrent
from forwarder.data.entities.MetricHistory import MetricHistory
import logging

def test_lock_current_metrics():
    testHandler = TestHandler()

    testHandler.init_database()

    current_metrics = [
        MetricCurrent(
            data_product_name="consuntiviDiProduzione",
            check_name="checkAcceptedValues",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            metric_source_name="dataSourceSample",
            status_code="NEW",
            locking_service_code="",
            otlp_sending_datetime_code="20250128160044565",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 15:00:00"
        )
    ]
    testHandler.populate_metrics(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = testHandler.lock_new_current_metric()

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = MetricCurrent.get_all_current_metrics(testHandler.get_configurations())
    metric = current_metrics[0]

    assert metric.status_code == "LOCKED"
    assert metric.locking_service_code.startswith("sd-")

    testHandler.destroy_database()
    testHandler.destroy_checks()

def test_check_not_found():
    testHandler = TestHandler()

    testHandler.init_database()

    current_metrics = [
        MetricCurrent(
            data_product_name="consuntiviDiProduzione",
            check_name="check",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            metric_source_name="dataSourceSample",
            status_code="NEW",
            locking_service_code="",
            otlp_sending_datetime_code="20250128160044565",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 15:00:00"
        )
    ]
    testHandler.populate_metrics(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = testHandler.lock_new_current_metric()

    testHandler.job_without_blindata(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 0
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 1

    history_metrics = MetricHistory.get_all_history_metrics(testHandler.get_configurations())
    metric = history_metrics[0]

    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.check_name == "check"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.metric_source_name == "dataSourceSample"
    assert metric.status_code == "ERR_CHECK_NOT_FOUND"
    assert metric.locking_service_code.startswith("sd-")
    assert metric.otlp_sending_datetime_code == "20250128160044565"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"    

    testHandler.destroy_database()
    testHandler.destroy_checks()

def test_error_blindata():
    testHandler = TestHandler()

    testHandler.init_database()

    testHandler.create_quality_check("check")

    current_metrics = [
        MetricCurrent(
            data_product_name="consuntiviDiProduzione",
            check_name="check",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            metric_source_name="dataSourceSample",
            status_code="NEW",
            locking_service_code="",
            otlp_sending_datetime_code="20250128160044565",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 15:00:00"
        )
    ]
    testHandler.populate_metrics(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = testHandler.lock_new_current_metric()

    testHandler.job_without_blindata(current_metrics, STATUS_CODE=502)

    assert len(testHandler.quality_checks) == 1

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 0
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 1

    history_metrics = MetricHistory.get_all_history_metrics(testHandler.get_configurations())
    metric = history_metrics[0]

    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.check_name == "check"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.metric_source_name == "dataSourceSample"
    assert metric.status_code == "ERR_BLINDATA_502"
    assert metric.locking_service_code.startswith("sd-")
    assert metric.otlp_sending_datetime_code == "20250128160044565"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"   

    testHandler.destroy_database()
    testHandler.destroy_checks()

def test_success_with_existing_quality_check():
    testHandler = TestHandler()

    testHandler.init_database()

    testHandler.create_quality_check("check")

    current_metrics = [
        MetricCurrent(
            data_product_name="consuntiviDiProduzione",
            check_name="check",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            metric_source_name="dataSourceSample",
            status_code="NEW",
            locking_service_code="",
            otlp_sending_datetime_code="20250128160044565",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 15:00:00"
        )
    ]
    testHandler.populate_metrics(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = testHandler.lock_new_current_metric()

    testHandler.job_without_blindata(current_metrics)

    assert len(testHandler.quality_checks) == 1

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 0
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 1

    history_metrics = MetricHistory.get_all_history_metrics(testHandler.get_configurations())
    metric = history_metrics[0]

    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.check_name == "check"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.metric_source_name == "dataSourceSample"
    assert metric.status_code == "SUCCESS"
    assert metric.locking_service_code.startswith("sd-")
    assert metric.otlp_sending_datetime_code == "20250128160044565"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"   

    testHandler.destroy_database()
    testHandler.destroy_checks()