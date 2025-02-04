from TestHandler import TestHandler
from forwarder.data.entities.MetricCurrent import MetricCurrent
from forwarder.data.entities.MetricHistory import MetricHistory
import logging

def test_lock_current_metrics():
    testHandler = TestHandler()

    testHandler.init_database()

    current_metrics = [
        MetricCurrent(
            business_domain_name="",  
            data_product_name="consuntiviDiProduzione",
            expectation_name="checkAcceptedValues",
            data_source_name="dataSourceSample",
            data_asset_name="dataAssetSample",
            column_name="pickup_datetime",
            blindata_suite_name="-consuntiviDiProduzione",
            gx_suite_name="dataSourceSample-dataAssetSamplee",
            data_quality_dimension_name="Validity",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            app_name="consuntiviDiProduzione-quality_sidecar",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            status_code="NEW",
            locking_service_code="",  
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 16:00:00"
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

def test_empty_business_domain_name():
    testHandler = TestHandler()

    testHandler.init_database()

    current_metrics = [
         MetricCurrent(
            business_domain_name="",  
            data_product_name="consuntiviDiProduzione",
            expectation_name="checkAcceptedValues",
            data_source_name="dataSourceSample",
            data_asset_name="dataAssetSample",
            column_name="pickup_datetime",
            blindata_suite_name="-consuntiviDiProduzione",
            gx_suite_name="dataSourceSample-dataAssetSample",
            data_quality_dimension_name="Validity",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            app_name="consuntiviDiProduzione-quality_sidecar",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            status_code="NEW",
            locking_service_code="",  
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 16:00:00"
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

    assert metric.business_domain_name == ""
    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.expectation_name == "checkAcceptedValues"
    assert metric.data_source_name == "dataSourceSample"
    assert metric.data_asset_name == "dataAssetSample"
    assert metric.column_name == "pickup_datetime"
    assert metric.blindata_suite_name == "-consuntiviDiProduzione"
    assert metric.gx_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.data_quality_dimension_name == "Validity"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.app_name == "consuntiviDiProduzione-quality_sidecar"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert metric.status_code == "ERR_EMPTY_BUSINESS_DOMAIN"
    assert metric.source_service_code.startswith("sd-")

    testHandler.destroy_database()
    testHandler.destroy_checks_and_suite()

def test_null_blindata_suite_name():
    testHandler = TestHandler()

    testHandler.init_database()

    current_metrics = [
        MetricCurrent(
            business_domain_name="wrongBusinessDomain",  
            data_product_name="consuntiviDiProduzione",
            expectation_name="checkAcceptedValues",
            data_source_name="dataSourceSample",
            data_asset_name="dataAssetSample",
            column_name="pickup_datetime",
            blindata_suite_name=None,
            gx_suite_name="dataSourceSample-dataAssetSample",
            data_quality_dimension_name="Validity",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            app_name="consuntiviDiProduzione-quality_sidecar",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            status_code="NEW",
            locking_service_code="",  
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 16:00:00"
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

    assert metric.business_domain_name == "wrongBusinessDomain"
    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.expectation_name == "checkAcceptedValues"
    assert metric.data_source_name == "dataSourceSample"
    assert metric.data_asset_name == "dataAssetSample"
    assert metric.column_name == "pickup_datetime"
    assert metric.blindata_suite_name == None
    assert metric.gx_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.data_quality_dimension_name == "Validity"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.app_name == "consuntiviDiProduzione-quality_sidecar"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert metric.status_code == "ERR_WRONG_BUSINESS_DOMAIN"
    assert metric.source_service_code.startswith("sd-")

    testHandler.destroy_database()
    testHandler.destroy_checks_and_suite()

def test_blindata_suite_not_found():
    testHandler = TestHandler()

    testHandler.init_database()

    current_metrics = [
        MetricCurrent(
            business_domain_name="eniPowerProduzione",  
            data_product_name="consuntiviDiProduzione",
            expectation_name="checkAcceptedValues",
            data_source_name="dataSourceSample",
            data_asset_name="dataAssetSample",
            column_name="pickup_datetime",
            blindata_suite_name="eniPowerProduzione-consuntiviDiProduzione",
            gx_suite_name="dataSourceSample-dataAssetSample",
            data_quality_dimension_name="Validity",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            app_name="consuntiviDiProduzione-quality_sidecar",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            status_code="NEW",
            locking_service_code="",  
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 16:00:00"
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

    assert metric.business_domain_name == "eniPowerProduzione"
    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.expectation_name == "checkAcceptedValues"
    assert metric.data_source_name == "dataSourceSample"
    assert metric.data_asset_name == "dataAssetSample"
    assert metric.column_name == "pickup_datetime"
    assert metric.blindata_suite_name == "eniPowerProduzione-consuntiviDiProduzione"
    assert metric.gx_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.data_quality_dimension_name == "Validity"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.app_name == "consuntiviDiProduzione-quality_sidecar"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert metric.status_code == "ERR_BLINDATA_SUITE_NOT_FOUND"
    assert metric.source_service_code.startswith("sd-")

    testHandler.destroy_database()
    testHandler.destroy_checks_and_suite()

def test_error_check_creation():
    testHandler = TestHandler()

    testHandler.init_database()

    testHandler.create_quality_suite("eniPowerProduzione-consuntiviDiProduzione")

    current_metrics = [
        MetricCurrent(
            business_domain_name="eniPowerProduzione",  
            data_product_name="consuntiviDiProduzione",
            expectation_name="checkAcceptedValues",
            data_source_name="dataSourceSample",
            data_asset_name="dataAssetSample",
            column_name="pickup_datetime",
            blindata_suite_name="eniPowerProduzione-consuntiviDiProduzione",
            gx_suite_name="dataSourceSample-dataAssetSample",
            data_quality_dimension_name="Validity",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            app_name="consuntiviDiProduzione-quality_sidecar",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            status_code="NEW",
            locking_service_code="",  
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 16:00:00"
        )
    ]
    testHandler.populate_metrics(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = testHandler.lock_new_current_metric()

    testHandler.job_without_blindata(current_metrics, ERR_FAILED_BLINDATA_CHECK_CREATION=True)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 0
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 1

    history_metrics = MetricHistory.get_all_history_metrics(testHandler.get_configurations())
    metric = history_metrics[0]

    assert metric.business_domain_name == "eniPowerProduzione"
    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.expectation_name == "checkAcceptedValues"
    assert metric.data_source_name == "dataSourceSample"
    assert metric.data_asset_name == "dataAssetSample"
    assert metric.column_name == "pickup_datetime"
    assert metric.blindata_suite_name == "eniPowerProduzione-consuntiviDiProduzione"
    assert metric.gx_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.data_quality_dimension_name == "Validity"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.app_name == "consuntiviDiProduzione-quality_sidecar"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert metric.status_code == "ERR_FAILED_BLINDATA_CHECK_CREATION"
    assert metric.source_service_code.startswith("sd-")

    testHandler.destroy_database()
    testHandler.destroy_checks_and_suite()

def test_error_blindata():
    testHandler = TestHandler()

    testHandler.init_database()

    testHandler.create_quality_suite("eniPowerProduzione-consuntiviDiProduzione")

    current_metrics = [
        MetricCurrent(
            business_domain_name="eniPowerProduzione",  
            data_product_name="consuntiviDiProduzione",
            expectation_name="checkAcceptedValues",
            data_source_name="dataSourceSample",
            data_asset_name="dataAssetSample",
            column_name="pickup_datetime",
            blindata_suite_name="eniPowerProduzione-consuntiviDiProduzione",
            gx_suite_name="dataSourceSample-dataAssetSample",
            data_quality_dimension_name="Validity",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            app_name="consuntiviDiProduzione-quality_sidecar",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            status_code="NEW",
            locking_service_code="",  
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 16:00:00"
        )
    ]
    testHandler.populate_metrics(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = testHandler.lock_new_current_metric()

    testHandler.job_without_blindata(current_metrics, STATUS_CODE=502)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 0
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 1

    history_metrics = MetricHistory.get_all_history_metrics(testHandler.get_configurations())
    metric = history_metrics[0]

    assert metric.business_domain_name == "eniPowerProduzione"
    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.expectation_name == "checkAcceptedValues"
    assert metric.data_source_name == "dataSourceSample"
    assert metric.data_asset_name == "dataAssetSample"
    assert metric.column_name == "pickup_datetime"
    assert metric.blindata_suite_name == "eniPowerProduzione-consuntiviDiProduzione"
    assert metric.gx_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.data_quality_dimension_name == "Validity"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.app_name == "consuntiviDiProduzione-quality_sidecar"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert metric.status_code == "ERR_BLINDATA_502"
    assert metric.source_service_code.startswith("sd-")

    testHandler.destroy_database()
    testHandler.destroy_checks_and_suite()

def test_success_with_existing_quality_check():
    testHandler = TestHandler()

    testHandler.init_database()

    quality_suite = testHandler.create_quality_suite("eniPowerProduzione-consuntiviDiProduzione")
    testHandler.create_quality_check("checkAcceptedValues_dataSourceSample-dataAssetSample-pickup_datetime", quality_suite, "Validity")

    current_metrics = [
        MetricCurrent(
            business_domain_name="eniPowerProduzione",  
            data_product_name="consuntiviDiProduzione",
            expectation_name="checkAcceptedValues",
            data_source_name="dataSourceSample",
            data_asset_name="dataAssetSample",
            column_name="pickup_datetime",
            blindata_suite_name="eniPowerProduzione-consuntiviDiProduzione",
            gx_suite_name="dataSourceSample-dataAssetSample",
            data_quality_dimension_name="Validity",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            app_name="consuntiviDiProduzione-quality_sidecar",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            status_code="NEW",
            locking_service_code="",  
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 16:00:00"
        )
    ]
    testHandler.populate_metrics(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = testHandler.lock_new_current_metric()

    testHandler.job_without_blindata(current_metrics)

    assert len(testHandler.quality_checks) == 1
    assert len(testHandler.quality_suites) == 1

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 0
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 1

    history_metrics = MetricHistory.get_all_history_metrics(testHandler.get_configurations())
    metric = history_metrics[0]

    assert metric.business_domain_name == "eniPowerProduzione"
    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.expectation_name == "checkAcceptedValues"
    assert metric.data_source_name == "dataSourceSample"
    assert metric.data_asset_name == "dataAssetSample"
    assert metric.column_name == "pickup_datetime"
    assert metric.blindata_suite_name == "eniPowerProduzione-consuntiviDiProduzione"
    assert metric.gx_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.data_quality_dimension_name == "Validity"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.app_name == "consuntiviDiProduzione-quality_sidecar"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert metric.status_code == "SUCCESS"
    assert metric.source_service_code.startswith("sd-")

    testHandler.destroy_database()
    testHandler.destroy_checks_and_suite()

def test_success_without_existing_quality_check():
    testHandler = TestHandler()

    testHandler.init_database()

    testHandler.create_quality_suite("eniPowerProduzione-consuntiviDiProduzione")

    current_metrics = [
        MetricCurrent(
            business_domain_name="eniPowerProduzione",  
            data_product_name="consuntiviDiProduzione",
            expectation_name="checkAcceptedValues",
            data_source_name="dataSourceSample",
            data_asset_name="dataAssetSample",
            column_name="pickup_datetime",
            blindata_suite_name="eniPowerProduzione-consuntiviDiProduzione",
            gx_suite_name="dataSourceSample-dataAssetSample",
            data_quality_dimension_name="Validity",
            metric_value=100.00,
            unit_of_measure="%",
            checked_elements_nbr=10000,
            errors_nbr=0,
            app_name="consuntiviDiProduzione-quality_sidecar",
            otlp_sending_datetime="2025-01-28 16:00:44.565",
            status_code="NEW",
            locking_service_code="",  
            insert_datetime="2025-01-28 15:00:00",
            update_datetime="2025-01-28 16:00:00"
        )
    ]
    testHandler.populate_metrics(current_metrics)

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 1
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 0

    current_metrics = testHandler.lock_new_current_metric()

    testHandler.job_without_blindata(current_metrics)

    assert len(testHandler.quality_checks) == 1
    assert len(testHandler.quality_suites) == 1

    assert len(MetricCurrent.get_all_current_metrics(testHandler.get_configurations())) == 0
    assert len(MetricHistory.get_all_history_metrics(testHandler.get_configurations())) == 1

    history_metrics = MetricHistory.get_all_history_metrics(testHandler.get_configurations())
    metric = history_metrics[0]

    assert metric.business_domain_name == "eniPowerProduzione"
    assert metric.data_product_name == "consuntiviDiProduzione"
    assert metric.expectation_name == "checkAcceptedValues"
    assert metric.data_source_name == "dataSourceSample"
    assert metric.data_asset_name == "dataAssetSample"
    assert metric.column_name == "pickup_datetime"
    assert metric.blindata_suite_name == "eniPowerProduzione-consuntiviDiProduzione"
    assert metric.gx_suite_name == "dataSourceSample-dataAssetSample"
    assert metric.data_quality_dimension_name == "Validity"
    assert metric.metric_value == 100.00
    assert metric.unit_of_measure == "%"
    assert metric.checked_elements_nbr == 10000
    assert metric.errors_nbr == 0
    assert metric.app_name == "consuntiviDiProduzione-quality_sidecar"
    assert metric.otlp_sending_datetime == "2025-01-28 16:00:44.565"
    assert metric.status_code == "SUCCESS"
    assert metric.source_service_code.startswith("sd-")

    testHandler.destroy_database()
    testHandler.destroy_checks_and_suite()