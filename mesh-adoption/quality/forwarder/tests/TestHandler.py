import pytest
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from forwarder import create_processor, init_configurations
from main import lock_new_current_metrics, validate_business_domain_name, validate_blindata_suite_name, validate_quality_result_upload
from forwarder.data.entities.MetricCurrent import MetricCurrent
from forwarder.data.entities.MetricHistory import MetricHistory
from forwarder.data.enum.MetricStatusCode import MetricStatusCode
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from forwarder.blindata.blindata import get_blindata_token
import logging
import re

class TestHandler():
    def __init__(self):
        self.configurations = init_configurations("base")
        self.quality_checks = []
        self.quality_suites = []
        self.init_database()

    def get_configurations(self):
        return self.configurations

    def init_database(self):
        logging.info(f"Setting up the database: {self.configurations.DATABASE_URL}")
        self.configurations.engine = create_engine(self.configurations.DATABASE_URL, isolation_level=self.configurations.ENGINE_ISOLATION_LEVEL)
        self.configurations.SESSION_MAKER = sessionmaker(bind=self.configurations.engine)

        MetricCurrent.metadata.create_all(self.configurations.engine)
        MetricHistory.metadata.create_all(self.configurations.engine)

    def destroy_database(self):
        session = self.configurations.SESSION_MAKER()
        session.query(MetricCurrent).delete()
        session.query(MetricHistory).delete()
        session.commit()
        session.close()
        self.configurations.engine.dispose()

    def destroy_checks_and_suite(self):
        self.quality_checks.clear()
        self.quality_suites.clear()

    def populate_metrics(self, data):
        session = self.configurations.SESSION_MAKER()
        for record in data:
            session.add(record)
        session.commit()

    def get_quality_check(self, current_metric):
        quality_check_code = f"{current_metric.expectation_name}_{current_metric.data_source_name}-{current_metric.data_asset_name}-{current_metric.column_name}"
    
        if any(quality_check_code == check['code'] for check in self.quality_checks):
            return True
        return False
    
    def get_quality_suite(self, current_metric):
        logging.info(self.quality_suites)
        if any(current_metric.blindata_suite_name == suite['code'] for suite in self.quality_suites):
            return True
        return False
    
    def handle_quality_check_creation(self, current_metric, ERR_FAILED_BLINDATA_CHECK_CREATION):
        blindata_suite = self.get_quality_suite(current_metric)

        if not blindata_suite:
            MetricHistory.save(self.configurations, current_metric, MetricStatusCode.ERR_BLINDATA_SUITE_NOT_FOUND.value)
            MetricCurrent.delete(self.configurations, current_metric)
            return False
        else:
            if ERR_FAILED_BLINDATA_CHECK_CREATION:
                MetricHistory.save(self.configurations, current_metric, MetricStatusCode.ERR_FAILED_BLINDATA_CHECK_CREATION.value)
                MetricCurrent.delete(self.configurations, current_metric)
                return False
            else:
                self.create_quality_check(f"{current_metric.expectation_name}_{current_metric.data_source_name}-{current_metric.data_asset_name}-{current_metric.column_name}", current_metric.data_quality_dimension_name,blindata_suite)
                return True

    def job_without_blindata(self, current_metrics, ERR_FAILED_BLINDATA_CHECK_CREATION=False, STATUS_CODE = 200):
        if current_metrics is not None and len(current_metrics) != 0:
            for current_metric in current_metrics:
                if not validate_business_domain_name(self.configurations, current_metric):
                    return

                if not validate_blindata_suite_name(self.configurations, current_metric):
                    return

                quality_check = self.get_quality_check(current_metric)

                if not quality_check:
                    quality_check = self.handle_quality_check_creation(current_metric, ERR_FAILED_BLINDATA_CHECK_CREATION)
                    if not quality_check:
                        return 

                validate_quality_result_upload(self.configurations, current_metric, STATUS_CODE)

    def lock_new_current_metric(self):
        return lock_new_current_metrics(self.configurations)

    def create_quality_suite(self, code):
        suite = {
            "code": code
        }
        self.quality_suites.append(suite)
        return suite

    def create_quality_check(self, code, dimension, suite):
        check = {
            "code": code,
            "warningThreshold": 95.0,
            "successThreshold": 100.0,
            "qualitySuite": suite,
            "scoreStrategy": "PERCENTAGE",
            "additionalProperties": [
                {
                    "name": "Data Quality Dimension",
                    "value": dimension
                }
            ]
        }
        self.quality_checks.append(check)
        return check