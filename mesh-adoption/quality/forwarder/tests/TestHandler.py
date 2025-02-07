import pytest
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from forwarder import init_configurations
from main import lock_new_metrics, process_quality_result_upload
from forwarder.data.entities.MetricCurrent import MetricCurrent
from forwarder.data.entities.MetricHistory import MetricHistory
from forwarder.data.enum.MetricStatusCode import MetricStatusCode
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import logging
import re

class TestHandler():
    def __init__(self):
        self.configurations = init_configurations("base")
        self.quality_checks = []
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
        session.query(MetricHistory).delete()
        session.query(MetricCurrent).delete()
        session.commit()
        session.close()
        self.configurations.engine.dispose()

    def destroy_checks(self):
        self.quality_checks.clear()

    def populate_metrics(self, data):
        session = self.configurations.SESSION_MAKER()
        for record in data:
            session.add(record)
        session.commit()

    def get_quality_check(self, current_metric):
        quality_check_code = f"{current_metric.check_name}"
    
        if any(quality_check_code == check['code'] for check in self.quality_checks):
            return True
        return False

    def job_without_blindata(self, current_metrics, STATUS_CODE = 201):
        print(len(current_metrics))
        if current_metrics is not None and len(current_metrics) != 0:
            for current_metric in current_metrics:
                quality_check = self.get_quality_check(current_metric)

                if not quality_check:
                    MetricHistory.save(self.configurations, current_metric, MetricStatusCode.ERR_CHECK_NOT_FOUND.value)
                    MetricCurrent.delete(self.configurations, current_metric)
                    return

                process_quality_result_upload(self.configurations, current_metric, STATUS_CODE)

    def lock_new_current_metric(self):
        return lock_new_metrics(self.configurations)

    def create_quality_check(self, code):
        check = {
            "code": code
        }
        self.quality_checks.append(check)
        return check