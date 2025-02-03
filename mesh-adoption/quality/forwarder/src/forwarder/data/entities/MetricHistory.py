from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime, timezone
from sqlalchemy.orm import class_mapper

logging.basicConfig(level=logging.INFO)
Base = declarative_base()

class MetricHistory(Base):
    __tablename__ = 'metric_history'
    
    business_domain_name = Column(String)
    data_product_name = Column(String, primary_key=True)
    expectation_name = Column(String, primary_key=True)
    data_source_name = Column(String, primary_key=True)
    data_asset_name = Column(String, primary_key=True)
    column_name = Column(String, primary_key=True)
    blindata_suite_name = Column(String)
    gx_suite_name = Column(String)
    data_quality_dimension_name = Column(String)
    metric_value = Column(Float)
    unit_of_measure = Column(String)
    checked_elements_nbr = Column(Integer)
    errors_nbr = Column(Integer)
    app_name = Column(String, primary_key=True)
    otlp_sending_datetime = Column(String, primary_key=True)
    status_code = Column(String)
    insert_datetime = Column(String)
    source_service_code = Column(String)

    @staticmethod
    def get_all_history_metrics(configurations):
        with configurations.SESSION_MAKER() as session:
            try:
                all_history_metrics = session.query(MetricHistory).all()
                if not all_history_metrics:
                    logging.warning(f"No history metrics found in {MetricHistory.__tablename__}.")
                return all_history_metrics
            except SQLAlchemyError as e:
                logging.error(f"SQLAlchemyError while retrieving history metrics from {MetricHistory.__tablename__}: {str(e)}")
                session.rollback()
                raise
            except Exception as e:
                logging.error(f"Unexpected error while retrieving history metrics: {str(e)}")
                raise

    @staticmethod
    def save(configurations, current_metric, status_code):
        now_utc = datetime.now(timezone.utc)
        metric_history = MetricHistory(
            business_domain_name = current_metric.business_domain_name,
            data_product_name = current_metric.data_product_name,
            expectation_name = current_metric.expectation_name,
            data_source_name = current_metric.data_source_name,
            data_asset_name = current_metric.data_asset_name,
            column_name = current_metric.column_name,
            blindata_suite_name = current_metric.blindata_suite_name,
            gx_suite_name = current_metric.gx_suite_name,
            data_quality_dimension_name = current_metric.data_quality_dimension_name,
            metric_value = current_metric.metric_value,
            unit_of_measure = current_metric.unit_of_measure,
            checked_elements_nbr = current_metric.checked_elements_nbr,
            errors_nbr = current_metric.errors_nbr,
            app_name = current_metric.app_name,
            otlp_sending_datetime = current_metric.otlp_sending_datetime,
            status_code = status_code,
            insert_datetime = now_utc,
            source_service_code = current_metric.locking_service_code
        )

        with configurations.SESSION_MAKER() as session:
            try:
                session.add(metric_history)
                session.commit()

                primary_key = {column.name: getattr(metric_history, column.name) for column in class_mapper(MetricHistory).primary_key}
                logging.info(f"Metric with {primary_key} saved into {MetricHistory.__tablename__} successfully.")
            except SQLAlchemyError as e:
                logging.error(f"SQLAlchemyError while saving metric into {MetricHistory.__tablename__}: {str(e)}")
                session.rollback()
                raise
            except Exception as e:
                logging.error(f"Unexpected error while saving metric into {MetricHistory.__tablename__}: {str(e)}")
                raise

    def __str__(self):
        return (
            f"MetricHistory(business_domain_name={self.business_domain_name}, "
            f"data_product_name={self.data_product_name}, "
            f"expectation_name={self.expectation_name}, "
            f"data_source_name={self.data_source_name}, "
            f"data_asset_name={self.data_asset_name}, "
            f"column_name={self.column_name}, "
            f"blindata_suite_name={self.blindata_suite_name}, "
            f"gx_suite_name={self.gx_suite_name}, "
            f"data_quality_dimension_name={self.data_quality_dimension_name}, "
            f"metric_value={self.metric_value}, "
            f"unit_of_measure={self.unit_of_measure}, "
            f"checked_elements_nbr={self.checked_elements_nbr}, "
            f"errors_nbr={self.errors_nbr}, "
            f"app_name={self.app_name}, "
            f"otlp_sending_datetime={self.otlp_sending_datetime}, "
            f"status_code={self.status_code}, "
            f"insert_datetime={self.insert_datetime}, "
            f"source_service_code={self.source_service_code})"
        )

