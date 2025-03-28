from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime, timezone
from sqlalchemy.orm import class_mapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
Base = declarative_base()

class MetricCurrent(Base):
    __tablename__ = 'metric_current'
    
    data_product_name = Column(String)
    check_name = Column(String, primary_key=True)
    metric_value = Column(Float)
    unit_of_measure = Column(String)
    expectation_checked_elements_nbr = Column(Integer)
    expectation_output_errors_nbr = Column(Integer)
    expectation_output_metric_val = Column(Float)
    metric_source_name = Column(String)
    status_code = Column(String)
    locking_service_code = Column(String)
    otlp_sending_datetime_code = Column(String, primary_key=True)
    otlp_sending_datetime = Column(String)
    insert_datetime = Column(String)
    update_datetime = Column(String)

    @staticmethod
    def get_all_current_metrics(configurations):
        with configurations.SESSION_MAKER() as session:
            try:
                metrics = session.query(MetricCurrent).all()
                if not metrics:
                    logging.warning(f"No metrics found in {MetricCurrent.__tablename__}.")
                return metrics
            except SQLAlchemyError as e:
                logging.error(f"SQLAlchemyError while retrieving metrics from {MetricCurrent.__tablename__}: {str(e)}")
                session.rollback()
                raise

    @staticmethod
    def lock_new_current_metrics(configurations, hostname):
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + str(int(datetime.now(timezone.utc).microsecond / 1000)).zfill(3)
        locking_service_code = f"{configurations.ENVIRONMENT}-{hostname}-{timestamp}"

        with configurations.SESSION_MAKER() as session:
            try:
                updated = session.query(MetricCurrent).filter(MetricCurrent.status_code == 'NEW').update({
                    'status_code': 'LOCKED',
                    'locking_service_code': locking_service_code,
                    'update_datetime': datetime.now(timezone.utc)
                })
                if updated == 0:
                    logging.warning(f"No records found with 'NEW' status to lock.")
                session.commit()

                locked_current_metrics = session.query(MetricCurrent).filter(
                    (MetricCurrent.status_code == 'LOCKED') & (MetricCurrent.locking_service_code == locking_service_code)
                ).all()
                
                return locked_current_metrics
            except SQLAlchemyError as e:
                logging.error(f"SQLAlchemyError while locking new metrics: {str(e)}")
                session.rollback()
                raise

    @staticmethod
    def delete(configurations, metric):
        with configurations.SESSION_MAKER() as session:
            try:  
                metric_to_delete = session.query(MetricCurrent).filter_by(
                    check_name=metric.check_name,
                    otlp_sending_datetime_code=metric.otlp_sending_datetime_code
                ).first()
                
                if metric_to_delete:
                    session.delete(metric_to_delete)
                    session.commit()

                    primary_key = {column.name: getattr(metric_to_delete, column.name) for column in class_mapper(MetricCurrent).primary_key}
                    logging.info(f"Metric with {primary_key} deleted from {MetricCurrent.__tablename__} successfully.")
                else:
                    primary_key = {column.name: getattr(metric_to_delete, column.name) for column in class_mapper(MetricCurrent).primary_key}
                    logging.warning(f"Metric with {primary_key} not found for deletion from {MetricCurrent.__tablename__}.")

            except SQLAlchemyError as e:
                logging.error(f"SQLAlchemyError while deleting metric: {str(e)}")
                session.rollback()
                raise
            except Exception as e:
                logging.error(f"Unexpected error while deleting metric: {str(e)}")
                raise

    def __str__(self):
        return (
            f"MetricCurrent(data_product_name={self.data_product_name}, "
            f"check_name={self.check_name}, "
            f"metric_value={self.metric_value}, "
            f"unit_of_measure={self.unit_of_measure}, "
            f"expectation_checked_elements_nbr={self.expectation_checked_elements_nbr}, "
            f"expectation_output_errors_nbr={self.expectation_output_errors_nbr}, "
            f"expectation_output_metric_val={self.expectation_output_metric_val}, "
            f"metric_source_name={self.metric_source_name}, "
            f"status_code={self.status_code}, "
            f"locking_service_code={self.locking_service_code}, "
            f"otlp_sending_datetime_code={self.otlp_sending_datetime_code}, "
            f"otlp_sending_datetime={self.otlp_sending_datetime}, "
            f"insert_datetime={self.insert_datetime}, "
            f"update_datetime={self.update_datetime})"
        )