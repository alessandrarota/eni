from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime, timezone
from sqlalchemy.orm import class_mapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
Base = declarative_base()

class MetricHistory(Base):
    __tablename__ = 'metric_history'

    data_product_name = Column(String)
    check_name = Column(String, primary_key=True)
    metric_value = Column(Float)
    unit_of_measure = Column(String)
    checked_elements_nbr = Column(Integer)
    errors_nbr = Column(Integer)
    metric_source_name = Column(String)
    status_code = Column(String)
    locking_service_code = Column(String)
    otlp_sending_datetime_code = Column(String, primary_key=True)
    otlp_sending_datetime = Column(String)
    insert_datetime = Column(String)
    update_datetime = Column(String)

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
            data_product_name=current_metric.data_product_name,
            check_name=current_metric.check_name,
            metric_value=current_metric.metric_value,
            unit_of_measure=current_metric.unit_of_measure,
            checked_elements_nbr=current_metric.checked_elements_nbr,
            errors_nbr=current_metric.errors_nbr,
            metric_source_name=current_metric.metric_source_name,
            status_code=status_code,
            locking_service_code=current_metric.locking_service_code,
            otlp_sending_datetime_code=current_metric.otlp_sending_datetime_code,
            otlp_sending_datetime=current_metric.otlp_sending_datetime,
            insert_datetime=now_utc,
            update_datetime=now_utc
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
            f"MetricHistory(data_product_name={self.data_product_name}, "
            f"check_name={self.check_name}, "
            f"metric_value={self.metric_value}, "
            f"unit_of_measure={self.unit_of_measure}, "
            f"checked_elements_nbr={self.checked_elements_nbr}, "
            f"errors_nbr={self.errors_nbr}, "
            f"metric_source_name={self.metric_source_name}, "
            f"status_code={self.status_code}, "
            f"locking_service_code={self.locking_service_code}, "
            f"otlp_sending_datetime_code={self.otlp_sending_datetime_code}, "
            f"otlp_sending_datetime={self.otlp_sending_datetime}, "
            f"insert_datetime={self.insert_datetime}, "
            f"update_datetime={self.update_datetime})"
        )

