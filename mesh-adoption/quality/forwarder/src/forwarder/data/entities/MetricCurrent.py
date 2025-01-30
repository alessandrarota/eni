from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
Base = declarative_base()

class MetricCurrent(Base):
    __tablename__ = 'metric_current'
    
    business_domain_name = Column(String)
    data_product_name = Column(String, primary_key=True)
    expectation_name = Column(String, primary_key=True)
    data_source_name = Column(String, primary_key=True)
    data_asset_name = Column(String, primary_key=True)
    column_name = Column(String, primary_key=True)
    blindata_suite_name = Column(String)
    gx_suite_name = Column(String)
    metric_value = Column(Float)
    unit_of_measure = Column(String)
    checked_elements_nbr = Column(Integer)
    errors_nbr = Column(Integer)
    app_name = Column(String, primary_key=True)
    otlp_sending_datetime = Column(String, primary_key=True)
    status_code = Column(String)
    locking_service_code = Column(String)
    insert_datetime = Column(String)
    update_datetime = Column(String)

    @staticmethod
    def get_all_current_metrics(configurations):
        with configurations.SESSION_MAKER() as session:
            try:
                all_current_metrics = session.query(MetricCurrent).all()

                return all_current_metrics
            except SQLAlchemyError as e:
                session.rollback()
                raise e
    
    @staticmethod
    def delete_current_metrics(configurations, current_metrics):
        with configurations.SESSION_MAKER() as session:
            for metric in current_metrics:
                #logging.info(f"Metric Current Object: {metric}")
                try:                    
                    metric_to_delete = session.query(MetricCurrent).filter_by(
                        data_product_name=metric.data_product_name,
                        app_name=metric.app_name,
                        expectation_name=metric.expectation_name,
                        data_source_name=metric.data_source_name,
                        data_asset_name=metric.data_asset_name,
                        column_name=metric.column_name,
                        otlp_sending_datetime=metric.otlp_sending_datetime
                    ).first()

                    
                    if metric_to_delete:
                        session.delete(metric_to_delete)
                        session.commit()
                    else:
                        logging.warning(f"Metric {metric.metric_name} not found for deletion.")

                except SQLAlchemyError as e:
                    session.rollback()
                    logging.error(f"Error while deleting metric: {e}")
                    raise e
                
        logging.info(f"{len(current_metrics)} metrics deleted from {MetricCurrent.__tablename__} successfully.")