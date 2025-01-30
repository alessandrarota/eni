from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

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
                return all_history_metrics
            except SQLAlchemyError as e:
                session.rollback()
                raise e

    @staticmethod
    def save_history_metrics(configurations, history_metrics):
        with configurations.SESSION_MAKER() as session:
            try:
                for metric in history_metrics:
                    existing_metric = session.query(MetricHistory).filter_by(
                        data_product_name=metric.data_product_name,
                        app_name=metric.app_name,
                        expectation_name=metric.expectation_name,
                        data_source_name=metric.data_source_name,
                        data_asset_name=metric.data_asset_name,
                        column_name=metric.column_name,
                        otlp_sending_datetime=metric.otlp_sending_datetime
                    ).first()
                    
                    if not existing_metric:
                        session.add(metric)
                    else:
                        logging.info(f"Metric already exists: {metric.data_product_name} - {metric.app_name} - {metric.metric_name} - {metric.timestamp}")
                
                session.commit()
                logging.info(f"{len(history_metrics)} metrics saved into {MetricHistory.__tablename__} successfully.")
        
            except SQLAlchemyError as e:
                session.rollback()
                logging.error(f"Error while saving metrics into {MetricHistory.__tablename__}: {e}")
                raise e
    



