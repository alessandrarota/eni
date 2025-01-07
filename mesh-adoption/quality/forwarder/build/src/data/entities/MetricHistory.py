from sqlalchemy import Column, String, DateTime, Float, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy
import logging

Base = declarative_base()

class MetricHistory(Base):
    __tablename__ = 'metric_history'
    
    data_product_name = Column(String, primary_key=True)
    app_name = Column(String, primary_key=True)
    expectation_name = Column(String, primary_key=True)
    metric_name = Column(String, primary_key=True)
    metric_description = Column(String)
    value = Column(Float)
    unit_of_measure = Column(String)
    element_count = Column(Integer)
    unexpected_count = Column(Integer)
    timestamp = Column(String, primary_key=True)
    insert_datetime = Column(String)
    flow_name = Column(String)

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
                        metric_name=metric.metric_name,
                        timestamp=metric.timestamp
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



