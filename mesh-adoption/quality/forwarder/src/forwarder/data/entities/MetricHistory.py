from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
Base = declarative_base()

class MetricHistory(Base):
    __tablename__ = 'metric_history'
    
    data_product_name = Column(String, primary_key=True)
    app_name = Column(String, primary_key=True)
    expectation_name = Column(String, primary_key=True)
    metric_name = Column(String, primary_key=True)
    metric_description = Column(String)
    metric_value = Column(Float)
    unit_of_measure = Column(String)
    element_count = Column(Integer)
    unexpected_count = Column(Integer)
    timestamp = Column(String, primary_key=True)
    data_source_name = Column(String, primary_key=True)
    data_asset_name = Column(String, primary_key=True)
    column_name = Column(String, primary_key=True)
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
                        timestamp=metric.timestamp,
                        data_source_name=metric.data_source_name,
                        data_asset_name=metric.data_asset_name,
                        column_name=metric.column_name
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
    
    def __repr__(self):
        return f"<MetricHistory(data_product_name={self.data_product_name}, app_name={self.app_name}, " \
               f"expectation_name={self.expectation_name}, metric_name={self.metric_name}, " \
               f"metric_description={self.metric_description}, metric_value={self.metric_value}, " \
               f"unit_of_measure={self.unit_of_measure}, element_count={self.element_count}, " \
               f"unexpected_count={self.unexpected_count}, timestamp={self.timestamp}, " \
               f"data_source_name={self.data_source_name}, data_asset_name={self.data_asset_name}, " \
               f"column_name={self.column_name}, insert_datetime={self.insert_datetime}, " \
               f"flow_name={self.flow_name})>"



