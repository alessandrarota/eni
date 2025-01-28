from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
Base = declarative_base()

class MetricCurrent(Base):
    __tablename__ = 'metric_current'
    
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
                        metric_name=metric.metric_name,
                        timestamp=metric.timestamp,
                        data_source_name=metric.data_source_name,
                        data_asset_name=metric.data_asset_name,
                        column_name=metric.column_name
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

    def __repr__(self):
        return f"<MetricCurrent(data_product_name={self.data_product_name}, app_name={self.app_name}, " \
               f"expectation_name={self.expectation_name}, metric_name={self.metric_name}, " \
               f"metric_description={self.metric_description}, metric_value={self.metric_value}, " \
               f"unit_of_measure={self.unit_of_measure}, element_count={self.element_count}, " \
               f"unexpected_count={self.unexpected_count}, timestamp={self.timestamp}, " \
               f"data_source_name={self.data_source_name}, data_asset_name={self.data_asset_name}, " \
               f"column_name={self.column_name})>"