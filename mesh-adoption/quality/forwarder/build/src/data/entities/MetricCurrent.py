from sqlalchemy import Column, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy
import logging

Base = declarative_base()

class MetricCurrent(Base):
    __tablename__ = 'metric_current'
    
    data_product_name = Column(String, primary_key=True)
    app_name = Column(String, primary_key=True)
    metric_name = Column(String, primary_key=True)
    metric_description = Column(String)
    value = Column(Float)
    unit_of_measure = Column(String)
    timestamp = Column(String, primary_key=True)

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
                try:                    
                    metric_to_delete = session.query(MetricCurrent).filter_by(
                        data_product_name=metric.data_product_name,
                        app_name=metric.app_name,
                        metric_name=metric.metric_name,
                        timestamp=metric.timestamp
                    ).first()
                    
                    if metric_to_delete:
                        session.delete(metric_to_delete)
                        session.commit()
                    else:
                        logging.warning(f"Metric {metric['metric_name']} not found for deletion.")

                except SQLAlchemyError as e:
                    session.rollback()
                    logging.error(f"Error while deleting metric: {e}")
                    raise e
                
        logging.info(f"{len(current_metrics)} metrics deleted from {MetricCurrent.__tablename__} successfully.")
