import re
import logging

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def clean_df_columns(df):
    try:
        cleaned_columns = [clean_column(col) for col in df.columns]
        return df.toDF(*cleaned_columns)
    except Exception as e:
        logger.error(f"Error while cleaning Spark DataFrame columns: {e}")
        raise

def clean_column(column_name):
    try:
        return re.sub(r'\W+', '_', column_name).replace(" ", "_").upper()
    except Exception as e:
        logger.error(f"Error while cleaning column name '{column_name}': {e}")
        raise

def clean_kwargs_columns(kwargs):
    try:
        for key, value in kwargs.items():
            if key.lower().startswith("column"): 
                if isinstance(value, str):
                    kwargs[key] = clean_column(value)
                elif isinstance(value, list):
                    kwargs[key] = [clean_column(val) for val in value]
        return kwargs
    except Exception as e:
        logger.error(f"Error while cleaning column names in kwargs: {e}")
        raise