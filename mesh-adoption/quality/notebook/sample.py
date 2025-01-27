# export OTEL_METRIC_EXPORT_INTERVAL=10000
# export OTEL_LOG_LEVEL=debug
# export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
# opentelemetry-instrument --metrics_exporter otlp,console --logs_exporter otlp,console --service_name test python app.py
## opentelemetry-instrument --traces_exporter otlp,console --metrics_exporter otlp,console --logs_exporter otlp,console --service_name test python app.py

import logging

from random import random
from time import sleep
from randomname import get_name

from opentelemetry import trace
from opentelemetry import metrics
from opentelemetry.metrics import Observation


tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)

GENERATED_OBJECTS = 0

def cpu_usage_callback(options):
    return [
        Observation(value=get_cpu_usage(), attributes={"data-product-name": "consuntiviDiProduzione", "app-name": "appTest"}),
        # Observation(value=get_cpu_usage(), attributes={"core": "core_1"}),
        # Observation(value=get_cpu_usage(), attributes={"core": "core_2"}),
        # Observation(value=get_cpu_usage(), attributes={"core": "core_3"}),
    ]

cpu_usage_metric = meter.create_observable_gauge(
    name="cpu_usage_percentage",
    description="CPU Usage Percentage",
    unit="%",
    callbacks=[cpu_usage_callback],
)

def objects_count_callback(options):
    return [
        Observation(value=get_obj_count(), attributes={"data-product-name": "consuntiviDiProduzione", "app-name": "appTest"}),
    ]

roll_counter_metric = meter.create_observable_counter(
    name="objects_generated_total",
    description="Total number of objects generated",
    unit="",
    callbacks=[objects_count_callback],
)

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_cpu_usage():
    logger.info('Retrieved CPU usage.')
    return round(random() * 100, 2)


def get_obj_count():
    logger.info('Retrieved objects count.')
    global GENERATED_OBJECTS
    return GENERATED_OBJECTS


def get_obj():
    global GENERATED_OBJECTS
    GENERATED_OBJECTS += 1
    with tracer.start_as_current_span("roll") as rollspan:
        current_obj = {'name': get_name(), 'age': int(random() * 100 % 90)}
        logger.info('Generated new object.')
        rollspan.set_attribute("roll.name", current_obj['name'])
        rollspan.set_attribute("roll.age", current_obj['age'])
        return current_obj


def main():
    # while(True):
    _ = get_obj()
        # sleep_sec = random() * 10
        # logger.info(f'Sleeping for {sleep_sec} seconds.')
        # sleep(sleep_sec)


if __name__ == "__main__":
    logger.info('Script started.')
    main()