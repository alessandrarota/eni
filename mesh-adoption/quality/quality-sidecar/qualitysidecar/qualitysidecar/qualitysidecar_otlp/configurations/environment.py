class BaseConfig(object):
    OTEL_EXPORTER_OTLP_PROTOCOL = "http/protobuf"

class DevelopmentConfig(BaseConfig):
    OTEL_EXPORTER_OTLP_ENDPOINT = "https://api-app.eni.com/sd-xops/otel-collector/"

class TestingConfig(BaseConfig):
    OTEL_EXPORTER_OTLP_ENDPOINT = "https://api-app.eni.com/st-xops/otel-collector/"

class ProductionConfig(BaseConfig):
    OTEL_EXPORTER_OTLP_ENDPOINT = "https://api-app.eni.com/pr-xops/otel-collector/"