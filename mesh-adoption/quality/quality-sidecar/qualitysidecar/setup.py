from setuptools import setup, find_packages

setup(
    name='qualitysidecar',
    version='1.0',
    # packages=find_packages(where='qualitysidecar'),  # Specifica la directory di ricerca
    # package_dir={'': 'qualitysidecar'},
    #packages=['qualitysidecar'],
    packages=find_packages(),
    install_requires=[
        'great_expectations==1.3.5',
        'pyspark',
        'pandas',
        'opentelemetry-api',
        'opentelemetry-distro',
        'opentelemetry-exporter-otlp', 
        # 'opentelemetry-exporter-otlp-proto-grpc', 
        'opentelemetry-semantic-conventions',
        'opentelemetry-util-http',
        'wrapt',
        'opentelemetry-exporter-otlp-proto-http',
        'sh',
        'dotenv',
    ],
)