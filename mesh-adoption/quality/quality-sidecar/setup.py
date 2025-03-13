from setuptools import setup, find_packages

setup(
    name='qualitysidecar',
    version='1.0',
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        'great_expectations==1.3.5',
        'pyspark',
        'pandas',
        'opentelemetry-api',
        'opentelemetry-distro',
        'opentelemetry-exporter-otlp', 
        'opentelemetry-semantic-conventions',
        'opentelemetry-util-http',
        'wrapt',
        'opentelemetry-exporter-otlp-proto-http',
        'sh',
        'dotenv',
        'azure-storage-blob',
        'pyarrow',
        'fastparquet'
    ],
)