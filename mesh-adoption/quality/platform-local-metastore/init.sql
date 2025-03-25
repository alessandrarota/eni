IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'quality')
BEGIN
    CREATE DATABASE quality;
END
GO

USE quality;
GO

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'quality')
BEGIN
    CREATE DATABASE quality;
END
GO

USE quality;
GO

-- Creazione della tabella metric_current
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'metric_current')
BEGIN
    CREATE TABLE metric_current (
            data_product_name VARCHAR(255) NULL,
            check_name VARCHAR(255) NOT NULL,
            metric_value FLOAT NULL,
            unit_of_measure VARCHAR(8) NULL,
            expectation_checked_elements_nbr INT NULL,
            expectation_output_errors_nbr INT NULL,
            expectation_output_metric_val FLOAT NULL,
            metric_source_name VARCHAR(255) NULL,
            status_code VARCHAR(255) NOT NULL,
            locking_service_code VARCHAR(255) NULL,
            otlp_sending_datetime_code VARCHAR(14) NOT NULL,
            otlp_sending_datetime VARCHAR(30) NOT NULL,
            insert_datetime VARCHAR(30) NOT NULL,
            update_datetime VARCHAR(30) NOT NULL,
            CONSTRAINT PK_metric_current PRIMARY KEY (check_name, otlp_sending_datetime_code)
        );
END
GO

-- Creazione della tabella metric_history
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'metric_history')
BEGIN
    CREATE TABLE metric_history (
        data_product_name VARCHAR(255) NULL,
        check_name VARCHAR(255) NOT NULL,
        metric_value FLOAT NULL,
        unit_of_measure VARCHAR(8) NULL,
        expectation_checked_elements_nbr INT NULL,
        expectation_output_errors_nbr INT NULL,
        expectation_output_metric_val FLOAT NULL,
        metric_source_name VARCHAR(255) NULL,
        status_code VARCHAR(255) NOT NULL,
        locking_service_code VARCHAR(255) NULL,
        otlp_sending_datetime_code VARCHAR(14) NOT NULL,
        otlp_sending_datetime VARCHAR(30) NOT NULL,
        insert_datetime VARCHAR(30) NOT NULL,
        update_datetime VARCHAR(30) NOT NULL,
        CONSTRAINT PK_metric_history PRIMARY KEY (check_name, otlp_sending_datetime_code)
    );
END
GO
