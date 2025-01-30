IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'quality')
BEGIN
    CREATE DATABASE quality;
END
GO

USE quality;
GO

-- -- Creazione della tabella metric_current
-- IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'metric_current')
-- BEGIN
--     CREATE TABLE metric_current (
--         data_product_name NVARCHAR(255) NOT NULL,
--         app_name NVARCHAR(255) NOT NULL,
--         metric_name NVARCHAR(255) NOT NULL,
--         metric_description NVARCHAR(MAX) NULL,
--         value FLOAT NULL,
--         unit_of_measure NVARCHAR(255) NULL,
--         timestamp NVARCHAR(255) NOT NULL,
--         CONSTRAINT PK_metric_current PRIMARY KEY (data_product_name, app_name, metric_name, timestamp)
--     );
-- END
-- GO

-- -- Creazione della tabella metric_history
-- IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'metric_history')
-- BEGIN
--     CREATE TABLE metric_history (
--         data_product_name NVARCHAR(255) NOT NULL,
--         app_name NVARCHAR(255) NOT NULL,
--         metric_name NVARCHAR(255) NOT NULL,
--         metric_description NVARCHAR(MAX) NULL,
--         value FLOAT NULL,
--         unit_of_measure NVARCHAR(255) NULL,
--         timestamp NVARCHAR(255) NOT NULL,
--         insert_datetime NVARCHAR(255) NOT NULL,
--         flow_name NVARCHAR(255) NULL,
--         CONSTRAINT PK_metric_history PRIMARY KEY (data_product_name, app_name, metric_name, timestamp)
--     );
-- END
-- GO

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
            business_domain_name VARCHAR(255) NULL,
            data_product_name VARCHAR(128) NOT NULL,
            expectation_name VARCHAR(128) NOT NULL,
            data_source_name VARCHAR(128) NOT NULL,
            data_asset_name VARCHAR(128) NOT NULL,
            column_name VARCHAR(128) NOT NULL,
            blindata_suite_name VARCHAR(128) NULL,
            gx_suite_name VARCHAR(128) NOT NULL,
            metric_value FLOAT NULL,
            unit_of_measure VARCHAR(8) NULL,
            checked_elements_nbr INT NULL,
            errors_nbr INT NULL,
            app_name VARCHAR(128) NOT NULL,
            otlp_sending_datetime VARCHAR(30)  NOT NULL,
            status_code VARCHAR(64) NOT NULL,
            locking_service_code VARCHAR(128) NULL,
            insert_datetime VARCHAR(30) NOT NULL,
            update_datetime VARCHAR(30) NOT NULL,
            CONSTRAINT PK_metric_current PRIMARY KEY (data_product_name, app_name, expectation_name, data_source_name, data_asset_name, column_name, otlp_sending_datetime)
        
        );
END
GO

-- Creazione della tabella metric_history
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'metric_history')
BEGIN
    CREATE TABLE metric_history (
            business_domain_name VARCHAR(255) NULL,
            data_product_name VARCHAR(128) NOT NULL,
            expectation_name VARCHAR(128) NOT NULL,
            data_source_name VARCHAR(128) NOT NULL,
            data_asset_name VARCHAR(128) NOT NULL,
            column_name VARCHAR(128) NOT NULL,
            blindata_suite_name VARCHAR(128) NULL,
            gx_suite_name VARCHAR(128) NOT NULL,
            metric_value FLOAT NULL,
            unit_of_measure VARCHAR(8) NULL,
            checked_elements_nbr INT NULL,
            errors_nbr INT NULL,
            app_name VARCHAR(128) NOT NULL,
            otlp_sending_datetime VARCHAR(30)  NOT NULL,
            status_code VARCHAR(64) NOT NULL,
            source_service_code VARCHAR(128) NULL,
            insert_datetime VARCHAR(30) NOT NULL,
            CONSTRAINT PK_metric_history PRIMARY KEY (data_product_name, app_name, expectation_name, data_source_name, data_asset_name, column_name, otlp_sending_datetime)
        
        );
END
GO
