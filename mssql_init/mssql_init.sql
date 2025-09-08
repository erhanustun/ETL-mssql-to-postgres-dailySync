-- Creating Database
IF NOT EXISTS (SELECT name FROM master.sys.databases WHERE name = 'source_db')
BEGIN
    CREATE DATABASE source_db;
END
GO

-- Enabling required permissions (for BULK INSERT)
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'Ad Hoc Distributed Queries', 1;
RECONFIGURE;
EXEC sp_configure 'xp_cmdshell', 1;
RECONFIGURE;
GO

-- Switching to the database
USE source_db;
GO

-- Creating orders table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.orders (
        OrderID BIGINT PRIMARY KEY,
        UserID BIGINT,
        AddedToCartAt DATETIME,  -- M/D/YYYY H:MM formatına uygun DATETIME tipi
        OrderCreatedAt DATETIME, -- M/D/YYYY H:MM formatına uygun DATETIME tipi
        Amount DECIMAL(18, 4),
        Product NVARCHAR(255),
        IsDelivered BIT
    );
    PRINT 'Table dbo.orders created.';
END
ELSE
BEGIN
    PRINT 'Table dbo.orders already exists.';
END
GO

-- Creating orders_staging table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders_staging' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.orders_staging (
        OrderID BIGINT,
        UserID BIGINT,
        AddedToCartAt DATETIME,
        OrderCreatedAt DATETIME,
        Amount DECIMAL(18, 4),
        Product NVARCHAR(255),
        IsDelivered BIT
    );
    PRINT 'Table dbo.orders_staging created.';
END
ELSE
BEGIN
    PRINT 'Table dbo.orders_staging already exists.';
END
GO