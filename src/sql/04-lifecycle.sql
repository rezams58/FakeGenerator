/*
  Apply after 01-schema.sql and 03-multinstance-publisher.sql. Idempotent.
  Adds order lifecycle columns used by the lifecycle generator.
*/
SET QUOTED_IDENTIFIER ON;
GO

IF COL_LENGTH('dbo.Orders', 'Status') IS NULL
    ALTER TABLE dbo.Orders ADD Status NVARCHAR(30) NOT NULL
        CONSTRAINT DF_Orders_Status DEFAULT 'Created';
GO

IF COL_LENGTH('dbo.Orders', 'LastEventSequenceNo') IS NULL
    ALTER TABLE dbo.Orders ADD LastEventSequenceNo BIGINT NULL;
GO

IF COL_LENGTH('dbo.Orders', 'UpdatedAtUtc') IS NULL
    ALTER TABLE dbo.Orders ADD UpdatedAtUtc DATETIME2(3) NULL;
GO

/* Non-terminal states only (Delivered and Cancelled are terminal), so the active-order lookup stays small. */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Orders') AND name = 'IX_Orders_ActiveStatus')
    CREATE INDEX IX_Orders_ActiveStatus
        ON dbo.Orders(Status, OrderId)
        WHERE Status IN ('Created', 'Paid', 'Shipped');
GO
