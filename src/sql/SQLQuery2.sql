-- Run against the OrderTest database.
IF OBJECT_ID('dbo.Orders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Orders
    (
        OrderID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Orders PRIMARY KEY,
        OrderNumber VARCHAR(30) NOT NULL,
        CustomerID INT NOT NULL,
        NationalCode VARCHAR(10) NOT NULL,
        OrderDate DATETIME2 NOT NULL,
        TotalAmount DECIMAL(18,2) NOT NULL,
        OrderStatus VARCHAR(20) NOT NULL,
        PaymentMethod VARCHAR(30) NOT NULL,
        City NVARCHAR(100) NOT NULL,
        IsPaid BIT NOT NULL,
        CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_Orders_CreatedAt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_Orders_OrderNumber UNIQUE (OrderNumber)
    );
END;
GO

IF OBJECT_ID('dbo.Outbox', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Outbox
    (
        EventId UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_Outbox PRIMARY KEY,
        AggregateKey VARCHAR(30) NOT NULL,
        EventType VARCHAR(100) NOT NULL,
        Payload NVARCHAR(MAX) NOT NULL,
        CreatedAt DATETIME2 NOT NULL CONSTRAINT DF_Outbox_CreatedAt DEFAULT SYSUTCDATETIME(),
        PublishedAt DATETIME2 NULL,
        RetryCount INT NOT NULL CONSTRAINT DF_Outbox_RetryCount DEFAULT 0,
        LastError NVARCHAR(2000) NULL
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Outbox_Pending' AND object_id = OBJECT_ID('dbo.Outbox'))
    CREATE INDEX IX_Outbox_Pending ON dbo.Outbox(CreatedAt, EventId) WHERE PublishedAt IS NULL;
GO

IF OBJECT_ID('dbo.NationalCodePrefixes', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.NationalCodePrefixes
    (
        Id INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_NationalCodePrefixes PRIMARY KEY,
        Prefix VARCHAR(9) NOT NULL,
        IsActive BIT NOT NULL CONSTRAINT DF_NationalCodePrefixes_IsActive DEFAULT 1
    );
END;
GO

-- Illustrative values for a test environment only; replace with approved test prefixes.
IF NOT EXISTS (SELECT 1 FROM dbo.NationalCodePrefixes)
    INSERT INTO dbo.NationalCodePrefixes(Prefix) VALUES ('123'), ('4567'), ('982'), ('76543');
GO
