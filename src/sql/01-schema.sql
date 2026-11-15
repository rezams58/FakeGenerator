IF OBJECT_ID('dbo.Orders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Orders
    (
        OrderId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        SequenceNo BIGINT NOT NULL,
        OrderNumber NVARCHAR(40) NOT NULL,
        CustomerId NVARCHAR(40) NOT NULL,
        Amount DECIMAL(18,2) NOT NULL,
        CreatedAtUtc DATETIME2(3) NOT NULL
    );
    CREATE UNIQUE INDEX UX_Orders_SequenceNo ON dbo.Orders(SequenceNo);
END;
GO

IF OBJECT_ID('dbo.Outbox', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Outbox
    (
        EventId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        SequenceNo BIGINT NOT NULL,
        AggregateKey NVARCHAR(40) NOT NULL,
        KafkaPartition INT NOT NULL,
        EventType NVARCHAR(100) NOT NULL,
        Payload NVARCHAR(MAX) NOT NULL,
        CreatedAtUtc DATETIME2(3) NOT NULL,
        PublishedAtUtc DATETIME2(3) NULL,
        DeadLetterAtUtc DATETIME2(3) NULL,
        RetryCount INT NOT NULL CONSTRAINT DF_Outbox_RetryCount DEFAULT 0,
        LastError NVARCHAR(2000) NULL
    );
    CREATE UNIQUE INDEX UX_Outbox_SequenceNo ON dbo.Outbox(SequenceNo);
    CREATE INDEX IX_Outbox_PendingPartitionSequence
        ON dbo.Outbox(KafkaPartition, SequenceNo)
        INCLUDE (EventId, AggregateKey, EventType, RetryCount)
        WHERE PublishedAtUtc IS NULL AND DeadLetterAtUtc IS NULL;
END;
GO
