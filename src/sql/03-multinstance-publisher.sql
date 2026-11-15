/*
  Apply after 01-schema.sql.
  Creates the SQL lease table used to coordinate publisher instances.
  SQL Server time is authoritative for lease expiry.
*/
IF COL_LENGTH('dbo.Outbox', 'DeadLetterAtUtc') IS NULL
    ALTER TABLE dbo.Outbox ADD DeadLetterAtUtc DATETIME2(3) NULL;
GO

IF OBJECT_ID('dbo.PublisherPartitionLease', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.PublisherPartitionLease
    (
        PartitionId INT NOT NULL CONSTRAINT PK_PublisherPartitionLease PRIMARY KEY,
        OwnerId UNIQUEIDENTIFIER NULL,
        LeaseUntilUtc DATETIME2(3) NOT NULL,
        FencingToken BIGINT NOT NULL CONSTRAINT DF_PublisherPartitionLease_FencingToken DEFAULT 0,
        UpdatedAtUtc DATETIME2(3) NOT NULL CONSTRAINT DF_PublisherPartitionLease_UpdatedAtUtc DEFAULT SYSUTCDATETIME(),
        CONSTRAINT CK_PublisherPartitionLease_PartitionId CHECK (PartitionId >= 0)
    );
END;
GO

/* Six configured partitions: 0..5. Keep this in sync with Kafka:PartitionCount. */
;WITH ids AS
(
    SELECT 0 AS PartitionId UNION ALL SELECT 1 UNION ALL SELECT 2
    UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
)
INSERT INTO dbo.PublisherPartitionLease (PartitionId, OwnerId, LeaseUntilUtc, FencingToken)
SELECT i.PartitionId, NULL, '2000-01-01T00:00:00', 0
FROM ids i
WHERE NOT EXISTS
(
    SELECT 1 FROM dbo.PublisherPartitionLease l WHERE l.PartitionId = i.PartitionId
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Outbox') AND name = 'IX_Outbox_PendingPartitionSequence')
    CREATE INDEX IX_Outbox_PendingPartitionSequence
        ON dbo.Outbox(KafkaPartition, SequenceNo)
        INCLUDE (EventId, AggregateKey, EventType, RetryCount)
        WHERE PublishedAtUtc IS NULL AND DeadLetterAtUtc IS NULL;
GO
