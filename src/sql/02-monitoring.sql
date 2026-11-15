SELECT COUNT_BIG(*) AS TotalOrders,
       MIN(CreatedAtUtc) AS FirstOrderDate,
       MAX(CreatedAtUtc) AS LastOrderDate,
       SUM(Amount) AS TotalSales,
       AVG(Amount) AS AverageOrderAmount
FROM dbo.Orders;

SELECT COUNT_BIG(*) AS TotalEvents,
       SUM(CASE WHEN PublishedAtUtc IS NOT NULL THEN 1 ELSE 0 END) AS PublishedEvents,
       SUM(CASE WHEN PublishedAtUtc IS NULL AND DeadLetterAtUtc IS NULL AND RetryCount = 0 THEN 1 ELSE 0 END) AS PendingEvents,
       SUM(CASE WHEN PublishedAtUtc IS NULL AND DeadLetterAtUtc IS NULL AND RetryCount > 0 THEN 1 ELSE 0 END) AS RetriedEvents,
       SUM(CASE WHEN DeadLetterAtUtc IS NOT NULL THEN 1 ELSE 0 END) AS DeadLetterEvents
FROM dbo.Outbox;

SELECT TOP (100) EventId, SequenceNo, KafkaPartition, AggregateKey, RetryCount,
       LastError, CreatedAtUtc, DeadLetterAtUtc
FROM dbo.Outbox
WHERE PublishedAtUtc IS NULL
ORDER BY KafkaPartition, SequenceNo;

SELECT PartitionId, OwnerId, LeaseUntilUtc, FencingToken, UpdatedAtUtc
FROM dbo.PublisherPartitionLease
ORDER BY PartitionId;
