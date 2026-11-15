DECLARE @RecordCount INT;

-- تعداد تصادفی بین 100000 و 500000
SET @RecordCount = 100 +     ABS(CHECKSUM(NEWID())) % 401;

select @RecordCount

;WITH Numbers AS
(
    SELECT TOP (@RecordCount)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS N
    FROM sys.all_objects A
    CROSS JOIN sys.all_objects B
),
RandomData AS
(
    SELECT
        N,

        ABS(CHECKSUM(NEWID())) % 100000 + 1 AS CustomerID,

        DATEADD(
            DAY,
            -ABS(CHECKSUM(NEWID())) % 730,
            GETDATE()
        ) AS OrderDate,

        CAST(
            10000 + ABS(CHECKSUM(NEWID())) % 50000000
            AS DECIMAL(18,2)
        ) / 100 AS TotalAmount,

        ABS(CHECKSUM(NEWID())) % 4 AS StatusID,

        ABS(CHECKSUM(NEWID())) % 4 AS PaymentID,

        ABS(CHECKSUM(NEWID())) % 8 AS CityID,

        ABS(CHECKSUM(NEWID())) % 2 AS IsPaid

    FROM Numbers
)
select * from RandomData