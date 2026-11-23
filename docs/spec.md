# SPEC / ENGINEERING ISSUE
# FakeOrderGenerator — Multi-Instance Kafka Publisher

**نوع:** Engineering Issue / Implementation Specification  
**اولویت:** High  
**وضعیت:** Implementation Candidate — Verification Pending  
**پلتفرم:** .NET 8, SQL Server, Apache Kafka  
**نسخه مبنا:** `FakeGenerator_multinstance_fixed.zip`

---

## 1. هدف پروژه

هدف، تکمیل و پایدارسازی Publisher پروژه FakeOrderGenerator است تا بتواند حجم بالایی از سفارش‌های تولیدشده را از طریق SQL Server Outbox به Kafka منتشر کند.

سیستم باید از اجرای هم‌زمان چند Instance پشتیبانی کند، بدون اینکه دو Instance به‌طور عادی مالک یک Partition باشند یا پیام‌های یک Partition را خارج از ترتیب منتشر کنند.

### پارامترهای اصلی

| پارامتر | مقدار |
|---|---:|
| تعداد پیام هدف | 500,000 |
| تعداد Kafka Partition | 6 |
| اولویت عملکرد | Maximum Throughput |
| ترتیب پیام‌ها | درون هر Partition |
| تعداد Publisher Instance | قابل تنظیم |
| Delivery Semantics | At-Least-Once |
| Database | SQL Server |
| Runtime | .NET 8 |

## 2. معماری فعلی

جریان اصلی سیستم:

1. `OrderGeneratorWorker` سفارش مصنوعی تولید می‌کند.
2. سفارش و رکورد Outbox در SQL Server ذخیره می‌شوند.
3. `KafkaPublisherWorker` Partitionهای آزاد را از SQL Server Claim می‌کند.
4. برای هر Partition مالکیت‌گرفته‌شده، یک Publisher مستقل اجرا می‌شود.
5. پیام‌ها بر اساس `SequenceNo` مرتب و به Kafka ارسال می‌شوند.
6. پس از Commit تراکنش Kafka، وضعیت پیام‌ها در SQL Server به Published تغییر می‌کند.
7. در صورت خطا، Retry انجام می‌شود و پس از رسیدن به سقف مجاز، پیام وارد وضعیت Dead Letter می‌شود.

### اجزای اصلی سورس

- `Workers/OrderGeneratorWorker.cs`
- `Workers/KafkaPublisherWorker.cs`
- `Kafka/PartitionedKafkaPublisher.cs`
- `Kafka/KafkaTopicValidator.cs`
- `Data/SqlOrderRepository.cs`
- `Data/Partitioning.cs`
- `Configuration/GeneratorSettings.cs`
- `sql/01-schema.sql`
- `sql/03-multinstance-publisher.sql`
- `tests/FakeGenerator.Tests/`

ایجنت باید پیش از تغییر، این فایل‌ها را به‌صورت کامل مطالعه کند.

## 3. الزامات مالکیت Partition

مالکیت Partition باید در SQL Server مدیریت شود.

جدول اصلی:

`dbo.PublisherPartitionLease`

ستون‌های مورد انتظار:

- `PartitionId`
- `OwnerId`
- `LeaseUntilUtc`
- `FencingToken`
- `UpdatedAtUtc`

### رفتار مورد انتظار

- Claim باید اتمیک باشد.
- یک Partition نباید هم‌زمان دو مالک معتبر داشته باشد.
- زمان SQL Server (`SYSUTCDATETIME`) مرجع اعتبار Lease باشد.
- هر بار انتقال مالکیت باید `FencingToken` را افزایش دهد.
- Renew فقط با OwnerId و FencingToken معتبر امکان‌پذیر باشد.
- اگر Renew ناموفق شد، Publisher باید فوراً ارسال جدید را متوقف کند.
- Release فقط برای مالک و Token معتبر انجام شود.
- در صورت Crash شدن Instance، پس از انقضای Lease امکان تصاحب Partition وجود داشته باشد.

### نکته مهم درباره Fencing

Fencing Token در SQL Server به‌تنهایی نمی‌تواند Producer قدیمی Kafka را متوقف کند.

برای همین نسخه فعلی از Kafka Transactional Producer با شناسه ثابت برای هر Topic و Partition استفاده می‌کند:

`ClientId + Topic + PartitionId`

Producer جدید باید بتواند Producer قدیمی را از طریق Kafka Transactional Fencing خارج کند.

ایجنت نباید این سازوکار را حذف کند یا تضمین آن را صرفاً به Lease SQL وابسته بداند.

## 4. الزامات ترتیب پیام‌ها

ترتیب فقط درون هر Partition الزامی است.

الزامات:

- هر Partition یک Lane مستقل داشته باشد.
- خواندن Outbox بر اساس `SequenceNo ASC` باشد.
- ارسال پیام‌های یک Batch به ترتیب انجام شود.
- تا تعیین تکلیف پیام شکست‌خورده، پیام‌های بعدی همان Partition نباید از آن عبور کنند.
- Partitionهای دیگر باید مستقل و هم‌زمان کار کنند.
- ترتیب سراسری بین شش Partition لازم نیست.

### Dead Letter

در نسخه فعلی، پیام پس از رسیدن به `MaxRetryCount` با `DeadLetterAtUtc` مشخص می‌شود.

پس از Dead Letter شدن پیام، انتشار پیام بعدی همان Partition مجاز است.

این تصمیم به معنی آن است که پیام Dead Letter شده در Stream اصلی Kafka حضور نخواهد داشت.

ایجنت باید این رفتار را حفظ کند، مگر اینکه تغییر آن به‌عنوان یک تصمیم معماری مستقل مطرح و تأیید شود.

## 5. الزامات Kafka Publisher

کلاس اصلی:

`PartitionedKafkaPublisher.cs`

تنظیمات مهم:

- `Acks = All`
- `EnableIdempotence = true`
- `CompressionType = Lz4`
- `TransactionalId` مستقل و ثابت برای هر Partition
- ارسال Batch در قالب Kafka Transaction
- Commit پس از موفقیت Deliveryها
- Abort در صورت شکست Transaction

### الزامات عملکردی

- از ارسال ترتیبی و Await کردن تک‌تک پیام‌ها اجتناب شود، مگر در مواردی که برای صحت ضروری باشد.
- امکان ارسال هم‌زمان Deliveryها درون یک Batch حفظ شود.
- هم‌زمانی Partitionها مستقل باشد.
- `BatchSize` قابل تنظیم باشد.
- `LingerMs` و تنظیمات Producer قابل تنظیم باشند.
- خطای Kafka نباید باعث ثبت اشتباه پیام به‌عنوان Published شود.

### اعتبارسنجی Topic

قبل از شروع انتشار:

- وجود Topic بررسی شود.
- تعداد Partitionهای واقعی بررسی شود.
- اگر تعداد Partition کمتر از مقدار مورد انتظار است، Worker باید Fail Fast کند.
- خطاهای Metadata نباید صرفاً Log شوند و سپس انتشار ادامه پیدا کند.

Topic در این پروژه باید حداقل شش Partition داشته باشد.

## 6. الزامات SQL Outbox

Repository اصلی:

`Data/SqlOrderRepository.cs`

الزامات:

- خواندن پیام فقط برای Partition تحت مالکیت معتبر انجام شود.
- خواندن پیام‌ها بر اساس SequenceNo باشد.
- پیام Published دوباره انتخاب نشود.
- پیام Dead Letter شده دوباره انتخاب نشود.
- عملیات Update موفقیت باید گروهی باشد.
- عملیات ثبت Failure باید فقط روی پیام واقعاً شکست‌خورده اثر بگذارد.
- تمام عملیات حساس به مالکیت باید OwnerId و FencingToken را بررسی کنند.
- در صورت از دست رفتن Lease، عملیات SQL قدیمی نباید موفق شود.

### Bulk Update

برای کاهش Round Trip به SQL Server، ثبت Published شدن پیام‌های یک Batch باید به‌صورت گروهی انجام شود.

تنها پیام‌هایی که Kafka Transaction آن‌ها با موفقیت Commit شده است، اجازه دارند در SQL به Published تغییر کنند.

## 7. Retry و Dead Letter

رفتار مورد انتظار:

1. در صورت شکست Delivery، Transaction مربوطه Abort شود.
2. پیام شکست‌خورده شناسایی شود.
3. پیام‌های موفق قبلی نباید به اشتباه Failure بخورند.
4. RetryCount پیام شکست‌خورده افزایش یابد.
5. تا قبل از رسیدن به سقف Retry، پیام سر صف باقی بماند.
6. پس از رسیدن به MaxRetryCount، پیام Dead Letter شود.
7. خطا و اطلاعات کافی برای بررسی بعدی حفظ شوند.

ایجنت باید مخصوصاً منطق تشخیص پیام شکست‌خورده در `Task.WhenAll` را بررسی کند و مطمئن شود ترتیب Deliveryها و گزارش خطا باعث شناسایی اشتباه Event نمی‌شود.

## 8. Delivery Semantics و Consumer

سیستم Exactly-Once بین SQL Server و Kafka ندارد.

سناریوی مهم:

1. Kafka Transaction Commit می‌شود.
2. برنامه قبل از ثبت PublishedAtUtc در SQL متوقف می‌شود.
3. پس از بازیابی Lease، پیام مجدداً منتشر می‌شود.

بنابراین رفتار سیستم At-Least-Once است.

### الزام Consumer

Consumer باید:

- از `IsolationLevel.ReadCommitted` استفاده کند.
- `EventId` را از Header دریافت کند.
- جدول Inbox با Unique Constraint روی EventId داشته باشد.
- درج Inbox و اعمال Business Operation را در یک SQL Transaction انجام دهد.
- در صورت وجود EventId قبلی، Business Operation را دوباره اجرا نکند.

این موضوع باید در مستندات پروژه باقی بماند.

## 9. توزیع Partition بین Instanceها

برای شش Partition، نمونه تنظیمات:

| تعداد Instance | MaxPartitionsPerInstance |
|---:|---:|
| 1 | 6 |
| 2 | 3 |
| 3 | 2 |
| 6 | 1 |

این مقدار حداکثر تعداد Partitionهایی است که هر Instance می‌تواند مالک شود؛ تخصیص واقعی باید بر اساس Lease انجام شود.

تمام Instanceهای یک Deployment باید از مقادیر سازگار برای موارد زیر استفاده کنند:

- Kafka Bootstrap Servers
- Topic
- Partition Count
- Kafka ClientId / Transactional ID Namespace

## 10. محدودیت شناخته‌شده Generator

در نسخه فعلی، تخصیص `SequenceNo` از طریق `MAX(SequenceNo)` انجام می‌شود.

این روش برای اجرای هم‌زمان چند Order Generator امن نیست.

بنابراین:

- Multi-instance Publisher در Scope این Issue است.
- Multi-instance Order Generator در Scope فعلی نیست.
- ایجنت نباید بدون طراحی مستقل، ادعای پشتیبانی از چند Generator هم‌زمان کند.

در صورت نیاز به این قابلیت، باید Sequence Allocator مستقل با SQL Sequence یا روش اتمیک مناسب طراحی شود.

## 11. تست‌های الزامی

ایجنت باید تست‌های زیر را اضافه یا تکمیل کند:

### Unit Tests

- Partition Mapping
- ترتیب SequenceNo
- رفتار Retry
- رفتار Dead Letter
- ساخت Transactional ID
- تشخیص خطای Delivery
- رفتار Abort و Commit

### SQL Integration Tests

- دو Instance هم‌زمان نتوانند مالک معتبر یک Partition باشند.
- Lease منقضی‌شده قابل تصاحب باشد.
- FencingToken در Takeover افزایش یابد.
- Token قدیمی نتواند Renew کند.
- Token قدیمی نتواند Published یا Failed ثبت کند.
- Release مالک قدیمی مالکیت جدید را آزاد نکند.

### Kafka Integration Tests

- ارسال Batch و Commit
- Abort شدن Transaction
- Fencing Producer قدیمی
- ترتیب پیام‌ها در یک Partition
- امکان پردازش هم‌زمان Partitionهای مستقل
- مشاهده‌نشدن پیام Abortشده توسط Consumer با `read_committed`

### End-to-End

سناریوی اصلی:

1. ایجاد Topic با شش Partition.
2. اجرای Migration دیتابیس.
3. تولید 500,000 سفارش.
4. اجرای دو Publisher Instance.
5. توقف ناگهانی یکی از Instanceها.
6. انتظار برای Expire شدن Lease.
7. تصاحب Partition توسط Instance دیگر.
8. بررسی عدم از دست رفتن پیام.
9. بررسی ترتیب پیام‌ها در هر Partition.
10. بررسی Deduplication سمت Consumer.

## 12. معیارهای پذیرش (Acceptance Criteria)

این Issue زمانی قابل Done شدن است که:

- [ ] پروژه با .NET 8 Restore شود.
- [ ] پروژه Build موفق داشته باشد.
- [ ] Unit Tests موفق باشند.
- [ ] SQL Migration روی دیتابیس تمیز اجرا شود.
- [ ] SQL Integration Tests موفق باشند.
- [ ] Kafka Integration Tests موفق باشند.
- [ ] Topic با کمتر از شش Partition باعث توقف Publisher شود.
- [ ] دو Instance نتوانند هم‌زمان مالک معتبر یک Partition باشند.
- [ ] پس از Crash، Partition قابل بازیابی باشد.
- [ ] Producer قدیمی توسط Kafka Fencing متوقف شود.
- [ ] ترتیب پیام‌ها در هر Partition حفظ شود.
- [ ] پیام شکست‌خورده باعث عبور اشتباه پیام‌های بعدی نشود.
- [ ] Dead Letter طبق MaxRetryCount کار کند.
- [ ] SQL Update فقط بعد از Kafka Commit انجام شود.
- [ ] Deduplication سمت Consumer آزمایش شود.
- [ ] گزارش Throughput برای 500,000 پیام ارائه شود.
- [ ] تعداد پیام‌های تولیدشده، Published، Dead Letter و Duplicate گزارش شود.
- [ ] هیچ ادعای تست موفق بدون اجرای واقعی ثبت نشود.

## 13. موارد خارج از Scope

- تضمین Exactly-Once بین SQL Server و Kafka
- اجرای هم‌زمان چند Order Generator با Sequence Allocator فعلی
- تغییر خودکار تعداد Partitionهای Topic
- تغییر قرارداد پیام‌های فعلی بدون تأیید
- حذف پیام‌های Dead Letter از دیتابیس
- تغییر رفتار Consumerهای موجود بدون بررسی سازگاری

## 14. دستورالعمل ویژه برای AI Agents

قبل از هر تغییر:

1. سورس کامل را بخوان.
2. README و IMPLEMENTATION_NOTES را بررسی کن.
3. ساختار واقعی SQL را با Repository تطبیق بده.
4. وابستگی‌ها و رفتار نسخه فعلی را بررسی کن.
5. هیچ متد یا فایل موجود را بدون بررسی حذف نکن.

هنگام پیاده‌سازی:

- تغییرات را کوچک و قابل بازبینی نگه دار.
- ترتیب پیام‌ها را در اولویت صحت قرار بده.
- هیچ تضمین Exactly-Once غیرواقعی ارائه نکن.
- خطاهای SQL و Kafka را از هم تفکیک کن.
- از ثبت Published قبل از Kafka Commit جلوگیری کن.
- در صورت از دست رفتن Lease، ارسال را متوقف کن.
- تنظیمات را Hardcode نکن.
- Migrationها را قابل اجرای مجدد طراحی کن.
- تست‌ها را واقعاً اجرا کن.

در پایان:

1. فهرست فایل‌های تغییرکرده را ارائه بده.
2. معماری نهایی را توضیح بده.
3. خروجی واقعی Build و Test را گزارش کن.
4. تست‌هایی که اجرا نشده‌اند را صریحاً اعلام کن.
5. ریسک‌های باقی‌مانده را بنویس.
6. ZIP نهایی را فقط پس از اعتبارسنجی فایل‌ها ارائه کن.

**قانون نهایی:** هیچ‌گاه موفقیت Build، تست، Performance یا Production Readiness را بدون شواهد اجرای واقعی اعلام نکن.

## Approved v2 authority
For new final-topic work, specs/001-fake-data-kafka-publishing/spec.md (v2 amendment)
and contracts/final-topic-v2.md supersede this document's v1 duplicate acceptance.
The Kafka Streams architecture is approved; implementation is pending.
