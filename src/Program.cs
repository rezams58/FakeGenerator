using FakeOrderGenerator.Configuration;
using FakeOrderGenerator.Data;
using FakeOrderGenerator.Generation;
using FakeOrderGenerator.Generation.Order;
using FakeOrderGenerator.Kafka;
using FakeOrderGenerator.Reporting;
using FakeOrderGenerator.Workers;
using Microsoft.Extensions.Options;

bool verifyMode = args.Contains("--verify");
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOptions<ConnectionSettings>()
    .Bind(builder.Configuration.GetSection("ConnectionStrings")).ValidateOnStart();
builder.Services.AddOptions<KafkaSettings>()
    .Bind(builder.Configuration.GetSection("Kafka")).ValidateOnStart();
builder.Services.AddOptions<PublisherConfig>()
    .Bind(builder.Configuration.GetSection("Publisher")).ValidateOnStart();
builder.Services.AddOptions<GeneratorConfig>()
    .Bind(builder.Configuration.GetSection("Generator")).ValidateOnStart();
builder.Services.AddOptions<ReportingConfig>()
    .Bind(builder.Configuration.GetSection("Reporting")).ValidateOnStart();

builder.Services.AddSingleton<IValidateOptions<ConnectionSettings>, OptionsValidators.Connection>();
builder.Services.AddSingleton<IValidateOptions<KafkaSettings>, OptionsValidators.Kafka>();
builder.Services.AddSingleton<IValidateOptions<PublisherConfig>, OptionsValidators.Publisher>();
builder.Services.AddSingleton<IValidateOptions<GeneratorConfig>, OptionsValidators.Generator>();
builder.Services.AddSingleton<IValidateOptions<ReportingConfig>, OptionsValidators.Reporting>();

if (builder.Configuration.GetSection("Generator:RecordCount").Exists())
    Console.Error.WriteLine("WARNING: Generator:RecordCount is obsolete; use Generator:TargetEventCount.");

builder.Services.AddSingleton<SqlOrderRepository>();
builder.Services.AddSingleton<IOutboxRepository>(sp => sp.GetRequiredService<SqlOrderRepository>());
builder.Services.AddSingleton<IPartitionPublisherFactory, KafkaPartitionPublisherFactory>();
builder.Services.AddSingleton<ITopicValidator, KafkaTopicValidatorAdapter>();
builder.Services.AddSingleton<OrderDefinition>(sp =>
{
    var generator = sp.GetRequiredService<IOptions<GeneratorConfig>>().Value;
    generator.Entities.TryGetValue(OrderDefinition.EntityName, out var orderConfig);
    return new OrderDefinition(generator, orderConfig);
});
builder.Services.AddSingleton<IFakeEntityDefinition>(sp => sp.GetRequiredService<OrderDefinition>());
builder.Services.AddSingleton<IEntityWriter, OrderWriter>();
if (!verifyMode)
{
    builder.Services.AddHostedService<GeneratorWorker>();
    builder.Services.AddHostedService<KafkaPublisherWorker>();
}

var app = builder.Build();
if (verifyMode)
{
    // --verify: run after a generate + publish run. Reads the stream, checks it against SQL, writes the run report.
    var sp = app.Services;
    var started = DateTime.UtcNow;
    var kafka = sp.GetRequiredService<IOptions<KafkaSettings>>().Value;
    var generator = sp.GetRequiredService<IOptions<GeneratorConfig>>().Value;
    var reporting = sp.GetRequiredService<IOptions<ReportingConfig>>().Value;
    string cs = sp.GetRequiredService<IOptions<ConnectionSettings>>().Value.SqlServer;
    var lifecycle = sp.GetRequiredService<IFakeEntityDefinition>().Lifecycle;

    var verification = await new StreamVerifier(kafka, cs).VerifyAsync(lifecycle, CancellationToken.None);
    var writer = new RunReportWriter(reporting, cs);
    var report = await writer.BuildAsync(started, generator.TargetEventCount, lifecycle.TerminalStates, verification, CancellationToken.None);
    string path = writer.Write(report);
    Console.WriteLine($"Run report: {path}");
    Console.WriteLine($"Verifier: {report.VerifierStatus}; published={report.Published}; deadLettered={report.DeadLettered}; pending={report.Pending}; " +
                      $"duplicates={report.Duplicates}; ordering={report.OrderingViolations}; lifecycle={report.LifecycleViolations}; " +
                      $"gaps={report.DeadLetterGaps}; missing={report.MissingOnStream}");
    foreach (var f in report.Failures) Console.Error.WriteLine($"FAIL: {f}");
    return report.Passed ? 0 : 1;
}

await app.RunAsync();
return 0;
