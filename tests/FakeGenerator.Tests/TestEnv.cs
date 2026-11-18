namespace FakeGenerator.Tests;

/// <summary>Opt-in integration test environment. Missing variables mark tests as skipped ("not run"), never as passed.</summary>
public static class TestEnv
{
    public const string SqlVariable = "FAKEGEN_TEST_SQL";
    public const string KafkaVariable = "FAKEGEN_TEST_KAFKA";

    public static string? Sql => Get(SqlVariable);
    public static string? Kafka => Get(KafkaVariable);

    private static readonly Lazy<Dictionary<string, string>> LocalFile = new(LoadLocalFile);

    /// <summary>A real environment variable wins; otherwise the value comes from the git-ignored <c>local.env</c> file.</summary>
    private static string? Get(string name)
    {
        var v = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(v)) LocalFile.Value.TryGetValue(name, out v);
        return string.IsNullOrWhiteSpace(v) ? null : v;
    }

    /// <summary>Finds <c>local.env</c> next to the test project (searching up from the build output). Format: KEY=VALUE, '#' comments.</summary>
    private static Dictionary<string, string> LoadLocalFile()
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            string path = Path.Combine(dir.FullName, "local.env");
            if (!File.Exists(path)) continue;
            foreach (var raw in File.ReadAllLines(path))
            {
                var line = raw.Trim();
                int eq = line.IndexOf('=');
                if (line.Length == 0 || line[0] == '#' || eq <= 0) continue;
                result[line[..eq].Trim()] = line[(eq + 1)..].Trim().Trim('"');
            }
            break;
        }
        return result;
    }

    public static string? SkipReason(string variable) =>
        Get(variable) is null ? $"NOT RUN: {variable} is not set (environment variable or tests/FakeGenerator.Tests/local.env)." : null;
}

/// <summary>[Fact] that is reported as skipped (not run) when FAKEGEN_TEST_SQL is missing.</summary>
public sealed class SqlFactAttribute : FactAttribute
{
    public SqlFactAttribute() { Skip = TestEnv.SkipReason(TestEnv.SqlVariable); }
}

/// <summary>[Fact] that is reported as skipped (not run) when FAKEGEN_TEST_KAFKA is missing.</summary>
public sealed class KafkaFactAttribute : FactAttribute
{
    public KafkaFactAttribute() { Skip = TestEnv.SkipReason(TestEnv.KafkaVariable); }
}

/// <summary>[Fact] skipped (not run) unless both FAKEGEN_TEST_SQL and FAKEGEN_TEST_KAFKA are set.</summary>
public sealed class SqlKafkaFactAttribute : FactAttribute
{
    public SqlKafkaFactAttribute()
    {
        Skip = TestEnv.SkipReason(TestEnv.SqlVariable) ?? TestEnv.SkipReason(TestEnv.KafkaVariable);
    }
}
