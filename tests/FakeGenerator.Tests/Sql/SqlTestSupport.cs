using Microsoft.Data.SqlClient;
using System.Text.RegularExpressions;

namespace FakeGenerator.Tests.Sql;

internal static class SqlTestSupport
{
    public static async Task RunScriptAsync(string cs, string path, CancellationToken ct)
    {
        string script = await File.ReadAllTextAsync(path, ct);
        foreach (var batch in Regex.Split(script, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(batch)) continue;
            await using var cn = new SqlConnection(cs);
            await cn.OpenAsync(ct);
            await using var cmd = new SqlCommand(batch, cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    public static string SqlDir()
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !Directory.Exists(Path.Combine(dir, "src", "sql"))) dir = Path.GetDirectoryName(dir);
        return Path.Combine(dir ?? throw new DirectoryNotFoundException("src/sql not found"), "src", "sql");
    }

    public static async Task ApplySchemaAsync(string cs, CancellationToken ct)
    {
        foreach (var f in new[] { "01-schema.sql", "03-multinstance-publisher.sql", "04-lifecycle.sql" })
            await RunScriptAsync(cs, Path.Combine(SqlDir(), f), ct);
    }

    public static async Task<int> ScalarAsync(string cs, string sql, CancellationToken ct)
    {
        await using var cn = new SqlConnection(cs);
        await cn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, cn);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }
}
