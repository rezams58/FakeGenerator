namespace FakeOrderGenerator.Data;

/// <summary>Retry versus dead-letter rule. <c>SqlOrderRepository.MarkFailedAsync</c> applies the same rule in SQL.</summary>
public static class RetryPolicy
{
    /// <summary>True when an event whose retry count has just become <paramref name="newRetryCount"/> must be dead-lettered.</summary>
    public static bool ShouldDeadLetter(int newRetryCount, int maxRetryCount) => newRetryCount >= maxRetryCount;

    /// <summary>Error text persisted in Outbox.LastError for the failed event only (max 2000 chars).</summary>
    public static string FormatError(Exception ex)
    {
        var root = ex.InnerException ?? ex;
        string text = $"{root.GetType().Name}: {root.Message}";
        return text.Length <= 2000 ? text : text[..2000];
    }
}
