using FakeOrderGenerator.Kafka;
using static FakeGenerator.Tests.Unit.WorkerHarness;

namespace FakeGenerator.Tests.Unit;

public sealed class PublisherWorkerTests
{
    private static async Task RunUntilAsync(Func<Task> start, Func<bool> done, Func<Task> stop)
    {
        await start();
        try { await WaitUntilAsync(done); }
        finally { await stop(); }
    }

    [Fact]
    public async Task Success_marks_published_once_after_commit()
    {
        var log = new List<string>();
        var repo = new FakeOutboxRepository { Log = s => { lock (log) log.Add(s); } };
        repo.Pending.AddRange([Msg(1), Msg(2), Msg(3)]);
        repo.LeasesToHand.Add(Lease());
        var pub = new FakePublisher { Log = s => { lock (log) log.Add(s); } };
        var worker = Create(repo, pub, new FakeTopicValidator());

        await RunUntilAsync(() => worker.StartAsync(CancellationToken.None),
            () => repo.MarkPublishedCalls.Count >= 1, () => worker.StopAsync(CancellationToken.None));

        Assert.Single(repo.MarkPublishedCalls);
        Assert.Equal(3, repo.MarkPublishedCalls[0].Count);
        Assert.Empty(repo.MarkFailedCalls);
        lock (log) Assert.True(log.IndexOf("committed") < log.IndexOf("mark-published"));
    }

    [Fact]
    public async Task Commit_failure_never_marks_published()
    {
        var repo = new FakeOutboxRepository();
        repo.Pending.AddRange([Msg(1), Msg(2)]);
        repo.LeasesToHand.Add(Lease());
        var pub = new FakePublisher { Behavior = (_, _, _) => throw new InvalidOperationException("commit failed") };
        var worker = Create(repo, pub, new FakeTopicValidator());

        await RunUntilAsync(() => worker.StartAsync(CancellationToken.None),
            () => pub.Calls.Count >= 1, () => worker.StopAsync(CancellationToken.None));

        Assert.Empty(repo.MarkPublishedCalls);
        Assert.Equal(2, repo.Pending.Count);
    }

    [Fact]
    public async Task Delivery_failure_marks_only_failed_event_and_republishes_prefix()
    {
        var repo = new FakeOutboxRepository();
        var m1 = Msg(1); var m2 = Msg(2); var m3 = Msg(3);
        repo.Pending.AddRange([m1, m2, m3]);
        repo.LeasesToHand.Add(Lease());
        var pub = new FakePublisher
        {
            Behavior = (batch, call, _) =>
                call == 1 ? throw new KafkaPublishBatchException(batch[1], new InvalidOperationException("boom")) : Task.CompletedTask
        };
        var worker = Create(repo, pub, new FakeTopicValidator());

        await RunUntilAsync(() => worker.StartAsync(CancellationToken.None),
            () => repo.MarkPublishedCalls.Count >= 1 && repo.MarkFailedCalls.Count >= 1, () => worker.StopAsync(CancellationToken.None));

        Assert.Equal(m2.EventId, repo.MarkFailedCalls[0].Item.EventId);
        Assert.Contains("boom", repo.MarkFailedCalls[0].Error);
        Assert.Equal([m1.EventId], repo.MarkPublishedCalls[0]);
        Assert.Equal([m1.EventId], pub.Calls[1].Select(x => x.EventId)); // prefix only re-published
    }

    [Fact]
    public async Task Failed_renew_stops_lane_and_sends_nothing_more_for_that_partition()
    {
        var repo = new FakeOutboxRepository { RenewResult = () => false };
        repo.Pending.AddRange(Enumerable.Range(1, 2000).Select(i => Msg(i)));
        repo.LeasesToHand.Add(Lease());
        var pub = new FakePublisher { Behavior = (_, _, ct) => Task.Delay(50, ct) };
        var worker = Create(repo, pub, new FakeTopicValidator(), renewSeconds: 1);

        await worker.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => repo.RenewFailedAtUtc is not null, 8000);
            await WaitUntilAsync(() => repo.Released.Count >= 1);
            int callsAtStop = pub.Calls.Count;
            await Task.Delay(300);

            Assert.Equal(callsAtStop, pub.Calls.Count);
            Assert.True(pub.LastPublishStartUtc <= repo.RenewFailedAtUtc!.Value.AddMilliseconds(100));
        }
        finally { await worker.StopAsync(CancellationToken.None); }
    }

    [Fact]
    public async Task Topic_validation_failure_stops_host_before_any_publish()
    {
        var repo = new FakeOutboxRepository();
        repo.Pending.Add(Msg(1));
        repo.LeasesToHand.Add(Lease());
        var pub = new FakePublisher();
        var validator = new FakeTopicValidator { Throw = new InvalidOperationException("bad topic") };
        var worker = Create(repo, pub, validator);

        // Validation runs before the first await, so the failure surfaces from StartAsync and stops the host.
        await Assert.ThrowsAsync<InvalidOperationException>(() => worker.StartAsync(CancellationToken.None));
        Assert.Empty(pub.Calls);
        Assert.Empty(repo.MarkPublishedCalls);
    }
}
