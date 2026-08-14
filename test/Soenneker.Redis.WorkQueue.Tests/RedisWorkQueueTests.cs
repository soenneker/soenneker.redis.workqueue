using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Soenneker.Redis.WorkQueue.Abstract;
using Soenneker.Redis.Util.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.Redis.WorkQueue.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
[NotInParallel]
public sealed class RedisWorkQueueTests : HostedUnitTest
{
    private readonly IRedisWorkQueue<TestWork> _queue;
    private readonly IRedisUtil _redis;

    public RedisWorkQueueTests(Host host) : base(host)
    {
        _queue = Resolve<IRedisWorkQueue<TestWork>>(true);
        _redis = Resolve<IRedisUtil>(true);
    }

    [Test]
    public async Task Claims_should_be_round_robin_across_partitions(CancellationToken cancellationToken)
    {
        string id = Guid.NewGuid().ToString("N");
        await Enqueue($"{id}-a1", $"{id}-a", cancellationToken);
        await Enqueue($"{id}-a2", $"{id}-a", cancellationToken);
        await Enqueue($"{id}-b1", $"{id}-b", cancellationToken);

        RedisWorkQueueClaim<TestWork>? first = await _queue.TryClaim("worker", cancellationToken);
        first.Should().NotBeNull();
        first!.Item.Id.Should().Be($"{id}-a1");
        (await _queue.Complete(first, cancellationToken)).Should().BeTrue();

        RedisWorkQueueClaim<TestWork>? second = await _queue.TryClaim("worker", cancellationToken);
        second.Should().NotBeNull();
        second!.Item.Id.Should().Be($"{id}-b1");
        (await _queue.Complete(second, cancellationToken)).Should().BeTrue();

        RedisWorkQueueClaim<TestWork>? third = await _queue.TryClaim("worker", cancellationToken);
        third.Should().NotBeNull();
        third!.Item.Id.Should().Be($"{id}-a2");
        (await _queue.Complete(third, cancellationToken)).Should().BeTrue();
    }

    [Test]
    public async Task Partition_semaphore_should_gate_claims(CancellationToken cancellationToken)
    {
        string id = Guid.NewGuid().ToString("N");
        await Enqueue($"{id}-1", id, cancellationToken);
        await Enqueue($"{id}-2", id, cancellationToken);

        RedisWorkQueueClaim<TestWork>? first = await _queue.TryClaim("worker-1", cancellationToken);
        first.Should().NotBeNull();

        RedisWorkQueueClaim<TestWork>? blocked = await _queue.TryClaim("worker-2", cancellationToken);
        blocked.Should().BeNull();

        (await _queue.Complete(first!, cancellationToken)).Should().BeTrue();

        RedisWorkQueueClaim<TestWork>? second = await _queue.TryClaim("worker-2", cancellationToken);
        second.Should().NotBeNull();
        second!.Item.Id.Should().Be($"{id}-2");
        (await _queue.Complete(second, cancellationToken)).Should().BeTrue();
    }

    [Test]
    public async Task Retry_and_completed_deduplication_should_work(CancellationToken cancellationToken)
    {
        string id = Guid.NewGuid().ToString("N");
        await Enqueue(id, id, cancellationToken);

        RedisWorkQueueClaim<TestWork>? first = await _queue.TryClaim("worker", cancellationToken);
        first.Should().NotBeNull();
        first!.Attempt.Should().Be(1);
        (await _queue.Retry(first, TimeSpan.Zero, cancellationToken)).Should().BeTrue();

        RedisWorkQueueClaim<TestWork>? retried = await _queue.TryClaim("worker", cancellationToken);
        retried.Should().NotBeNull();
        retried!.Attempt.Should().Be(2);
        (await _queue.Complete(retried, cancellationToken)).Should().BeTrue();

        bool duplicateAdded = await _queue.Enqueue(CreateItem(id, id), cancellationToken);
        duplicateAdded.Should().BeFalse();
    }

    [Test]
    public async Task Scheduled_items_should_not_be_claimed_early(CancellationToken cancellationToken)
    {
        string id = Guid.NewGuid().ToString("N");
        RedisWorkQueueItem<TestWork> item = CreateItem(id, id, DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(200));
        (await _queue.Enqueue(item, cancellationToken)).Should().BeTrue();

        (await _queue.TryClaim("worker", cancellationToken)).Should().BeNull();
        await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        await _queue.RunMaintenance(cancellationToken);

        RedisWorkQueueClaim<TestWork>? claim = await _queue.TryClaim("worker", cancellationToken);
        claim.Should().NotBeNull();
        (await _queue.Complete(claim!, cancellationToken)).Should().BeTrue();
    }

    [Test]
    public async Task Abandoned_claim_should_be_recovered_after_lease_expires(CancellationToken cancellationToken)
    {
        string id = Guid.NewGuid().ToString("N");
        await Enqueue(id, id, cancellationToken);

        RedisWorkQueueClaim<TestWork>? abandoned = await _queue.TryClaim("failed-worker", cancellationToken);
        abandoned.Should().NotBeNull();
        await abandoned!.DisposeAsync();

        await Task.Delay(TimeSpan.FromMilliseconds(2200), cancellationToken);
        await _queue.RunMaintenance(cancellationToken);

        RedisWorkQueueClaim<TestWork>? recovered = await _queue.TryClaim("replacement-worker", cancellationToken);
        recovered.Should().NotBeNull();
        recovered!.Item.Id.Should().Be(id);
        recovered.Attempt.Should().Be(2);
        (await _queue.Complete(recovered, cancellationToken)).Should().BeTrue();
    }

    [Test]
    public async Task Abandon_should_make_item_immediately_available(CancellationToken cancellationToken)
    {
        string id = Guid.NewGuid().ToString("N");
        await Enqueue(id, id, cancellationToken);

        RedisWorkQueueClaim<TestWork>? first = await _queue.TryClaim("worker-1", cancellationToken);
        first.Should().NotBeNull();
        (await _queue.Abandon(first!, cancellationToken)).Should().BeTrue();

        RedisWorkQueueClaim<TestWork>? replacement = await _queue.TryClaim("worker-2", cancellationToken);
        replacement.Should().NotBeNull();
        replacement!.Attempt.Should().Be(2);
        (await _queue.Complete(replacement, cancellationToken)).Should().BeTrue();
    }

    [Test]
    public async Task Maximum_attempts_should_dead_letter_and_support_requeue(CancellationToken cancellationToken)
    {
        string id = Guid.NewGuid().ToString("N");
        await Enqueue(id, id, cancellationToken);
        var failure = new RedisWorkQueueFailure { Reason = "ProviderUnavailable", Details = "Test failure" };

        RedisWorkQueueClaim<TestWork>? first = await _queue.TryClaim("worker", cancellationToken);
        first.Should().NotBeNull();
        (await _queue.Retry(first!, failure, TimeSpan.Zero, cancellationToken)).Should().BeTrue();

        RedisWorkQueueClaim<TestWork>? second = await _queue.TryClaim("worker", cancellationToken);
        second.Should().NotBeNull();
        second!.Attempt.Should().Be(2);
        (await _queue.Retry(second, failure, TimeSpan.Zero, cancellationToken)).Should().BeTrue();

        (await _queue.TryClaim("worker", cancellationToken)).Should().BeNull();
        RedisWorkQueueDeadLetter<TestWork>? deadLetter = await _queue.GetDeadLetter(id, cancellationToken);
        deadLetter.Should().NotBeNull();
        deadLetter!.Attempt.Should().Be(2);
        deadLetter.Failure.Reason.Should().Be("ProviderUnavailable");

        (await _queue.RequeueDeadLetter(id, cancellationToken)).Should().BeTrue();
        RedisWorkQueueClaim<TestWork>? requeued = await _queue.TryClaim("worker", cancellationToken);
        requeued.Should().NotBeNull();
        requeued!.Attempt.Should().Be(1);
        (await _queue.Complete(requeued, cancellationToken)).Should().BeTrue();
    }

    [Test]
    public async Task Unreadable_payload_should_be_dead_lettered_without_blocking_partition(CancellationToken cancellationToken)
    {
        string id = Guid.NewGuid().ToString("N");
        string partition = $"{id}-partition";
        await Enqueue($"{id}-poison", partition, cancellationToken);
        await Enqueue($"{id}-healthy", partition, cancellationToken);

        await _redis.SetHash(GetItemsKey(), $"{id}-poison", "{not-json", cancellationToken: cancellationToken);

        (await _queue.TryClaim("worker", cancellationToken)).Should().BeNull();
        RedisWorkQueueDeadLetter<TestWork>? deadLetter = await _queue.GetDeadLetter($"{id}-poison", cancellationToken);
        deadLetter.Should().NotBeNull();
        deadLetter!.Failure.Reason.Should().Be("PayloadDeserializationFailed");
        deadLetter.RawPayload.Should().Be("{not-json");

        RedisWorkQueueClaim<TestWork>? healthy = await _queue.TryClaim("worker", cancellationToken);
        healthy.Should().NotBeNull();
        healthy!.Item.Id.Should().Be($"{id}-healthy");
        (await _queue.Complete(healthy, cancellationToken)).Should().BeTrue();
    }

    private async Task Enqueue(string itemId, string partitionKey, CancellationToken cancellationToken)
    {
        bool added = await _queue.Enqueue(CreateItem(itemId, partitionKey), cancellationToken);
        added.Should().BeTrue();
    }

    private static RedisWorkQueueItem<TestWork> CreateItem(string itemId, string partitionKey, DateTimeOffset? availableAt = null) => new()
    {
        Id = itemId,
        PartitionKey = partitionKey,
        Value = new TestWork { Message = itemId },
        AvailableAt = availableAt
    };

    private static string GetItemsKey()
    {
        string queueTag = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(global::Soenneker.Redis.WorkQueue.Tests.Host.QueueName)))
                                 .ToLowerInvariant()[..24];
        return $"workqueue:{{{queueTag}}}:items";
    }
}
