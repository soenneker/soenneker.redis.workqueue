[![](https://img.shields.io/nuget/v/soenneker.redis.workqueue.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.redis.workqueue/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.redis.workqueue/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.redis.workqueue/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.redis.workqueue.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.redis.workqueue/)

# ![](https://user-images.githubusercontent.com/4441470/224455560-91ed3ee7-f510-4041-a8d2-3fc093025112.png) Soenneker.Redis.WorkQueue
### A durable, partition-aware Redis work queue with scheduling, leases, retries, deduplication, and round-robin fairness.

## Installation

```bash
dotnet add package Soenneker.Redis.WorkQueue
```

## Registration

```csharp
services.AddRedisWorkQueueAsSingleton<SendMessageWork>("send-message", options =>
{
    options.MaxConcurrentItemsPerPartition = 2;
    options.ClaimLeaseDuration = TimeSpan.FromMinutes(1);
    options.MaximumAttempts = 10;
    options.MaintenanceInterval = TimeSpan.FromSeconds(1);
});
```

Registration starts a background maintenance service by default. Every process may register it; a distributed Redis semaphore ensures that only one process maintains a logical queue at a time. Set `EnableBackgroundMaintenance` to `false` only when another component calls `RunMaintenance` periodically.

The partition key is the fairness and concurrency boundary. In Leadping, for example, use the business ID. Ready partitions are dispatched round-robin, and `Soenneker.Redis.Semaphores` prevents more than the configured number of claims for one partition across every process.

## Enqueue and process

```csharp
await queue.Enqueue(new RedisWorkQueueItem<SendMessageWork>
{
    Id = message.Id,
    PartitionKey = message.BusinessId,
    Value = new SendMessageWork(message.Id),
    AvailableAt = DateTimeOffset.UtcNow // set a future value to schedule it
}, cancellationToken);

await using RedisWorkQueueClaim<SendMessageWork>? claim =
    await queue.TryClaim(workerId, cancellationToken);

if (claim is null)
    return;

try
{
    await Process(claim.Item.Value, claim.OwnershipLostToken);
    await queue.Complete(claim, cancellationToken);
}
catch (Exception exception)
{
    await queue.Retry(claim, new RedisWorkQueueFailure
    {
        Reason = exception.GetType().Name,
        Details = exception.Message
    }, TimeSpan.FromSeconds(10), cancellationToken);
    throw;
}
```

The queue provides at-least-once delivery. Claims automatically renew both their work-item lease and their partition semaphore permit. Background maintenance promotes scheduled items, recovers expired claims, repairs ready partitions, and expires completed-item deduplication markers.

`Retry` moves work to the dead-letter store when it reaches `MaximumAttempts`. Use `GetDeadLetter` and `RequeueDeadLetter` to inspect and recover valid items. Missing or unreadable payloads are dead-lettered automatically so they cannot block a partition. Use `Abandon` when a healthy worker needs to relinquish a claim immediately without treating it as a processing failure.

The implementation uses typed `Soenneker.Redis.Util` operations and Redis transactions with optimistic conditions. It does not use Redis scripts.
