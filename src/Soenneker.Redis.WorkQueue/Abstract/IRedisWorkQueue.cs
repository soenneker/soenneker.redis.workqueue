using System;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Redis.WorkQueue.Abstract;

/// <summary>
/// A durable, partition-aware Redis work queue with round-robin fairness and distributed per-partition concurrency limits.
/// </summary>
public interface IRedisWorkQueue<T> where T : class
{
    /// <summary>Adds an item unless its identifier is already queued, processing, or recently completed.</summary>
    ValueTask<bool> Enqueue(RedisWorkQueueItem<T> item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Claims the next available item. A claim is returned only after a permit from that partition's Redis semaphore is acquired.
    /// </summary>
    ValueTask<RedisWorkQueueClaim<T>?> TryClaim(string ownerId, CancellationToken cancellationToken = default);

    /// <summary>Completes an owned claim and records its identifier for deduplication.</summary>
    ValueTask<bool> Complete(RedisWorkQueueClaim<T> claim, CancellationToken cancellationToken = default);

    /// <summary>Returns an owned claim to the queue, optionally after a delay.</summary>
    ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, TimeSpan? delay = null, CancellationToken cancellationToken = default);

    /// <summary>Returns an owned claim to its partition immediately.</summary>
    ValueTask<bool> Abandon(RedisWorkQueueClaim<T> claim, CancellationToken cancellationToken = default);

    /// <summary>Retries a claim while recording failure metadata. Claims at the configured attempt limit are dead-lettered.</summary>
    ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure, TimeSpan? delay = null,
        CancellationToken cancellationToken = default);

    /// <summary>Moves an owned claim to the dead-letter store.</summary>
    ValueTask<bool> DeadLetter(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure,
        CancellationToken cancellationToken = default);

    /// <summary>Gets a dead-letter record by item identifier.</summary>
    ValueTask<RedisWorkQueueDeadLetter<T>?> GetDeadLetter(string itemId, CancellationToken cancellationToken = default);

    /// <summary>Moves a valid dead-letter item back to the end of its partition queue.</summary>
    ValueTask<bool> RequeueDeadLetter(string itemId, CancellationToken cancellationToken = default);

    /// <summary>Moves due scheduled work and expired claims back into their partition queues.</summary>
    ValueTask<RedisWorkQueueMaintenanceResult> RunMaintenance(CancellationToken cancellationToken = default);
}
