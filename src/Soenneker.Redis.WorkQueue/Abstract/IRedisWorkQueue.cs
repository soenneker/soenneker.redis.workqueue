using System;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Redis.WorkQueue.Abstract;

/// <summary>
/// A durable, partition-aware Redis work queue with round-robin fairness and distributed per-partition concurrency limits.
/// </summary>
public interface IRedisWorkQueue<T> where T : class
{
    /// <summary>
    /// Adds an item unless its identifier is already queued, processing, or recently completed.
    /// </summary>
    /// <param name="item">Receives the entry when the key is found.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if adds an item unless its identifier is already queued, processing, or recently completed; otherwise, false.</returns>
    ValueTask<bool> Enqueue(RedisWorkQueueItem<T> item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Claims the next available item. A claim is returned only after a permit from that partition's Redis semaphore is acquired.
    /// </summary>
    /// <param name="ownerId">Identifier of the owner to target.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task whose result is the requested redis Work Queue Claim.</returns>
    ValueTask<RedisWorkQueueClaim<T>?> TryClaim(string ownerId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Completes an owned claim and records its identifier for deduplication.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if completes an owned claim and records its identifier for deduplication; otherwise, false.</returns>
    ValueTask<bool> Complete(RedisWorkQueueClaim<T> claim, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns an owned claim to the queue, optionally after a delay.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="delay">Delay to apply before continuing.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if the claim was returned to the queue for another attempt; otherwise, false.</returns>
    ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, TimeSpan? delay = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns an owned claim to its partition immediately.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if the claim was returned to its partition; otherwise, false.</returns>
    ValueTask<bool> Abandon(RedisWorkQueueClaim<T> claim, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retries a claim while recording failure metadata. Claims at the configured attempt limit are dead-lettered.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="failure">Failure for the retry operation.</param>
    /// <param name="delay">Delay to apply before continuing.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if retries a claim while recording failure metadata. Claims at the configured attempt limit are dead-lettered; otherwise, false.</returns>
    ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure, TimeSpan? delay = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves an owned claim to the dead-letter store.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="failure">Failure for the dead letter operation.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if moves an owned claim to the dead-letter store; otherwise, false.</returns>
    ValueTask<bool> DeadLetter(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a dead-letter record by item identifier.
    /// </summary>
    /// <param name="itemId">Identifier of the item to target.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task whose result is the requested redis Work Queue Dead Letter.</returns>
    ValueTask<RedisWorkQueueDeadLetter<T>?> GetDeadLetter(string itemId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves a valid dead-letter item back to the end of its partition queue.
    /// </summary>
    /// <param name="itemId">Identifier of the item to target.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if moves a valid dead-letter item back to the end of its partition queue; otherwise, false.</returns>
    ValueTask<bool> RequeueDeadLetter(string itemId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves due scheduled work and expired claims back into their partition queues.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task whose result is the requested redis Work Queue Maintenance Result.</returns>
    ValueTask<RedisWorkQueueMaintenanceResult> RunMaintenance(CancellationToken cancellationToken = default);
}
