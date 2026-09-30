using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Redis.WorkQueue.Abstract;

/// <summary>
/// A durable, partition-aware Redis work queue with round-robin fairness and distributed per-partition concurrency limits.
/// </summary>
/// <remarks>Pass a source-generated JsonSerializerContext to JSON operations for trimming and Native AOT support. Include metadata for RedisWorkQueueItem&lt;T&gt; and RedisWorkQueueDeadLetter&lt;T&gt;. Overloads without a context use reflection. Use compatible JSON contracts throughout each item's lifecycle.</remarks>
public interface IRedisWorkQueue<T> where T : class
{
    /// <summary>
    /// Adds an item unless its identifier is already queued, processing, or recently completed.
    /// </summary>
    /// <param name="item">The work item, stable identifier, and partition key to enqueue.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns><c>true</c> if the item was added; <c>false</c> if its identifier is already active or retained as completed.</returns>
    [RequiresUnreferencedCode("Reflection-based JSON serialization requires preserved payload types. Use the overload accepting JsonSerializerContext instead.")]
    [RequiresDynamicCode("Reflection-based JSON serialization may require runtime code generation. Use the overload accepting JsonSerializerContext instead.")]
    ValueTask<bool> Enqueue(RedisWorkQueueItem<T> item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds an item unless its identifier is already queued, processing, or recently completed.
    /// </summary>
    /// <param name="item">The work item, stable identifier, and partition key to enqueue.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="jsonContext">Generated metadata for queue items and dead-letter records.</param>
    /// <returns><c>true</c> if the item was added; <c>false</c> if its identifier is already active or retained as completed.</returns>
    ValueTask<bool> Enqueue(RedisWorkQueueItem<T> item, JsonSerializerContext jsonContext, CancellationToken cancellationToken = default);

    /// <summary>
    /// Claims the next available item. A claim is returned only after a permit from that partition's Redis semaphore is acquired.
    /// </summary>
    /// <param name="ownerId">A diagnostic identifier for the worker acquiring the claim.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>An owned claim, or <c>null</c> when no eligible item and partition permit are available.</returns>
    [RequiresUnreferencedCode("Reflection-based JSON serialization requires preserved payload types. Use the overload accepting JsonSerializerContext instead.")]
    [RequiresDynamicCode("Reflection-based JSON serialization may require runtime code generation. Use the overload accepting JsonSerializerContext instead.")]
    ValueTask<RedisWorkQueueClaim<T>?> TryClaim(string ownerId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Claims the next available item. A claim is returned only after a permit from that partition's Redis semaphore is acquired.
    /// </summary>
    /// <param name="ownerId">A diagnostic identifier for the worker acquiring the claim.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="jsonContext">Generated metadata for queue items and dead-letter records.</param>
    /// <returns>An owned claim, or <c>null</c> when no eligible item and partition permit are available.</returns>
    ValueTask<RedisWorkQueueClaim<T>?> TryClaim(string ownerId, JsonSerializerContext jsonContext, CancellationToken cancellationToken = default);

    /// <summary>
    /// Completes an owned claim and records its identifier for deduplication.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns><c>true</c> if the still-owned claim was completed; otherwise, <c>false</c>.</returns>
    ValueTask<bool> Complete(RedisWorkQueueClaim<T> claim, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns an owned claim to the queue, optionally after a delay.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="delay">Delay to apply before continuing.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>true if the claim was returned to the queue for another attempt; otherwise, false.</returns>
    [RequiresUnreferencedCode("Reflection-based JSON serialization requires preserved payload types. Use the overload accepting JsonSerializerContext instead.")]
    [RequiresDynamicCode("Reflection-based JSON serialization may require runtime code generation. Use the overload accepting JsonSerializerContext instead.")]
    ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, TimeSpan? delay = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns an owned claim to the queue, optionally after a delay.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="delay">Delay to apply before continuing.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="jsonContext">Generated metadata for queue items and dead-letter records.</param>
    /// <returns>true if the claim was returned to the queue for another attempt; otherwise, false.</returns>
    ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, JsonSerializerContext jsonContext, TimeSpan? delay = null, CancellationToken cancellationToken = default);

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
    /// <returns><c>true</c> if the still-owned claim was retried or dead-lettered; otherwise, <c>false</c>.</returns>
    [RequiresUnreferencedCode("Reflection-based JSON serialization requires preserved payload types. Use the overload accepting JsonSerializerContext instead.")]
    [RequiresDynamicCode("Reflection-based JSON serialization may require runtime code generation. Use the overload accepting JsonSerializerContext instead.")]
    ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure, TimeSpan? delay = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retries a claim while recording failure metadata. Claims at the configured attempt limit are dead-lettered.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="failure">Failure for the retry operation.</param>
    /// <param name="delay">Delay to apply before continuing.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="jsonContext">Generated metadata for queue items and dead-letter records.</param>
    /// <returns><c>true</c> if the still-owned claim was retried or dead-lettered; otherwise, <c>false</c>.</returns>
    ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure, JsonSerializerContext jsonContext, TimeSpan? delay = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves an owned claim to the dead-letter store.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="failure">Failure for the dead letter operation.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns><c>true</c> if the still-owned claim was moved; otherwise, <c>false</c>.</returns>
    [RequiresUnreferencedCode("Reflection-based JSON serialization requires preserved payload types. Use the overload accepting JsonSerializerContext instead.")]
    [RequiresDynamicCode("Reflection-based JSON serialization may require runtime code generation. Use the overload accepting JsonSerializerContext instead.")]
    ValueTask<bool> DeadLetter(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves an owned claim to the dead-letter store.
    /// </summary>
    /// <param name="claim">Owned queue claim to complete, retry, or abandon.</param>
    /// <param name="failure">Failure for the dead letter operation.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="jsonContext">Generated metadata for queue items and dead-letter records.</param>
    /// <returns><c>true</c> if the still-owned claim was moved; otherwise, <c>false</c>.</returns>
    ValueTask<bool> DeadLetter(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure, JsonSerializerContext jsonContext, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a dead-letter record by item identifier.
    /// </summary>
    /// <param name="itemId">Identifier of the item to target.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The dead-letter record, or <c>null</c> when it does not exist or cannot be read.</returns>
    [RequiresUnreferencedCode("Reflection-based JSON serialization requires preserved payload types. Use the overload accepting JsonSerializerContext instead.")]
    [RequiresDynamicCode("Reflection-based JSON serialization may require runtime code generation. Use the overload accepting JsonSerializerContext instead.")]
    ValueTask<RedisWorkQueueDeadLetter<T>?> GetDeadLetter(string itemId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a dead-letter record by item identifier.
    /// </summary>
    /// <param name="itemId">Identifier of the item to target.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="jsonContext">Generated metadata for queue items and dead-letter records.</param>
    /// <returns>The dead-letter record, or <c>null</c> when it does not exist or cannot be read.</returns>
    ValueTask<RedisWorkQueueDeadLetter<T>?> GetDeadLetter(string itemId, JsonSerializerContext jsonContext, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves a valid dead-letter item back to the end of its partition queue.
    /// </summary>
    /// <param name="itemId">Identifier of the item to target.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns><c>true</c> if the record was moved; otherwise, <c>false</c>.</returns>
    [RequiresUnreferencedCode("Reflection-based JSON serialization requires preserved payload types. Use the overload accepting JsonSerializerContext instead.")]
    [RequiresDynamicCode("Reflection-based JSON serialization may require runtime code generation. Use the overload accepting JsonSerializerContext instead.")]
    ValueTask<bool> RequeueDeadLetter(string itemId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves a valid dead-letter item back to the end of its partition queue.
    /// </summary>
    /// <param name="itemId">Identifier of the item to target.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="jsonContext">Generated metadata for queue items and dead-letter records.</param>
    /// <returns><c>true</c> if the record was moved; otherwise, <c>false</c>.</returns>
    ValueTask<bool> RequeueDeadLetter(string itemId, JsonSerializerContext jsonContext, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves due scheduled work and expired claims back into their partition queues.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>Counts for each maintenance action completed by this run.</returns>
    ValueTask<RedisWorkQueueMaintenanceResult> RunMaintenance(CancellationToken cancellationToken = default);
}
