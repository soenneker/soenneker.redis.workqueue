using System;

namespace Soenneker.Redis.WorkQueue;

/// <summary>A work item removed from normal dispatch because it could not be processed safely.</summary>
public sealed class RedisWorkQueueDeadLetter<T> where T : class
{
    public required string ItemId { get; init; }

    public required string PartitionKey { get; init; }

    public RedisWorkQueueItem<T>? Item { get; init; }

    /// <summary>The original serialized payload when it could not be deserialized.</summary>
    public string? RawPayload { get; init; }

    public required RedisWorkQueueFailure Failure { get; init; }

    public int Attempt { get; init; }

    public DateTimeOffset DeadLetteredAt { get; init; } = DateTimeOffset.UtcNow;
}
