using System;

namespace Soenneker.Redis.WorkQueue;

/// <summary>A durable work item assigned to a fairness and concurrency partition.</summary>
public sealed class RedisWorkQueueItem<T> where T : class
{
    public required string Id { get; init; }

    public required string PartitionKey { get; init; }

    public required T Value { get; init; }

    public DateTimeOffset EnqueuedAt { get; init; } = DateTimeOffset.UtcNow;

    public DateTimeOffset? AvailableAt { get; init; }
}
