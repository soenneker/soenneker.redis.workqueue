namespace Soenneker.Redis.WorkQueue;

/// <summary>Serializable information describing why work was retried or dead-lettered.</summary>
public sealed class RedisWorkQueueFailure
{
    public required string Reason { get; init; }

    public string? Details { get; init; }
}
