namespace Soenneker.Redis.WorkQueue;

/// <summary>Counts work repaired or promoted by a maintenance pass.</summary>
public sealed class RedisWorkQueueMaintenanceResult
{
    public int ScheduledItemsPromoted { get; init; }

    public int ExpiredClaimsRecovered { get; init; }

    public int PartitionsRepaired { get; init; }

    public int CompletedMarkersRemoved { get; init; }
}
