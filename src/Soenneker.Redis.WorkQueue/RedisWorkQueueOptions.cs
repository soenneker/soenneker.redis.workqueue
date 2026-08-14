using System;
using System.Text.Json;

namespace Soenneker.Redis.WorkQueue;

/// <summary>Configures one logical Redis work queue.</summary>
public sealed class RedisWorkQueueOptions
{
    public string QueueName { get; set; } = "default";

    public int MaxConcurrentItemsPerPartition { get; set; } = 1;

    public TimeSpan ClaimLeaseDuration { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan? ClaimRenewalInterval { get; set; }

    public TimeSpan DefaultRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>The claim attempt after which a retry is dead-lettered. Set to <c>null</c> to allow unlimited attempts.</summary>
    public int? MaximumAttempts { get; set; } = 10;

    public TimeSpan CompletedItemRetention { get; set; } = TimeSpan.FromDays(4);

    public int MaintenanceBatchSize { get; set; } = 100;

    public int PartitionScanLimit { get; set; } = 100;

    /// <summary>How often the registered background service runs distributed queue maintenance.</summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Whether registration adds the queue's background maintenance service.</summary>
    public bool EnableBackgroundMaintenance { get; set; } = true;

    public JsonSerializerOptions? SerializerOptions { get; set; }
}
