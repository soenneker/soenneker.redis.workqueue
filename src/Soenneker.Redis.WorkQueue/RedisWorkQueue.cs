using Soenneker.Extensions.ValueTask;
using Soenneker.Extensions.Task;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Soenneker.Redis.Semaphores;
using Soenneker.Redis.Semaphores.Abstract;
using Soenneker.Redis.Util.Abstract;
using Soenneker.Redis.WorkQueue.Abstract;
using Soenneker.Hashing.Sha256;
using Soenneker.Utils.Json;
using StackExchange.Redis;

namespace Soenneker.Redis.WorkQueue;

/// <inheritdoc cref="IRedisWorkQueue{T}"/>
public sealed class RedisWorkQueue<T> : IRedisWorkQueue<T> where T : class
{
    private static readonly Sha256HashingUtil _sha256 = new();

    private readonly IRedisUtil _redis;
    private readonly IRedisSemaphore _semaphore;
    private readonly ILogger<RedisWorkQueue<T>> _logger;
    private readonly RedisWorkQueueOptions _options;
    private readonly string _prefix;
    private readonly string _itemsKey;
    private readonly string _partitionsKey;
    private readonly string _readyPartitionsKey;
    private readonly string _readyPartitionSetKey;
    private readonly string _knownPartitionsKey;
    private readonly string _scheduledKey;
    private readonly string _leasesKey;
    private readonly string _ownersKey;
    private readonly string _attemptsKey;
    private readonly string _completedKey;
    private readonly string _deadLettersKey;
    private readonly TimeSpan _renewalInterval;
    private readonly RedisSemaphoreOptions _semaphoreOptions;

    public RedisWorkQueue(IRedisUtil redis, IRedisSemaphore semaphore, ILogger<RedisWorkQueue<T>> logger, RedisWorkQueueOptions options)
    {
        _redis = redis;
        _semaphore = semaphore;
        _logger = logger;
        _options = Validate(options);
        _renewalInterval = options.ClaimRenewalInterval ?? TimeSpan.FromTicks(options.ClaimLeaseDuration.Ticks / 3);

        string queueTag = _sha256.Hash(options.QueueName)[..24];
        _prefix = $"workqueue:{{{queueTag}}}";
        _itemsKey = $"{_prefix}:items";
        _partitionsKey = $"{_prefix}:partitions";
        _readyPartitionsKey = $"{_prefix}:ready-partitions";
        _readyPartitionSetKey = $"{_prefix}:ready-partition-set";
        _knownPartitionsKey = $"{_prefix}:known-partitions";
        _scheduledKey = $"{_prefix}:scheduled";
        _leasesKey = $"{_prefix}:leases";
        _ownersKey = $"{_prefix}:owners";
        _attemptsKey = $"{_prefix}:attempts";
        _completedKey = $"{_prefix}:completed";
        _deadLettersKey = $"{_prefix}:dead-letters";

        _semaphoreOptions = new RedisSemaphoreOptions
        {
            LeaseDuration = options.ClaimLeaseDuration,
            AutoRenew = true,
            RenewalInterval = _renewalInterval
        };
    }

    public async ValueTask<bool> Enqueue(RedisWorkQueueItem<T> item, CancellationToken cancellationToken = default)
    {
        ValidateItem(item);
        cancellationToken.ThrowIfCancellationRequested();

        DateTimeOffset now = DateTimeOffset.UtcNow;
        bool scheduled = item.AvailableAt is { } availableAt && availableAt > now;
        string serialized = JsonUtil.Serialize(item)!;
        string partitionQueueKey = GetPartitionQueueKey(item.PartitionKey);

        bool added = await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.HashNotExists(_itemsKey, item.Id));
            transaction.AddCondition(Condition.HashNotExists(_deadLettersKey, item.Id));
            transaction.AddCondition(Condition.SortedSetNotContains(_completedKey, item.Id));
            _ = transaction.HashSetAsync(_itemsKey, item.Id, serialized);
            _ = transaction.HashSetAsync(_partitionsKey, item.Id, item.PartitionKey);
            _ = transaction.SetAddAsync(_knownPartitionsKey, item.PartitionKey);

            if (scheduled)
                _ = transaction.SortedSetAddAsync(_scheduledKey, item.Id, ToScore(item.AvailableAt!.Value));
            else
                _ = transaction.ListRightPushAsync(partitionQueueKey, item.Id);
        }, cancellationToken).NoSync();

        if (added && !scheduled)
            await EnsurePartitionReady(item.PartitionKey, cancellationToken).NoSync();

        return added;
    }

    public async ValueTask<RedisWorkQueueClaim<T>?> TryClaim(string ownerId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ownerId);

        long readyCount = await _redis.GetListLength(_readyPartitionsKey, cancellationToken).NoSync() ?? 0;
        int scans = (int) Math.Min(readyCount, _options.PartitionScanLimit);

        for (var index = 0; index < scans; index++)
        {
            string? partitionKey = await PopReadyPartition(cancellationToken).NoSync();

            if (partitionKey is null)
                return null;

            RedisSemaphoreHandle? permit = await _semaphore.TryAcquire(GetSemaphoreName(partitionKey),
                _options.MaxConcurrentItemsPerPartition, _semaphoreOptions, cancellationToken).NoSync();

            if (permit is null)
            {
                await EnsurePartitionReady(partitionKey, cancellationToken).NoSync();
                continue;
            }

            RedisWorkQueueClaim<T>? claim = await TryClaimFromPartition(partitionKey, ownerId, permit, cancellationToken).NoSync();

            if (claim is not null)
                return claim;

            await permit.DisposeAsync().NoSync();
        }

        return null;
    }

    public async ValueTask<bool> Complete(RedisWorkQueueClaim<T> claim, CancellationToken cancellationToken = default)
    {
        ValidateClaim(claim);

        if (!claim.TrySettle())
            return false;

        bool completed = await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.HashEqual(_ownersKey, claim.Item.Id, claim.ClaimToken));
            _ = transaction.HashDeleteAsync(_ownersKey, claim.Item.Id);
            _ = transaction.SortedSetRemoveAsync(_leasesKey, claim.Item.Id);
            _ = transaction.HashDeleteAsync(_itemsKey, claim.Item.Id);
            _ = transaction.HashDeleteAsync(_partitionsKey, claim.Item.Id);
            _ = transaction.HashDeleteAsync(_attemptsKey, claim.Item.Id);
            _ = transaction.SortedSetAddAsync(_completedKey, claim.Item.Id, ToScore(DateTimeOffset.UtcNow));
        }, cancellationToken).NoSync();

        if (!completed)
        {
            claim.RestoreUnsettled();
            return false;
        }

        await claim.DisposeAsync().NoSync();
        return true;
    }

    public ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, TimeSpan? delay = null, CancellationToken cancellationToken = default) =>
        RetryInternal(claim, null, delay, cancellationToken);

    public ValueTask<bool> Retry(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure, TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(failure);
        return RetryInternal(claim, failure, delay, cancellationToken);
    }

    public ValueTask<bool> Abandon(RedisWorkQueueClaim<T> claim, CancellationToken cancellationToken = default) =>
        RetryInternal(claim, null, TimeSpan.Zero, cancellationToken, enforceMaximumAttempts: false);

    private async ValueTask<bool> RetryInternal(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure? failure, TimeSpan? delay,
        CancellationToken cancellationToken, bool enforceMaximumAttempts = true)
    {
        ValidateClaim(claim);

        if (enforceMaximumAttempts && _options.MaximumAttempts is { } maximumAttempts && claim.Attempt >= maximumAttempts)
        {
            RedisWorkQueueFailure terminalFailure = failure ?? new RedisWorkQueueFailure
            {
                Reason = "MaximumAttemptsExceeded",
                Details = $"The item reached the configured maximum of {maximumAttempts} attempts."
            };
            return await DeadLetter(claim, terminalFailure, cancellationToken).NoSync();
        }

        TimeSpan retryDelay = delay ?? _options.DefaultRetryDelay;

        if (retryDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(delay), "Retry delay cannot be negative.");

        if (!claim.TrySettle())
            return false;

        bool retryImmediately = retryDelay == TimeSpan.Zero;
        DateTimeOffset availableAt = DateTimeOffset.UtcNow + retryDelay;
        string partitionQueueKey = GetPartitionQueueKey(claim.Item.PartitionKey);

        bool retried = await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.HashEqual(_ownersKey, claim.Item.Id, claim.ClaimToken));
            _ = transaction.HashDeleteAsync(_ownersKey, claim.Item.Id);
            _ = transaction.SortedSetRemoveAsync(_leasesKey, claim.Item.Id);

            if (retryImmediately)
                _ = transaction.ListRightPushAsync(partitionQueueKey, claim.Item.Id);
            else
                _ = transaction.SortedSetAddAsync(_scheduledKey, claim.Item.Id, ToScore(availableAt));
        }, cancellationToken).NoSync();

        if (!retried)
        {
            claim.RestoreUnsettled();
            return false;
        }

        if (retryImmediately)
            await EnsurePartitionReady(claim.Item.PartitionKey, cancellationToken).NoSync();

        await claim.DisposeAsync().NoSync();
        return true;
    }

    public async ValueTask<bool> DeadLetter(RedisWorkQueueClaim<T> claim, RedisWorkQueueFailure failure,
        CancellationToken cancellationToken = default)
    {
        ValidateClaim(claim);
        ArgumentNullException.ThrowIfNull(failure);

        if (!claim.TrySettle())
            return false;

        var deadLetter = new RedisWorkQueueDeadLetter<T>
        {
            ItemId = claim.Item.Id,
            PartitionKey = claim.Item.PartitionKey,
            Item = claim.Item,
            Failure = failure,
            Attempt = claim.Attempt,
            DeadLetteredAt = DateTimeOffset.UtcNow
        };
        string serialized = JsonUtil.Serialize(deadLetter)!;

        bool moved = await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.HashEqual(_ownersKey, claim.Item.Id, claim.ClaimToken));
            _ = transaction.HashDeleteAsync(_ownersKey, claim.Item.Id);
            _ = transaction.SortedSetRemoveAsync(_leasesKey, claim.Item.Id);
            _ = transaction.SortedSetRemoveAsync(_scheduledKey, claim.Item.Id);
            _ = transaction.HashDeleteAsync(_itemsKey, claim.Item.Id);
            _ = transaction.HashDeleteAsync(_partitionsKey, claim.Item.Id);
            _ = transaction.HashDeleteAsync(_attemptsKey, claim.Item.Id);
            _ = transaction.HashSetAsync(_deadLettersKey, claim.Item.Id, serialized);
        }, cancellationToken).NoSync();

        if (!moved)
        {
            claim.RestoreUnsettled();
            return false;
        }

        await claim.DisposeAsync().NoSync();
        return true;
    }

    public async ValueTask<RedisWorkQueueDeadLetter<T>?> GetDeadLetter(string itemId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(itemId);
        string? serialized = await _redis.GetHash(_deadLettersKey, itemId, cancellationToken).NoSync();

        if (serialized is null)
            return null;

        try
        {
            return JsonUtil.Deserialize<RedisWorkQueueDeadLetter<T>>(serialized);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            _logger.LogError(exception, "Could not deserialize dead-letter record {ItemId} in queue {QueueName}", itemId, _options.QueueName);
            return null;
        }
    }

    public async ValueTask<bool> RequeueDeadLetter(string itemId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(itemId);
        RedisWorkQueueDeadLetter<T>? deadLetter = await GetDeadLetter(itemId, cancellationToken).NoSync();

        if (deadLetter?.Item is null)
            return false;

        RedisWorkQueueItem<T> item = deadLetter.Item;
        string serializedItem = JsonUtil.Serialize(item)!;

        bool moved = await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.HashExists(_deadLettersKey, itemId));
            transaction.AddCondition(Condition.HashNotExists(_itemsKey, itemId));
            transaction.AddCondition(Condition.SortedSetNotContains(_completedKey, itemId));
            _ = transaction.HashDeleteAsync(_deadLettersKey, itemId);
            _ = transaction.HashSetAsync(_itemsKey, itemId, serializedItem);
            _ = transaction.HashSetAsync(_partitionsKey, itemId, item.PartitionKey);
            _ = transaction.HashDeleteAsync(_attemptsKey, itemId);
            _ = transaction.SetAddAsync(_knownPartitionsKey, item.PartitionKey);
            _ = transaction.ListRightPushAsync(GetPartitionQueueKey(item.PartitionKey), itemId);
        }, cancellationToken).NoSync();

        if (moved)
            await EnsurePartitionReady(item.PartitionKey, cancellationToken).NoSync();

        return moved;
    }

    public async ValueTask<RedisWorkQueueMaintenanceResult> RunMaintenance(CancellationToken cancellationToken = default)
    {
        RedisSemaphoreHandle? maintenancePermit = await _semaphore.TryAcquire(GetMaintenanceSemaphoreName(), 1,
            _semaphoreOptions, cancellationToken).NoSync();

        if (maintenancePermit is null)
            return new RedisWorkQueueMaintenanceResult();

        await using (maintenancePermit.ConfigureAwait(false))
        {
            int promoted = await PromoteScheduled(cancellationToken).NoSync();
            int recovered = await RecoverExpired(cancellationToken).NoSync();
            int repaired = await RepairReadyPartitions(cancellationToken).NoSync();
            int removed = await RemoveExpiredCompletedMarkers(cancellationToken).NoSync();

            return new RedisWorkQueueMaintenanceResult
            {
                ScheduledItemsPromoted = promoted,
                ExpiredClaimsRecovered = recovered,
                PartitionsRepaired = repaired,
                CompletedMarkersRemoved = removed
            };
        }
    }

    private async ValueTask<RedisWorkQueueClaim<T>?> TryClaimFromPartition(string partitionKey, string ownerId,
        RedisSemaphoreHandle permit, CancellationToken cancellationToken)
    {
        string partitionQueueKey = GetPartitionQueueKey(partitionKey);
        string? itemId = await _redis.GetListValue(partitionQueueKey, 0, cancellationToken).NoSync();

        if (itemId is null)
            return null;

        string? serialized = await _redis.GetHash(_itemsKey, itemId, cancellationToken).NoSync();
        RedisWorkQueueItem<T>? item = null;
        Exception? deserializationException = null;

        if (serialized is not null)
        {
            try
            {
                item = JsonUtil.Deserialize<RedisWorkQueueItem<T>>(serialized);
            }
            catch (Exception exception) when (exception is JsonException or NotSupportedException)
            {
                deserializationException = exception;
            }
        }

        if (item is null)
        {
            _logger.LogError(deserializationException, "Dead-lettering unreadable Redis work queue item {ItemId} in queue {QueueName}", itemId,
                _options.QueueName);
            await MovePoisonItemToDeadLetter(partitionKey, partitionQueueKey, itemId, serialized, deserializationException, cancellationToken)
                .NoSync();
            return null;
        }

        string claimToken = $"{ownerId}:{Guid.NewGuid():N}";
        DateTimeOffset leaseExpiresAt = DateTimeOffset.UtcNow + _options.ClaimLeaseDuration;
        Task<long>? attemptTask = null;

        bool claimed = await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.ListIndexEqual(partitionQueueKey, 0, itemId));
            transaction.AddCondition(Condition.HashExists(_itemsKey, itemId));
            _ = transaction.ListLeftPopAsync(partitionQueueKey);
            _ = transaction.HashSetAsync(_ownersKey, itemId, claimToken);
            _ = transaction.SortedSetAddAsync(_leasesKey, itemId, ToScore(leaseExpiresAt));
            attemptTask = transaction.HashIncrementAsync(_attemptsKey, itemId);
        }, cancellationToken).NoSync();

        if (!claimed)
            return null;

        int attempt = checked((int) await attemptTask!.NoSync());
        await EnsurePartitionReadyIfNeeded(partitionKey, cancellationToken).NoSync();

        if (_options.MaximumAttempts is { } maximumAttempts && attempt > maximumAttempts)
        {
            await MoveOwnedItemToDeadLetter(item, itemId, claimToken, attempt, new RedisWorkQueueFailure
            {
                Reason = "MaximumAttemptsExceeded",
                Details = $"The item exceeded the configured maximum of {maximumAttempts} attempts after an abandoned or expired claim."
            }, cancellationToken).NoSync();
            return null;
        }

        RedisWorkQueueClaim<T>? claim = null;
        claim = new RedisWorkQueueClaim<T>(item, ownerId, attempt, leaseExpiresAt, _prefix, claimToken, permit, _renewalInterval,
            async token =>
            {
                DateTimeOffset nextExpiration = DateTimeOffset.UtcNow + _options.ClaimLeaseDuration;
                bool renewed = await RenewClaim(itemId, claimToken, nextExpiration, token).NoSync();

                if (renewed && claim is not null)
                    claim.LeaseExpiresAt = nextExpiration;

                return renewed;
            });

        return claim;
    }

    private async ValueTask MovePoisonItemToDeadLetter(string partitionKey, string partitionQueueKey, string itemId, string? rawPayload,
        Exception? exception, CancellationToken cancellationToken)
    {
        var deadLetter = new RedisWorkQueueDeadLetter<T>
        {
            ItemId = itemId,
            PartitionKey = partitionKey,
            RawPayload = rawPayload,
            Failure = new RedisWorkQueueFailure
            {
                Reason = rawPayload is null ? "MissingPayload" : "PayloadDeserializationFailed",
                Details = exception?.Message
            },
            DeadLetteredAt = DateTimeOffset.UtcNow
        };
        string serializedDeadLetter = JsonUtil.Serialize(deadLetter)!;

        bool moved = await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.ListIndexEqual(partitionQueueKey, 0, itemId));
            _ = transaction.ListLeftPopAsync(partitionQueueKey);
            _ = transaction.HashDeleteAsync(_itemsKey, itemId);
            _ = transaction.HashDeleteAsync(_partitionsKey, itemId);
            _ = transaction.HashDeleteAsync(_attemptsKey, itemId);
            _ = transaction.SortedSetRemoveAsync(_scheduledKey, itemId);
            _ = transaction.HashSetAsync(_deadLettersKey, itemId, serializedDeadLetter);
        }, cancellationToken).NoSync();

        if (moved)
            await EnsurePartitionReadyIfNeeded(partitionKey, cancellationToken).NoSync();
    }

    private async ValueTask<bool> MoveOwnedItemToDeadLetter(RedisWorkQueueItem<T> item, string itemId, string claimToken, int attempt,
        RedisWorkQueueFailure failure, CancellationToken cancellationToken)
    {
        var deadLetter = new RedisWorkQueueDeadLetter<T>
        {
            ItemId = itemId,
            PartitionKey = item.PartitionKey,
            Item = item,
            Failure = failure,
            Attempt = attempt,
            DeadLetteredAt = DateTimeOffset.UtcNow
        };
        string serialized = JsonUtil.Serialize(deadLetter)!;

        return await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.HashEqual(_ownersKey, itemId, claimToken));
            _ = transaction.HashDeleteAsync(_ownersKey, itemId);
            _ = transaction.SortedSetRemoveAsync(_leasesKey, itemId);
            _ = transaction.HashDeleteAsync(_itemsKey, itemId);
            _ = transaction.HashDeleteAsync(_partitionsKey, itemId);
            _ = transaction.HashDeleteAsync(_attemptsKey, itemId);
            _ = transaction.HashSetAsync(_deadLettersKey, itemId, serialized);
        }, cancellationToken).NoSync();
    }

    private ValueTask<bool> RenewClaim(string itemId, string claimToken, DateTimeOffset leaseExpiresAt, CancellationToken cancellationToken) =>
        _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.HashEqual(_ownersKey, itemId, claimToken));
            _ = transaction.SortedSetAddAsync(_leasesKey, itemId, ToScore(leaseExpiresAt));
        }, cancellationToken);

    private async ValueTask<int> PromoteScheduled(CancellationToken cancellationToken)
    {
        IReadOnlyList<string>? due = await _redis.GetSortedSetValuesByScore(_scheduledKey, maximumScore: ToScore(DateTimeOffset.UtcNow),
            take: _options.MaintenanceBatchSize, cancellationToken: cancellationToken).NoSync();

        if (due is null)
            return 0;

        var promoted = 0;

        foreach (string itemId in due)
        {
            double? score = await _redis.GetSortedSetScore(_scheduledKey, itemId, cancellationToken).NoSync();
            string? partitionKey = await _redis.GetHash(_partitionsKey, itemId, cancellationToken).NoSync();

            if (score is null || partitionKey is null)
                continue;

            bool moved = await _redis.ExecuteTransaction(transaction =>
            {
                transaction.AddCondition(Condition.SortedSetEqual(_scheduledKey, itemId, score.Value));
                _ = transaction.SortedSetRemoveAsync(_scheduledKey, itemId);
                _ = transaction.ListRightPushAsync(GetPartitionQueueKey(partitionKey), itemId);
            }, cancellationToken).NoSync();

            if (!moved)
                continue;

            await EnsurePartitionReady(partitionKey, cancellationToken).NoSync();
            promoted++;
        }

        return promoted;
    }

    private async ValueTask<int> RecoverExpired(CancellationToken cancellationToken)
    {
        IReadOnlyList<string>? expired = await _redis.GetSortedSetValuesByScore(_leasesKey, maximumScore: ToScore(DateTimeOffset.UtcNow),
            take: _options.MaintenanceBatchSize, cancellationToken: cancellationToken).NoSync();

        if (expired is null)
            return 0;

        var recovered = 0;

        foreach (string itemId in expired)
        {
            double? score = await _redis.GetSortedSetScore(_leasesKey, itemId, cancellationToken).NoSync();
            string? partitionKey = await _redis.GetHash(_partitionsKey, itemId, cancellationToken).NoSync();

            if (score is null || partitionKey is null)
                continue;

            bool moved = await _redis.ExecuteTransaction(transaction =>
            {
                transaction.AddCondition(Condition.SortedSetEqual(_leasesKey, itemId, score.Value));
                _ = transaction.SortedSetRemoveAsync(_leasesKey, itemId);
                _ = transaction.HashDeleteAsync(_ownersKey, itemId);
                _ = transaction.ListRightPushAsync(GetPartitionQueueKey(partitionKey), itemId);
            }, cancellationToken).NoSync();

            if (!moved)
                continue;

            await EnsurePartitionReady(partitionKey, cancellationToken).NoSync();
            recovered++;
        }

        return recovered;
    }

    private async ValueTask<int> RepairReadyPartitions(CancellationToken cancellationToken)
    {
        IReadOnlyList<string>? partitions = await _redis.GetSetValues(_knownPartitionsKey, cancellationToken).NoSync();

        if (partitions is null)
            return 0;

        var repaired = 0;

        foreach (string partition in partitions)
        {
            if (await EnsurePartitionReadyIfNeeded(partition, cancellationToken).NoSync())
                repaired++;
        }

        return repaired;
    }

    private async ValueTask<int> RemoveExpiredCompletedMarkers(CancellationToken cancellationToken)
    {
        double maximumScore = ToScore(DateTimeOffset.UtcNow - _options.CompletedItemRetention);
        IReadOnlyList<string>? expired = await _redis.GetSortedSetValuesByScore(_completedKey, maximumScore: maximumScore,
            take: _options.MaintenanceBatchSize, cancellationToken: cancellationToken).NoSync();

        if (expired is null)
            return 0;

        var removed = 0;

        foreach (string itemId in expired)
        {
            if (await _redis.RemoveSortedSetValue(_completedKey, itemId, cancellationToken).NoSync())
                removed++;
        }

        return removed;
    }

    private async ValueTask<string?> PopReadyPartition(CancellationToken cancellationToken)
    {
        string? partitionKey = await _redis.GetListValue(_readyPartitionsKey, 0, cancellationToken).NoSync();

        if (partitionKey is null)
            return null;

        bool popped = await _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.ListIndexEqual(_readyPartitionsKey, 0, partitionKey));
            _ = transaction.ListLeftPopAsync(_readyPartitionsKey);
            _ = transaction.SetRemoveAsync(_readyPartitionSetKey, partitionKey);
        }, cancellationToken).NoSync();

        return popped ? partitionKey : null;
    }

    private async ValueTask<bool> EnsurePartitionReadyIfNeeded(string partitionKey, CancellationToken cancellationToken)
    {
        long length = await _redis.GetListLength(GetPartitionQueueKey(partitionKey), cancellationToken).NoSync() ?? 0;
        return length > 0 && await EnsurePartitionReady(partitionKey, cancellationToken).NoSync();
    }

    private ValueTask<bool> EnsurePartitionReady(string partitionKey, CancellationToken cancellationToken) =>
        _redis.ExecuteTransaction(transaction =>
        {
            transaction.AddCondition(Condition.SetNotContains(_readyPartitionSetKey, partitionKey));
            _ = transaction.SetAddAsync(_readyPartitionSetKey, partitionKey);
            _ = transaction.ListRightPushAsync(_readyPartitionsKey, partitionKey);
        }, cancellationToken);

    private string GetPartitionQueueKey(string partitionKey) => $"{_prefix}:partition:{Hash(partitionKey)}";

    private string GetSemaphoreName(string partitionKey) => $"workqueue:{Hash(_options.QueueName)}:{Hash(partitionKey)}";

    private string GetMaintenanceSemaphoreName() => $"workqueue:{Hash(_options.QueueName)}:maintenance";

    private static string Hash(string value) => _sha256.Hash(value)[..24];

    private static double ToScore(DateTimeOffset value) => value.ToUnixTimeMilliseconds();

    private void ValidateClaim(RedisWorkQueueClaim<T> claim)
    {
        ArgumentNullException.ThrowIfNull(claim);

        if (!StringComparer.Ordinal.Equals(claim.QueueIdentity, _prefix))
            throw new ArgumentException("The claim belongs to a different Redis work queue.", nameof(claim));
    }

    private static void ValidateItem(RedisWorkQueueItem<T> item)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentException.ThrowIfNullOrWhiteSpace(item.Id);
        ArgumentException.ThrowIfNullOrWhiteSpace(item.PartitionKey);
        ArgumentNullException.ThrowIfNull(item.Value);
    }

    private static RedisWorkQueueOptions Validate(RedisWorkQueueOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.QueueName);

        if (options.MaxConcurrentItemsPerPartition <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "Per-partition concurrency must be greater than zero.");

        if (options.ClaimLeaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "Claim lease duration must be greater than zero.");

        TimeSpan renewal = options.ClaimRenewalInterval ?? TimeSpan.FromTicks(options.ClaimLeaseDuration.Ticks / 3);

        if (renewal <= TimeSpan.Zero || renewal >= options.ClaimLeaseDuration)
            throw new ArgumentOutOfRangeException(nameof(options), "Claim renewal interval must be greater than zero and less than the lease duration.");

        if (options.DefaultRetryDelay < TimeSpan.Zero || options.CompletedItemRetention <= TimeSpan.Zero || options.MaintenanceInterval <= TimeSpan.Zero ||
            options.MaintenanceBatchSize <= 0 || options.PartitionScanLimit <= 0 || options.MaximumAttempts is <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "Retry, retention, and batch settings are invalid.");

        return options;
    }
}
