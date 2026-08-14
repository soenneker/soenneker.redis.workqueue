using System;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Redis.Semaphores;

namespace Soenneker.Redis.WorkQueue;

/// <summary>An owned work item lease together with its partition semaphore permit.</summary>
public sealed class RedisWorkQueueClaim<T> : IAsyncDisposable where T : class
{
    private readonly RedisSemaphoreHandle _semaphoreHandle;
    private readonly CancellationTokenSource _renewalCancellation = new();
    private readonly CancellationTokenSource _leaseLostCancellation;
    private readonly Func<CancellationToken, ValueTask<bool>> _renew;
    private readonly TimeSpan _renewalInterval;
    private Task? _renewalTask;
    private int _disposed;
    private int _settled;

    public RedisWorkQueueItem<T> Item { get; }

    public string OwnerId { get; }

    public int Attempt { get; }

    public DateTimeOffset LeaseExpiresAt { get; internal set; }

    /// <summary>Canceled if either the queue lease or the distributed semaphore permit is lost.</summary>
    public CancellationToken OwnershipLostToken => _leaseLostCancellation.Token;

    internal string QueueIdentity { get; }

    internal string ClaimToken { get; }

    internal bool IsSettled => Volatile.Read(ref _settled) != 0;

    internal RedisWorkQueueClaim(RedisWorkQueueItem<T> item, string ownerId, int attempt, DateTimeOffset leaseExpiresAt, string queueIdentity,
        string claimToken, RedisSemaphoreHandle semaphoreHandle, TimeSpan renewalInterval, Func<CancellationToken, ValueTask<bool>> renew)
    {
        Item = item;
        OwnerId = ownerId;
        Attempt = attempt;
        LeaseExpiresAt = leaseExpiresAt;
        QueueIdentity = queueIdentity;
        ClaimToken = claimToken;
        _semaphoreHandle = semaphoreHandle;
        _renewalInterval = renewalInterval;
        _renew = renew;
        _leaseLostCancellation = CancellationTokenSource.CreateLinkedTokenSource(semaphoreHandle.PermitLostToken);
        _renewalTask = RunRenewalLoop();
    }

    internal bool TrySettle() => Interlocked.CompareExchange(ref _settled, 1, 0) == 0;

    internal void RestoreUnsettled() => Volatile.Write(ref _settled, 0);

    private async Task RunRenewalLoop()
    {
        try
        {
            using var timer = new PeriodicTimer(_renewalInterval);

            while (await timer.WaitForNextTickAsync(_renewalCancellation.Token).ConfigureAwait(false))
            {
                if (await _renew(_renewalCancellation.Token).ConfigureAwait(false))
                    continue;

                _leaseLostCancellation.Cancel();
                return;
            }
        }
        catch (OperationCanceledException) when (_renewalCancellation.IsCancellationRequested)
        {
        }
        catch
        {
            _leaseLostCancellation.Cancel();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _renewalCancellation.CancelAsync().ConfigureAwait(false);

        if (_renewalTask is not null)
            await _renewalTask.ConfigureAwait(false);

        await _semaphoreHandle.DisposeAsync().ConfigureAwait(false);
        _leaseLostCancellation.Dispose();
        _renewalCancellation.Dispose();
    }
}
