using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Soenneker.Redis.WorkQueue.Abstract;

namespace Soenneker.Redis.WorkQueue;

internal sealed class RedisWorkQueueMaintenanceService<T> : BackgroundService where T : class
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RedisWorkQueueRegistration<T> _registration;
    private readonly ILogger<RedisWorkQueueMaintenanceService<T>> _logger;

    public RedisWorkQueueMaintenanceService(IServiceScopeFactory scopeFactory, RedisWorkQueueRegistration<T> registration,
        ILogger<RedisWorkQueueMaintenanceService<T>> logger)
    {
        _scopeFactory = scopeFactory;
        _registration = registration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_registration.Options.MaintenanceInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunMaintenance(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Redis work queue maintenance failed for {QueueName}", _registration.Options.QueueName);
            }

            if (!await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                return;
        }
    }

    private async Task RunMaintenance(CancellationToken cancellationToken)
    {
        await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();
        IRedisWorkQueue<T> queue = scope.ServiceProvider.GetRequiredService<IRedisWorkQueue<T>>();
        await queue.RunMaintenance(cancellationToken).ConfigureAwait(false);
    }
}
