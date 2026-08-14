using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Soenneker.Redis.Semaphores.Abstract;
using Soenneker.Redis.Semaphores.Registrars;
using Soenneker.Redis.Util.Abstract;
using Soenneker.Redis.WorkQueue.Abstract;

namespace Soenneker.Redis.WorkQueue.Registrars;

/// <summary>Registers durable Redis work queues and their Redis semaphore dependency.</summary>
public static class RedisWorkQueueRegistrar
{
    /// <summary>Adds one typed Redis work queue as a singleton service.</summary>
    public static IServiceCollection AddRedisWorkQueueAsSingleton<T>(this IServiceCollection services, string queueName,
        Action<RedisWorkQueueOptions>? configure = null) where T : class
    {
        RedisWorkQueueOptions options = BuildOptions(queueName, configure);
        services.AddRedisSemaphoreAsSingleton();
        services.TryAddSingleton(new RedisWorkQueueRegistration<T>(options));
        services.TryAddSingleton<IRedisWorkQueue<T>>(provider => new RedisWorkQueue<T>(provider.GetRequiredService<IRedisUtil>(),
            provider.GetRequiredService<IRedisSemaphore>(), provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RedisWorkQueue<T>>>(),
            provider.GetRequiredService<RedisWorkQueueRegistration<T>>().Options));
        AddMaintenanceService<T>(services, options);
        return services;
    }

    /// <summary>Adds one typed Redis work queue as a scoped service.</summary>
    public static IServiceCollection AddRedisWorkQueueAsScoped<T>(this IServiceCollection services, string queueName,
        Action<RedisWorkQueueOptions>? configure = null) where T : class
    {
        RedisWorkQueueOptions options = BuildOptions(queueName, configure);
        services.AddRedisSemaphoreAsScoped();
        services.TryAddSingleton(new RedisWorkQueueRegistration<T>(options));
        services.TryAddScoped<IRedisWorkQueue<T>>(provider => new RedisWorkQueue<T>(provider.GetRequiredService<IRedisUtil>(),
            provider.GetRequiredService<IRedisSemaphore>(), provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RedisWorkQueue<T>>>(),
            provider.GetRequiredService<RedisWorkQueueRegistration<T>>().Options));
        AddMaintenanceService<T>(services, options);
        return services;
    }

    private static RedisWorkQueueOptions BuildOptions(string queueName, Action<RedisWorkQueueOptions>? configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        var options = new RedisWorkQueueOptions { QueueName = queueName };
        configure?.Invoke(options);
        return options;
    }

    private static void AddMaintenanceService<T>(IServiceCollection services, RedisWorkQueueOptions options) where T : class
    {
        if (options.EnableBackgroundMaintenance)
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RedisWorkQueueMaintenanceService<T>>());
    }
}
