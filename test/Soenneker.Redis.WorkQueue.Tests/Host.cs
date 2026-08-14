using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Soenneker.TestHosts.Unit;
using Soenneker.Utils.Test;
using Soenneker.Redis.WorkQueue.Registrars;

namespace Soenneker.Redis.WorkQueue.Tests;

public sealed class Host : UnitTestHost
{
    internal static readonly string QueueName = $"workqueue-tests-{System.Guid.NewGuid():N}";

    public override Task InitializeAsync()
    {
        SetupIoC(Services);

        return base.InitializeAsync();
    }

    private static void SetupIoC(IServiceCollection services)
    {
        services.AddLogging(builder =>
        {
            builder.AddSerilog(dispose: false);
        });

        IConfiguration config = TestUtil.BuildConfig();
        services.AddSingleton(config);

        services.AddRedisWorkQueueAsScoped<TestWork>(QueueName, options =>
        {
            options.ClaimLeaseDuration = System.TimeSpan.FromSeconds(2);
            options.ClaimRenewalInterval = System.TimeSpan.FromMilliseconds(500);
            options.DefaultRetryDelay = System.TimeSpan.Zero;
            options.MaximumAttempts = 2;
            options.EnableBackgroundMaintenance = false;
        });
    }
}
