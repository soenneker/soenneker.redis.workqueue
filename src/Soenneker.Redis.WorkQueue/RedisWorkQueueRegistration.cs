namespace Soenneker.Redis.WorkQueue;

internal sealed class RedisWorkQueueRegistration<T> where T : class
{
    internal RedisWorkQueueOptions Options { get; }

    internal RedisWorkQueueRegistration(RedisWorkQueueOptions options)
    {
        Options = options;
    }
}
