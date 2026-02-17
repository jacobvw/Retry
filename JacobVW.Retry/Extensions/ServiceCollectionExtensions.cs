using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Services;
using JacobVW.Retry.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JacobVW.Retry.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers JacobVW.Retry services: IRetryService, the default
    /// EfCoreRetryStore, and the background RetryProcessorService.
    /// 
    /// Consuming projects must also:
    /// 1. Register IRetryDbContext (implement on your DbContext)
    /// 2. Apply RetryableOperationConfiguration in your DbContext
    /// 3. Register IRetryOperationHandler implementations
    /// 
    /// To use a custom storage backend (e.g. Redis), register your
    /// own IRetryStore implementation AFTER calling this method —
    /// it will override the default EfCoreRetryStore.
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="processingInterval">
    /// How often to check for pending operations (default: 30 seconds)
    /// </param>
    public static IServiceCollection AddRetryService(
        this IServiceCollection services,
        TimeSpan? processingInterval = null)
    {
        // Default store (EF Core) — consumers can override with their own IRetryStore
        services.AddScoped<IRetryStore, EfCoreRetryStore>();

        services.AddScoped<IRetryService, RetryService>();

        services.AddSingleton<RetryProcessorService>(sp =>
            new RetryProcessorService(
                serviceProvider: sp,
                logger: sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RetryProcessorService>>(),
                interval: processingInterval));

        services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<RetryProcessorService>());

        return services;
    }
}
