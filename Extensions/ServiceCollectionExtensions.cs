using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JacobVW.Retry.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers JacobVW.Retry services: IRetryService and the
    /// background RetryProcessorService.
    /// 
    /// Consuming projects must also:
    /// 1. Register IRetryDbContext (implement on your DbContext)
    /// 2. Apply RetryableOperationConfiguration in your DbContext
    /// 3. Register IRetryOperationHandler implementations
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="processingInterval">
    /// How often to check for pending operations (default: 30 seconds)
    /// </param>
    public static IServiceCollection AddRetryService(
        this IServiceCollection services,
        TimeSpan? processingInterval = null)
    {
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
