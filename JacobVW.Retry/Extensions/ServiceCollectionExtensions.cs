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
    /// 3. Register handlers via <see cref="AddRetryHandler{THandler}"/>
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
        services.AddSingleton<RetryHandlerRegistry>();

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

    /// <summary>
    /// Registers an <see cref="IRetryOperationHandler"/> implementation.
    /// Captures the static OperationName at registration time.
    /// </summary>
    public static IServiceCollection AddRetryHandler<THandler>(this IServiceCollection services)
        where THandler : class, IRetryOperationHandler
    {
        services.AddScoped<THandler>();

        // Build or retrieve the registry and register this handler's operation name
        var registry = services
            .Where(d => d.ServiceType == typeof(RetryHandlerRegistry))
            .Select(d => d.ImplementationInstance)
            .OfType<RetryHandlerRegistry>()
            .FirstOrDefault();

        if (registry == null)
        {
            registry = new RetryHandlerRegistry();
            // Replace any existing registration
            services.AddSingleton(registry);
        }

        registry.Register(THandler.OperationName, typeof(THandler));

        return services;
    }
}
