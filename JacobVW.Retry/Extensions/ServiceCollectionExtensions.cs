using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Services;
using JacobVW.Retry.Stores;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JacobVW.Retry.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers JacobVW.Retry with the default EF Core storage backend.
    /// Automatically registers IRetryDbContext and IRetryStore for you.
    /// 
    /// Consuming projects must:
    /// 1. Implement IRetryDbContext on their DbContext
    /// 2. Apply RetryableOperationConfiguration in OnModelCreating
    /// 3. Register handlers via <see cref="AddRetryHandler{THandler}"/>
    /// </summary>
    /// <typeparam name="TDbContext">
    /// Your DbContext that implements <see cref="IRetryDbContext"/>
    /// </typeparam>
    /// <param name="services">Service collection</param>
    /// <param name="processingInterval">
    /// How often to check for pending operations (default: 30 seconds)
    /// </param>
    public static IServiceCollection AddRetryService<TDbContext>(
        this IServiceCollection services,
        TimeSpan? processingInterval = null)
        where TDbContext : DbContext, IRetryDbContext
    {
        // TODO: Split EF Core backend into a separate JacobVW.Retry.EntityFrameworkCore package
        services.AddScoped<IRetryDbContext>(sp => sp.GetRequiredService<TDbContext>());
        services.AddScoped<IRetryStore, EfCoreRetryStore>();

        return services.AddRetryServiceCore(processingInterval);
    }

    /// <summary>
    /// Registers JacobVW.Retry without a storage backend.
    /// Use this when providing a custom <see cref="IRetryStore"/>
    /// implementation (e.g. Redis, MongoDB).
    /// 
    /// You must register your own IRetryStore after calling this method.
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="processingInterval">
    /// How often to check for pending operations (default: 30 seconds)
    /// </param>
    public static IServiceCollection AddRetryService(
        this IServiceCollection services,
        TimeSpan? processingInterval = null)
    {
        return services.AddRetryServiceCore(processingInterval);
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
            services.AddSingleton(registry);
        }

        registry.Register(THandler.OperationName, typeof(THandler));

        return services;
    }

    private static IServiceCollection AddRetryServiceCore(
        this IServiceCollection services,
        TimeSpan? processingInterval)
    {
        services.AddSingleton<RetryHandlerRegistry>();

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
