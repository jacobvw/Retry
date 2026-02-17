using JacobVW.Retry.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JacobVW.Retry.Services;

/// <summary>
/// Background service that periodically processes pending
/// retryable operations. Configurable interval.
/// </summary>
public class RetryProcessorService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RetryProcessorService> _logger;
    private readonly TimeSpan _interval;

    public RetryProcessorService(
        IServiceProvider serviceProvider,
        ILogger<RetryProcessorService> logger,
        TimeSpan? interval = null)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _interval = interval ?? TimeSpan.FromSeconds(30);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "RetryProcessorService started with interval {Interval}",
            _interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var scope = _serviceProvider.CreateAsyncScope();
                var retryService = scope.ServiceProvider.GetRequiredService<IRetryService>();
                await retryService.ProcessPendingAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing pending retry operations");
            }

            await Task.Delay(_interval, stoppingToken);
        }
    }
}
