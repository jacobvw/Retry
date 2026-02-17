using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JacobVW.Retry.Services;

public class RetryService : IRetryService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RetryService> _logger;
    private static readonly Random Jitter = new();

    public RetryService(
        IServiceProvider serviceProvider,
        ILogger<RetryService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async Task<bool> EnqueueAsync(
        string operationName,
        string entityKey,
        DateTimeOffset eventTimestamp,
        string? serializedPayload,
        int maxRetries = 3,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<IRetryDbContext>();

        // Check if a newer or equal event for this entity has already been
        // completed or is pending — if so, skip this stale event
        var existingNewer = await dbContext.RetryableOperations
            .AnyAsync(
                x => x.EntityKey == entityKey
                     && x.OperationName == operationName
                     && x.EventTimestamp >= eventTimestamp
                     && x.Status != RetryStatus.Failed,
                cancellationToken);

        if (existingNewer)
        {
            _logger.LogDebug(
                "Skipping stale {OperationName} for {EntityKey}: event={EventTimestamp}, a newer or equal event already exists",
                operationName, entityKey, eventTimestamp);
            return false;
        }

        var operation = new RetryableOperation
        {
            OperationName = operationName,
            EntityKey = entityKey,
            EventTimestamp = eventTimestamp,
            SerializedPayload = serializedPayload,
            MaxRetries = maxRetries,
            Status = RetryStatus.Pending,
            NextRetryAt = DateTimeOffset.UtcNow
        };

        dbContext.RetryableOperations.Add(operation);
        await dbContext.SaveChangesAsync(cancellationToken);

        _logger.LogInformation(
            "Enqueued {OperationName} for {EntityKey} (timestamp={EventTimestamp})",
            operationName, entityKey, eventTimestamp);

        return true;
    }

    public async Task ProcessPendingAsync(CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<IRetryDbContext>();
        var handlers = scope.ServiceProvider.GetRequiredService<IEnumerable<IRetryOperationHandler>>();

        var now = DateTimeOffset.UtcNow;

        var pendingOperations = await dbContext.RetryableOperations
            .Where(x => (x.Status == RetryStatus.Pending || x.Status == RetryStatus.Failed)
                        && (x.NextRetryAt == null || x.NextRetryAt <= now)
                        && x.AttemptCount < x.MaxRetries)
            .OrderBy(x => x.NextRetryAt ?? x.CreatedAt)
            .Take(100) // batch size to avoid overwhelming the system
            .ToListAsync(cancellationToken);

        if (pendingOperations.Count == 0)
        {
            return;
        }

        _logger.LogInformation(
            "Processing {Count} pending retry operations",
            pendingOperations.Count);

        foreach (var operation in pendingOperations)
        {
            // Skip if a newer event for the same entity has already completed
            var superseded = await dbContext.RetryableOperations
                .AnyAsync(
                    x => x.EntityKey == operation.EntityKey
                         && x.OperationName == operation.OperationName
                         && x.EventTimestamp > operation.EventTimestamp
                         && x.Status == RetryStatus.Completed,
                    cancellationToken);

            if (superseded)
            {
                operation.Status = RetryStatus.Completed;
                operation.CompletedAt = DateTimeOffset.UtcNow;
                operation.UpdatedAt = DateTimeOffset.UtcNow;
                operation.LastError = "Superseded by newer event";
                await dbContext.SaveChangesAsync(cancellationToken);

                _logger.LogDebug(
                    "Skipping superseded {OperationName} for {EntityKey} (id={OperationId})",
                    operation.OperationName, operation.EntityKey, operation.Id);
                continue;
            }

            var handler = handlers.FirstOrDefault(h => h.OperationName == operation.OperationName);
            if (handler == null)
            {
                _logger.LogError(
                    "No handler registered for operation '{OperationName}' (id={OperationId})",
                    operation.OperationName, operation.Id);

                operation.Status = RetryStatus.Failed;
                operation.LastError = $"No handler registered for '{operation.OperationName}'";
                operation.UpdatedAt = DateTimeOffset.UtcNow;
                await dbContext.SaveChangesAsync(cancellationToken);
                continue;
            }

            operation.Status = RetryStatus.InProgress;
            operation.AttemptCount++;
            operation.UpdatedAt = DateTimeOffset.UtcNow;
            await dbContext.SaveChangesAsync(cancellationToken);

            try
            {
                await handler.HandleAsync(operation, cancellationToken);

                operation.Status = RetryStatus.Completed;
                operation.CompletedAt = DateTimeOffset.UtcNow;
                operation.UpdatedAt = DateTimeOffset.UtcNow;
                await dbContext.SaveChangesAsync(cancellationToken);

                _logger.LogInformation(
                    "Completed {OperationName} for {EntityKey} on attempt {Attempt} (id={OperationId})",
                    operation.OperationName, operation.EntityKey, operation.AttemptCount, operation.Id);
            }
            catch (Exception ex)
            {
                operation.LastError = ex.Message;
                operation.UpdatedAt = DateTimeOffset.UtcNow;

                if (operation.AttemptCount >= operation.MaxRetries)
                {
                    operation.Status = RetryStatus.Failed;
                    _logger.LogError(ex,
                        "Failed {OperationName} for {EntityKey} after {MaxRetries} attempts (id={OperationId})",
                        operation.OperationName, operation.EntityKey, operation.MaxRetries, operation.Id);
                }
                else
                {
                    operation.Status = RetryStatus.Failed;
                    operation.NextRetryAt = CalculateNextRetry(operation.AttemptCount);
                    _logger.LogWarning(ex,
                        "Retry {Attempt}/{MaxRetries} failed for {OperationName} {EntityKey}, next retry at {NextRetryAt} (id={OperationId})",
                        operation.AttemptCount, operation.MaxRetries, operation.OperationName,
                        operation.EntityKey, operation.NextRetryAt, operation.Id);
                }

                await dbContext.SaveChangesAsync(cancellationToken);
            }
        }
    }

    /// <summary>
    /// Exponential backoff with jitter.
    /// Attempt 1: ~2s, Attempt 2: ~4s, Attempt 3: ~8s, etc.
    /// </summary>
    private static DateTimeOffset CalculateNextRetry(int attemptCount)
    {
        var baseDelay = TimeSpan.FromSeconds(Math.Pow(2, attemptCount));
        var jitterMs = Jitter.Next(0, (int)(baseDelay.TotalMilliseconds * 0.3));
        return DateTimeOffset.UtcNow + baseDelay + TimeSpan.FromMilliseconds(jitterMs);
    }
}
