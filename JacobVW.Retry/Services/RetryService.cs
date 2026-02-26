using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JacobVW.Retry.Services;

public class RetryService : IRetryService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RetryService> _logger;
    private readonly TimeSpan _stuckInProgressThreshold;
    private static readonly Random Jitter = new();
    private static readonly TimeSpan DefaultMaxFailedDuration = TimeSpan.FromHours(24);
    private static readonly TimeSpan DefaultStuckInProgressThreshold = TimeSpan.FromMinutes(5);

    public RetryService(
        IServiceProvider serviceProvider,
        ILogger<RetryService> logger,
        TimeSpan? stuckInProgressThreshold = null)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _stuckInProgressThreshold = stuckInProgressThreshold ?? DefaultStuckInProgressThreshold;
    }

    public async Task<bool> EnqueueAsync(
        string operationName,
        string entityKey,
        DateTimeOffset eventTimestamp,
        string? serializedPayload,
        int maxRetries = 3,
        TimeSpan? maxFailedDuration = null,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();

        // Check if a newer or equal event for this entity has already been
        // completed or is pending — if so, skip this stale event
        var existingNewer = await store.HasNewerOrEqualEventAsync(
            operationName, entityKey, eventTimestamp, cancellationToken);

        if (existingNewer)
        {
            // Store the stale event as Discarded for auditing
            var discardedOp = new RetryableOperation
            {
                OperationName = operationName,
                EntityKey = entityKey,
                EventTimestamp = eventTimestamp,
                SerializedPayload = serializedPayload,
                MaxRetries = maxRetries,
                Status = RetryStatus.Discarded,
                LastError = $"Discarded: a newer or equal event already exists for {operationName}/{entityKey}"
            };
            await store.AddAsync(discardedOp, cancellationToken);

            _logger.LogDebug(
                "Discarded stale {OperationName} for {EntityKey}: event={EventTimestamp}, a newer or equal event already exists",
                operationName, entityKey, eventTimestamp);
            return false;
        }

        var now = DateTimeOffset.UtcNow;
        var expiresAt = now + (maxFailedDuration ?? DefaultMaxFailedDuration);

        var operation = new RetryableOperation
        {
            OperationName = operationName,
            EntityKey = entityKey,
            EventTimestamp = eventTimestamp,
            SerializedPayload = serializedPayload,
            MaxRetries = maxRetries,
            Status = RetryStatus.Pending,
            NextRetryAt = now,
            ExpiresAt = expiresAt
        };

        await store.AddAsync(operation, cancellationToken);

        _logger.LogInformation(
            "Enqueued {OperationName} for {EntityKey} (timestamp={EventTimestamp}, expires={ExpiresAt})",
            operationName, entityKey, eventTimestamp, expiresAt);

        return true;
    }

    public async Task ProcessPendingAsync(CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();
        var registry = scope.ServiceProvider.GetRequiredService<RetryHandlerRegistry>();

        var now = DateTimeOffset.UtcNow;

        // Recover operations stuck in InProgress — these are operations where
        // the processor was killed before it could save a final status.
        // Any operation still InProgress after the threshold is assumed abandoned.
        var stuckCutoff = now - _stuckInProgressThreshold;
        var stuckOperations = await store.GetStuckInProgressAsync(stuckCutoff, cancellationToken);

        foreach (var stuck in stuckOperations)
        {
            stuck.Status = RetryStatus.Pending;
            stuck.NextRetryAt = now;
            stuck.UpdatedAt = now;
            _logger.LogWarning(
                "Recovering stuck InProgress operation: {OperationName} for {EntityKey} " +
                "(id={OperationId}, stuckSince={UpdatedAt})",
                stuck.OperationName, stuck.EntityKey, stuck.Id, stuck.UpdatedAt);
        }

        if (stuckOperations.Count > 0)
        {
            await store.UpdateRangeAsync(stuckOperations, cancellationToken);
        }

        // Expire any operations that have passed their deadline
        var expiredOperations = await store.GetOperationsPastDeadlineAsync(now, cancellationToken);

        foreach (var expired in expiredOperations)
        {
            expired.Status = RetryStatus.Expired;
            expired.UpdatedAt = now;
            expired.LastError = $"Operation expired at {now:O} (deadline was {expired.ExpiresAt:O})";
            _logger.LogWarning(
                "Operation expired: {OperationName} for {EntityKey} (id={OperationId}, created={CreatedAt})",
                expired.OperationName, expired.EntityKey, expired.Id, expired.CreatedAt);
        }

        if (expiredOperations.Count > 0)
        {
            await store.UpdateRangeAsync(expiredOperations, cancellationToken);
        }

        // Now process pending operations that haven't expired
        var pendingOperations = await store.GetProcessableOperationsAsync(now, 100, cancellationToken);

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
            var superseded = await store.HasNewerCompletedEventAsync(
                operation.OperationName, operation.EntityKey,
                operation.EventTimestamp, cancellationToken);

            if (superseded)
            {
                operation.Status = RetryStatus.Superseded;
                operation.UpdatedAt = now;
                operation.LastError = "Superseded by newer event";
                await store.UpdateAsync(operation, cancellationToken);

                _logger.LogDebug(
                    "Skipping superseded {OperationName} for {EntityKey} (id={OperationId})",
                    operation.OperationName, operation.EntityKey, operation.Id);
                continue;
            }

            var handlerType = registry.GetHandlerType(operation.OperationName);
            if (handlerType == null)
            {
                _logger.LogError(
                    "No handler registered for operation '{OperationName}' (id={OperationId})",
                    operation.OperationName, operation.Id);

                operation.Status = RetryStatus.Failed;
                operation.LastError = $"No handler registered for '{operation.OperationName}'";
                operation.UpdatedAt = now;
                await store.UpdateAsync(operation, cancellationToken);
                continue;
            }

            var handler = (IRetryOperationHandler)scope.ServiceProvider.GetRequiredService(handlerType);

            operation.Status = RetryStatus.InProgress;
            operation.AttemptCount++;
            operation.UpdatedAt = now;
            await store.UpdateAsync(operation, cancellationToken);

            try
            {
                await handler.HandleAsync(operation, cancellationToken);

                operation.Status = RetryStatus.Completed;
                operation.CompletedAt = DateTimeOffset.UtcNow;
                operation.UpdatedAt = DateTimeOffset.UtcNow;
                operation.LastError = null;
                await store.UpdateAsync(operation, cancellationToken);

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
                    operation.Status = RetryStatus.Pending;
                    operation.NextRetryAt = CalculateNextRetry(operation.AttemptCount);
                    _logger.LogWarning(ex,
                        "Retry {Attempt}/{MaxRetries} failed for {OperationName} {EntityKey}, next retry at {NextRetryAt} (id={OperationId})",
                        operation.AttemptCount, operation.MaxRetries, operation.OperationName,
                        operation.EntityKey, operation.NextRetryAt, operation.Id);
                }

                await store.UpdateAsync(operation, cancellationToken);
            }
        }
    }

    public async Task<List<RetryableOperation>> GetFailedAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();
        return await store.GetPermanentlyFailedAsync(operationName, skip, take, cancellationToken);
    }

    public async Task<List<RetryableOperation>> GetExpiredAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();
        return await store.GetByStatusAsync(RetryStatus.Expired, operationName, skip, take, cancellationToken);
    }

    public async Task<List<RetryableOperation>> GetSupersededAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();
        return await store.GetByStatusAsync(RetryStatus.Superseded, operationName, skip, take, cancellationToken);
    }

    public async Task<List<RetryableOperation>> GetDiscardedAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();
        return await store.GetByStatusAsync(RetryStatus.Discarded, operationName, skip, take, cancellationToken);
    }

    public async Task<Dictionary<RetryStatus, int>> GetStatusCountsAsync(
        string? operationName = null,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();
        return await store.GetStatusCountsAsync(operationName, cancellationToken);
    }

    public async Task<int> RetryExpiredAsync(
        Guid? operationId = null,
        string? operationName = null,
        TimeSpan? newMaxFailedDuration = null,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();

        var now = DateTimeOffset.UtcNow;
        var newExpiresAt = now + (newMaxFailedDuration ?? DefaultMaxFailedDuration);

        var expiredOps = await store.GetExpiredByFilterAsync(operationId, operationName, cancellationToken);

        foreach (var op in expiredOps)
        {
            op.Status = RetryStatus.Pending;
            op.AttemptCount = 0;
            op.NextRetryAt = now;
            op.ExpiresAt = newExpiresAt;
            op.LastError = null;
            op.UpdatedAt = now;

            _logger.LogInformation(
                "Re-enqueued expired operation {OperationName} for {EntityKey} (id={Id}, new expiry={ExpiresAt})",
                op.OperationName, op.EntityKey, op.Id, newExpiresAt);
        }

        if (expiredOps.Count > 0)
        {
            await store.UpdateRangeAsync(expiredOps, cancellationToken);
        }

        return expiredOps.Count;
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
