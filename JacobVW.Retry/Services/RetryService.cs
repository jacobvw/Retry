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
    private readonly RetrySignal _signal;
    private readonly TimeSpan _stuckInProgressThreshold;
    private static readonly Random Jitter = new();
    private static readonly TimeSpan DefaultMaxFailedDuration = TimeSpan.FromHours(24);
    private static readonly TimeSpan DefaultStuckInProgressThreshold = TimeSpan.FromMinutes(5);

    public RetryService(
        IServiceProvider serviceProvider,
        ILogger<RetryService> logger,
        RetrySignal signal,
        TimeSpan? stuckInProgressThreshold = null)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _signal = signal;
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

        // Check if a strictly newer event for this entity has already been
        // completed or is pending — if so, skip this stale event.
        // Same-timestamp events are allowed through.
        var existingNewer = await store.HasNewerEventAsync(
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
                LastError = $"Discarded: a newer event already exists for {operationName}/{entityKey}"
            };
            await store.AddAsync(discardedOp, cancellationToken);

            _logger.LogDebug(
                "Discarded stale {OperationName} for {EntityKey}: event={EventTimestamp}, a newer event already exists",
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

        _signal.Notify();
        return true;
    }

    public async Task<int> EnqueueTransactionalAsync(
        IRetryDbContext dbContext,
        IEnumerable<RetryEnqueueRequest> requests,
        CancellationToken cancellationToken = default)
    {
        var requestList = requests.ToList();
        if (requestList.Count == 0) return 0;

        var enqueued = 0;
        var now = DateTimeOffset.UtcNow;

        // Collect distinct values for DB-translatable Contains() filters
        var operationNames = requestList.Select(r => r.OperationName).Distinct().ToList();
        var entityKeys = requestList.Select(r => r.EntityKey).Distinct().ToList();

        // Single query: fetch all active operations matching any of the
        // requested (OperationName, EntityKey) combinations, then group
        // client-side to find max EventTimestamp per pair.
        var candidates = await dbContext.RetryableOperations
            .Where(x => (x.Status == RetryStatus.Pending
                         || x.Status == RetryStatus.InProgress
                         || x.Status == RetryStatus.Completed)
                        && operationNames.Contains(x.OperationName)
                        && entityKeys.Contains(x.EntityKey))
            .Select(x => new { x.OperationName, x.EntityKey, x.EventTimestamp })
            .ToListAsync(cancellationToken);

        // Build a lookup for O(1) dedup checks in the loop
        var maxTimestampLookup = candidates
            .GroupBy(x => (x.OperationName, x.EntityKey))
            .ToDictionary(
                g => g.Key,
                g => g.Max(x => x.EventTimestamp));

        foreach (var request in requestList)
        {
            // Dedup: check if a strictly newer event already exists
            var isStale = maxTimestampLookup.TryGetValue(
                              (request.OperationName, request.EntityKey),
                              out var maxTs)
                          && maxTs > request.EventTimestamp;

            if (isStale)
            {
                // Add as Discarded for auditing (same as EnqueueAsync)
                var discardedOp = new RetryableOperation
                {
                    OperationName = request.OperationName,
                    EntityKey = request.EntityKey,
                    EventTimestamp = request.EventTimestamp,
                    SerializedPayload = request.SerializedPayload,
                    MaxRetries = request.MaxRetries,
                    Status = RetryStatus.Discarded,
                    LastError = $"Discarded: a newer event already exists for {request.OperationName}/{request.EntityKey}"
                };
                dbContext.RetryableOperations.Add(discardedOp);

                _logger.LogDebug(
                    "Transactional enqueue discarded stale {OperationName} for {EntityKey}: event={EventTimestamp}",
                    request.OperationName, request.EntityKey, request.EventTimestamp);
            }
            else
            {
                var expiresAt = now + (request.MaxFailedDuration ?? DefaultMaxFailedDuration);

                var operation = new RetryableOperation
                {
                    OperationName = request.OperationName,
                    EntityKey = request.EntityKey,
                    EventTimestamp = request.EventTimestamp,
                    SerializedPayload = request.SerializedPayload,
                    MaxRetries = request.MaxRetries,
                    Status = RetryStatus.Pending,
                    NextRetryAt = now,
                    ExpiresAt = expiresAt
                };
                dbContext.RetryableOperations.Add(operation);

                _logger.LogInformation(
                    "Transactional enqueue {OperationName} for {EntityKey} (timestamp={EventTimestamp}, expires={ExpiresAt})",
                    request.OperationName, request.EntityKey, request.EventTimestamp, expiresAt);

                enqueued++;
            }
        }

        // Do NOT call SaveChangesAsync — caller owns the transaction
        // Do NOT call _signal.Notify() — caller should call it after SaveChanges
        return enqueued;
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
            // Skip if a newer event (or same-timestamp but later-received) has already completed
            var superseded = await store.HasNewerCompletedEventAsync(
                operation.OperationName, operation.EntityKey,
                operation.EventTimestamp, operation.CreatedAt, cancellationToken);

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

            operation.Status = RetryStatus.InProgress;
            operation.AttemptCount++;
            operation.UpdatedAt = now;
            await store.UpdateAsync(operation, cancellationToken);

            try
            {
                // Resolve inside the try so DI construction failures are
                // caught and recorded as a proper attempt, not a phantom one.
                var handler = (IRetryOperationHandler)scope.ServiceProvider.GetRequiredService(handlerType);

                await handler.HandleAsync(operation, cancellationToken);

                // Only mark Completed if the handler didn't set a terminal status itself.
                // This allows handlers to set e.g. Discarded for permanently unprocessable items.
                if (operation.Status == RetryStatus.InProgress)
                {
                    operation.Status = RetryStatus.Completed;
                    operation.CompletedAt = DateTimeOffset.UtcNow;
                    operation.LastError = null;
                }

                operation.UpdatedAt = DateTimeOffset.UtcNow;
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

    public async Task<int> RetryAsync(
        Guid? operationId = null,
        string? operationName = null,
        TimeSpan? newMaxFailedDuration = null,
        CancellationToken cancellationToken = default)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IRetryStore>();

        var now = DateTimeOffset.UtcNow;
        var newExpiresAt = now + (newMaxFailedDuration ?? DefaultMaxFailedDuration);

        var terminalOps = await store.GetByFilterAsync(operationId, operationName, cancellationToken);

        foreach (var op in terminalOps)
        {
            var previousStatus = op.Status;
            op.Status = RetryStatus.Pending;
            op.AttemptCount = 0;
            op.NextRetryAt = now;
            op.ExpiresAt = newExpiresAt;
            op.LastError = null;
            op.UpdatedAt = now;

            _logger.LogInformation(
                "Re-enqueued {PreviousStatus} operation {OperationName} for {EntityKey} (id={Id}, new expiry={ExpiresAt})",
                previousStatus, op.OperationName, op.EntityKey, op.Id, newExpiresAt);
        }

        if (terminalOps.Count > 0)
        {
            await store.UpdateRangeAsync(terminalOps, cancellationToken);
            _signal.Notify();
        }

        return terminalOps.Count;
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
