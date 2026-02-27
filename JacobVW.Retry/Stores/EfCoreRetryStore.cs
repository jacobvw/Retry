using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Models;
using Microsoft.EntityFrameworkCore;

namespace JacobVW.Retry.Stores;

/// <summary>
/// Default IRetryStore implementation backed by Entity Framework Core.
/// Requires IRetryDbContext to be registered (consumers implement this
/// on their existing DbContext).
/// 
/// Registered as scoped to match DbContext lifetime.
/// </summary>
public class EfCoreRetryStore : IRetryStore
{
    private readonly IRetryDbContext _dbContext;

    public EfCoreRetryStore(IRetryDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    // ── Queries ──────────────────────────────────────────

    public async Task<bool> HasNewerEventAsync(
        string operationName,
        string entityKey,
        DateTimeOffset eventTimestamp,
        CancellationToken cancellationToken = default)
    {
        // Block re-enqueueing only if there's a strictly newer event.
        // Same-timestamp events are allowed through — CreatedAt ordering
        // during processing determines which one wins.
        // Expired, Superseded, Discarded, and permanently-Failed records do NOT block, so
        // a genuine new incoming event for the same entity can still be processed.
        return await _dbContext.RetryableOperations
            .AnyAsync(
                x => x.EntityKey == entityKey
                     && x.OperationName == operationName
                     && x.EventTimestamp > eventTimestamp
                     && (x.Status == RetryStatus.Pending
                         || x.Status == RetryStatus.InProgress
                         || x.Status == RetryStatus.Completed),
                cancellationToken);
    }

    public async Task<bool> HasNewerCompletedEventAsync(
        string operationName,
        string entityKey,
        DateTimeOffset eventTimestamp,
        DateTimeOffset createdAt,
        CancellationToken cancellationToken = default)
    {
        // Supersede if a completed event exists that is either:
        // (a) strictly newer by event timestamp, OR
        // (b) same event timestamp but received later (by CreatedAt)
        return await _dbContext.RetryableOperations
            .AnyAsync(
                x => x.EntityKey == entityKey
                     && x.OperationName == operationName
                     && x.Status == RetryStatus.Completed
                     && (x.EventTimestamp > eventTimestamp
                         || (x.EventTimestamp == eventTimestamp
                             && x.CreatedAt > createdAt)),
                cancellationToken);
    }

    public async Task<List<RetryableOperation>> GetStuckInProgressAsync(
        DateTimeOffset cutoff,
        CancellationToken cancellationToken = default)
    {
        return await _dbContext.RetryableOperations
            .Where(x => x.Status == RetryStatus.InProgress && x.UpdatedAt <= cutoff)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<RetryableOperation>> GetOperationsPastDeadlineAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        return await _dbContext.RetryableOperations
            .Where(x => (x.Status == RetryStatus.Pending
                         || x.Status == RetryStatus.Failed
                         || x.Status == RetryStatus.InProgress)
                        && x.ExpiresAt != null
                        && x.ExpiresAt <= now)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<RetryableOperation>> GetProcessableOperationsAsync(
        DateTimeOffset now,
        int batchSize = 100,
        CancellationToken cancellationToken = default)
    {
        // Only Pending is the active-retry status now — Failed means permanently
        // exhausted (AttemptCount >= MaxRetries) and should not be re-processed here.
        // AttemptCount == 0 ensures MaxRetries = 0 still gets one attempt ("try once, no retries").
        return await _dbContext.RetryableOperations
            .Where(x => x.Status == RetryStatus.Pending
                        && (x.NextRetryAt == null || x.NextRetryAt <= now)
                        && (x.AttemptCount == 0 || x.AttemptCount < x.MaxRetries)
                        && (x.ExpiresAt == null || x.ExpiresAt > now))
            .OrderBy(x => x.NextRetryAt ?? x.CreatedAt)
            .Take(batchSize)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<RetryableOperation>> GetByStatusAsync(
        RetryStatus status,
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        var query = _dbContext.RetryableOperations
            .Where(x => x.Status == status);

        if (!string.IsNullOrEmpty(operationName))
        {
            query = query.Where(x => x.OperationName == operationName);
        }

        return await query
            .OrderByDescending(x => x.UpdatedAt)
            .Skip(skip)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<RetryableOperation>> GetPermanentlyFailedAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        var query = _dbContext.RetryableOperations
            .Where(x => x.Status == RetryStatus.Failed
                        && x.AttemptCount >= x.MaxRetries);

        if (!string.IsNullOrEmpty(operationName))
        {
            query = query.Where(x => x.OperationName == operationName);
        }

        return await query
            .OrderByDescending(x => x.UpdatedAt)
            .Skip(skip)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    public async Task<Dictionary<RetryStatus, int>> GetStatusCountsAsync(
        string? operationName = null,
        CancellationToken cancellationToken = default)
    {
        var query = _dbContext.RetryableOperations.AsQueryable();

        if (!string.IsNullOrEmpty(operationName))
        {
            query = query.Where(x => x.OperationName == operationName);
        }

        // Client-side grouping — InMemory provider doesn't support server-side GroupBy+ToDictionary
        var items = await query
            .Select(x => x.Status)
            .ToListAsync(cancellationToken);

        return items
            .GroupBy(s => s)
            .ToDictionary(g => g.Key, g => g.Count());
    }

    public async Task<List<RetryableOperation>> GetByFilterAsync(
        Guid? operationId = null,
        string? operationName = null,
        CancellationToken cancellationToken = default)
    {
        // Exclude active statuses — only terminal operations can be retried
        var query = _dbContext.RetryableOperations
            .Where(x => x.Status != RetryStatus.Pending
                        && x.Status != RetryStatus.InProgress);

        if (operationId.HasValue)
        {
            query = query.Where(x => x.Id == operationId.Value);
        }

        if (!string.IsNullOrEmpty(operationName))
        {
            query = query.Where(x => x.OperationName == operationName);
        }

        return await query.ToListAsync(cancellationToken);
    }

    // ── Mutations ────────────────────────────────────────

    public async Task AddAsync(RetryableOperation operation, CancellationToken cancellationToken = default)
    {
        _dbContext.RetryableOperations.Add(operation);
        await _dbContext.SaveChangesAsync(cancellationToken);
    }

    public async Task UpdateAsync(RetryableOperation operation, CancellationToken cancellationToken = default)
    {
        // EF Core tracks the entity via change detection — just save
        await _dbContext.SaveChangesAsync(cancellationToken);
    }

    public async Task UpdateRangeAsync(IEnumerable<RetryableOperation> operations, CancellationToken cancellationToken = default)
    {
        // EF Core tracks entities via change detection — just save
        await _dbContext.SaveChangesAsync(cancellationToken);
    }
}
