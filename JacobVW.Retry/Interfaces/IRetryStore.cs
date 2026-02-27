using JacobVW.Retry.Models;

namespace JacobVW.Retry.Interfaces;

/// <summary>
/// Provider-agnostic storage interface for retryable operations.
/// Each mutation method persists immediately (no explicit SaveChanges).
/// 
/// The default implementation is EfCoreRetryStore, but consumers can
/// implement this interface for alternative backends (Redis, MongoDB, etc.).
/// </summary>
public interface IRetryStore
{
    // ── Queries ──────────────────────────────────────────

    /// <summary>
    /// Check if a strictly newer non-failed event already exists
    /// for this entity + operation combination.
    /// Same-timestamp events are allowed through — CreatedAt ordering
    /// during processing determines which one wins.
    /// </summary>
    Task<bool> HasNewerEventAsync(
        string operationName,
        string entityKey,
        DateTimeOffset eventTimestamp,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Check if a newer completed event exists for this entity + operation.
    /// Used for supersede detection during processing.
    /// A completed event supersedes if it has a strictly newer EventTimestamp,
    /// or the same EventTimestamp but a later CreatedAt.
    /// </summary>
    Task<bool> HasNewerCompletedEventAsync(
        string operationName,
        string entityKey,
        DateTimeOffset eventTimestamp,
        DateTimeOffset createdAt,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get operations that have been stuck in InProgress for longer than
    /// <paramref name="stuckThreshold"/>. These are operations where the
    /// processor crashed before it could update the status.
    /// </summary>
    Task<List<RetryableOperation>> GetStuckInProgressAsync(
        DateTimeOffset cutoff,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get active operations (Pending/Failed/InProgress) that have
    /// passed their ExpiresAt deadline.
    /// </summary>
    Task<List<RetryableOperation>> GetOperationsPastDeadlineAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get operations ready for processing: Pending/Failed, NextRetryAt
    /// is in the past, attempt count below max, and not expired.
    /// Ordered by NextRetryAt ascending.
    /// </summary>
    Task<List<RetryableOperation>> GetProcessableOperationsAsync(
        DateTimeOffset now,
        int batchSize = 100,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get operations by status with optional operation name filter.
    /// Ordered by UpdatedAt descending.
    /// </summary>
    Task<List<RetryableOperation>> GetByStatusAsync(
        RetryStatus status,
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get permanently failed operations where attempt count >= max retries.
    /// </summary>
    Task<List<RetryableOperation>> GetPermanentlyFailedAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get count of operations grouped by status.
    /// </summary>
    Task<Dictionary<RetryStatus, int>> GetStatusCountsAsync(
        string? operationName = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get expired operations, optionally filtered by ID and/or operation name.
    /// </summary>
    Task<List<RetryableOperation>> GetExpiredByFilterAsync(
        Guid? operationId = null,
        string? operationName = null,
        CancellationToken cancellationToken = default);

    // ── Mutations (each persists immediately) ────────────

    /// <summary>
    /// Add a new operation and persist.
    /// </summary>
    Task AddAsync(RetryableOperation operation, CancellationToken cancellationToken = default);

    /// <summary>
    /// Update a single operation and persist.
    /// </summary>
    Task UpdateAsync(RetryableOperation operation, CancellationToken cancellationToken = default);

    /// <summary>
    /// Update multiple operations and persist.
    /// </summary>
    Task UpdateRangeAsync(IEnumerable<RetryableOperation> operations, CancellationToken cancellationToken = default);
}
