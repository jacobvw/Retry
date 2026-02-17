using JacobVW.Retry.Models;

namespace JacobVW.Retry.Interfaces;

/// <summary>
/// DB-backed retry service with timestamp-aware idempotency.
/// Enqueue operations for reliable processing with automatic
/// retries. Out-of-order events are discarded based on timestamps.
/// </summary>
public interface IRetryService
{
    /// <summary>
    /// Enqueue an operation for processing. If a completed operation
    /// for the same entityKey with a newer timestamp already exists,
    /// the enqueue is skipped and returns false.
    /// </summary>
    /// <param name="operationName">Handler routing key (e.g. "StockUpdate")</param>
    /// <param name="entityKey">Composite entity identifier for deduplication</param>
    /// <param name="eventTimestamp">Source event timestamp for ordering</param>
    /// <param name="serializedPayload">JSON payload for the handler</param>
    /// <param name="maxRetries">Maximum retry attempts (default 3)</param>
    /// <param name="maxFailedDuration">Max time since creation before the operation expires and stops retrying (default: 24 hours)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if enqueued, false if skipped (stale event)</returns>
    Task<bool> EnqueueAsync(
        string operationName,
        string entityKey,
        DateTimeOffset eventTimestamp,
        string? serializedPayload,
        int maxRetries = 3,
        TimeSpan? maxFailedDuration = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Process all pending and retryable operations.
    /// Called by the background processor service.
    /// </summary>
    Task ProcessPendingAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get all permanently failed operations — either exceeded
    /// max retries or expired past their max failed duration.
    /// </summary>
    Task<List<RetryableOperation>> GetFailedAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Get operations that expired (ExpiresAt passed) without completing.
    /// </summary>
    Task<List<RetryableOperation>> GetExpiredAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Get operations that were superseded (skipped because a newer
    /// event for the same entity was already completed).
    /// </summary>
    Task<List<RetryableOperation>> GetSupersededAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get operations that were discarded on enqueue because a newer
    /// or equal event already existed. Stored for auditing purposes.
    /// </summary>
    Task<List<RetryableOperation>> GetDiscardedAsync(
        string? operationName = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get count of operations by status, optionally filtered by operation name.
    /// </summary>
    Task<Dictionary<RetryStatus, int>> GetStatusCountsAsync(
        string? operationName = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Re-enqueue expired operations for another attempt.
    /// Resets status to Pending, extends ExpiresAt by the given duration,
    /// and resets attempt count.
    /// </summary>
    /// <param name="operationId">Optional: retry a single operation by ID</param>
    /// <param name="operationName">Optional: retry all expired operations matching this name</param>
    /// <param name="newMaxFailedDuration">New expiry window from now (default: 24 hours)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Number of operations re-enqueued</returns>
    Task<int> RetryExpiredAsync(
        Guid? operationId = null,
        string? operationName = null,
        TimeSpan? newMaxFailedDuration = null,
        CancellationToken cancellationToken = default);
}
