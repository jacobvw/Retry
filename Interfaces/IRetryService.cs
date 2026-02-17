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
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if enqueued, false if skipped (stale event)</returns>
    Task<bool> EnqueueAsync(
        string operationName,
        string entityKey,
        DateTimeOffset eventTimestamp,
        string? serializedPayload,
        int maxRetries = 3,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Process all pending and retryable operations.
    /// Called by the background processor service.
    /// </summary>
    Task ProcessPendingAsync(CancellationToken cancellationToken = default);
}
