namespace JacobVW.Retry.Models;

/// <summary>
/// Describes a single operation to enqueue. Used by EnqueueTransactionalAsync
/// to accept a batch of operations.
/// </summary>
public class RetryEnqueueRequest
{
    public required string OperationName { get; init; }
    public required string EntityKey { get; init; }
    public required DateTimeOffset EventTimestamp { get; init; }
    public string? SerializedPayload { get; init; }
    public int MaxRetries { get; init; } = 3;
    public TimeSpan? MaxFailedDuration { get; init; }
}
