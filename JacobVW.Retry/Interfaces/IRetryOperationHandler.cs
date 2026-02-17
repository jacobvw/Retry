using JacobVW.Retry.Models;

namespace JacobVW.Retry.Interfaces;

/// <summary>
/// Handles a specific type of retryable operation.
/// Register one handler per OperationName.
/// </summary>
public interface IRetryOperationHandler
{
    /// <summary>
    /// The operation name this handler processes.
    /// Must match <see cref="RetryableOperation.OperationName"/>.
    /// Implement as: <c>public static string OperationName => "MyOperation";</c>
    /// </summary>
    static abstract string OperationName { get; }
    
    /// <summary>
    /// Execute the operation. Throw on failure to trigger retry.
    /// </summary>
    Task HandleAsync(
        RetryableOperation operation,
        CancellationToken cancellationToken = default);
}
