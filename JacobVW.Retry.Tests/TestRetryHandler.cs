using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Models;

namespace JacobVW.Retry.Tests;

/// <summary>
/// A test handler that succeeds by default.
/// Set ShouldThrow to make it fail on demand.
/// </summary>
public class TestRetryHandler : IRetryOperationHandler
{
    public static string OperationName => "TestOperation";
    public int HandleCallCount { get; private set; }
    public bool ShouldThrow { get; set; }
    public string ThrowMessage { get; set; } = "Handler failed";
    public RetryStatus? StatusOverride { get; set; }
    public List<RetryableOperation> HandledOperations { get; } = [];

    public Task HandleAsync(RetryableOperation operation, CancellationToken cancellationToken = default)
    {
        HandleCallCount++;
        HandledOperations.Add(operation);

        if (StatusOverride.HasValue)
        {
            operation.Status = StatusOverride.Value;
        }

        if (ShouldThrow)
        {
            throw new InvalidOperationException(ThrowMessage);
        }

        return Task.CompletedTask;
    }
}
