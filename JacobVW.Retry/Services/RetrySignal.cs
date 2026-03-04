namespace JacobVW.Retry.Services;

/// <summary>
/// Async signal that allows producers (EnqueueAsync, RetryAsync) to
/// wake the <see cref="RetryProcessorService"/> immediately instead
/// of waiting for the next polling interval.
/// Registered as a singleton so the same instance is shared between
/// the scoped <see cref="RetryService"/> and the hosted processor.
/// </summary>
public class RetrySignal
{
    private readonly SemaphoreSlim _signal = new(0);

    /// <summary>
    /// Signal the processor to wake up. Safe to call multiple times —
    /// only one release is queued at a time.
    /// </summary>
    public void Notify()
    {
        if (_signal.CurrentCount == 0)
            _signal.Release();
    }

    /// <summary>
    /// Wait for a signal or until the timeout expires, whichever comes first.
    /// Used by <see cref="RetryProcessorService"/> in place of Task.Delay
    /// so the polling interval is still honoured as a maximum wait time.
    /// </summary>
    public Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        => _signal.WaitAsync(timeout, cancellationToken);

    /// <summary>
    /// Same as <see cref="WaitAsync"/> but returns the underlying bool
    /// (true = signaled, false = timed out). Useful in tests.
    /// </summary>
    public Task<bool> WaitForTestAsync(TimeSpan timeout, CancellationToken cancellationToken)
        => _signal.WaitAsync(timeout, cancellationToken);
}
