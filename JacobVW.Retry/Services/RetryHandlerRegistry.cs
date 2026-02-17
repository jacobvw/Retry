namespace JacobVW.Retry.Services;

/// <summary>
/// Maps operation names to their handler types.
/// Populated at DI registration via AddRetryHandler&lt;T&gt;().
/// </summary>
public class RetryHandlerRegistry
{
    private readonly Dictionary<string, Type> _handlers = new();

    public void Register(string operationName, Type handlerType)
    {
        _handlers[operationName] = handlerType;
    }

    public Type? GetHandlerType(string operationName)
    {
        return _handlers.GetValueOrDefault(operationName);
    }
}
