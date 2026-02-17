# JacobVW.Retry

DB-backed retry service with timestamp-aware idempotency for .NET applications.

## Features

- **DB-backed persistence** — survives app restarts, works across multiple instances
- **Timestamp-aware deduplication** — out-of-order events are automatically discarded
- **Exponential backoff with jitter** — prevents thundering herd on retries
- **Pluggable handlers** — implement `IRetryOperationHandler` for each operation type
- **One-liner setup** — `builder.Services.AddRetryService()`

## Installation

```bash
dotnet add package JacobVW.Retry
```

Or as a project reference:
```xml
<ProjectReference Include="../JacobVW.Retry/JacobVW.Retry.csproj" />
```

## Setup

### 1. Implement `IRetryDbContext` on your DbContext

```csharp
public class MyDbContext : DbContext, IRetryDbContext
{
    public DbSet<RetryableOperation> RetryableOperations { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new RetryableOperationConfiguration());
    }
}
```

### 2. Register services

```csharp
builder.Services.AddRetryService();
```

### 3. Create a handler

```csharp
public class MyOperationHandler : IRetryOperationHandler
{
    public string OperationName => "MyOperation";

    public async Task HandleAsync(RetryableOperation operation, CancellationToken ct)
    {
        var payload = JsonSerializer.Deserialize<MyPayload>(operation.SerializedPayload!);
        // do work — throw on failure to trigger retry
    }
}
```

### 4. Register your handler

```csharp
builder.Services.AddScoped<MyOperationHandler>();
builder.Services.AddScoped<IRetryOperationHandler>(sp =>
    sp.GetRequiredService<MyOperationHandler>());
```

### 5. Enqueue operations

```csharp
await retryService.EnqueueAsync(
    operationName: "MyOperation",
    entityKey: "unique-entity-id",
    eventTimestamp: DateTimeOffset.UtcNow,
    serializedPayload: JsonSerializer.Serialize(payload));
```

## Configuration

```csharp
// Custom processing interval (default: 30 seconds)
builder.Services.AddRetryService(processingInterval: TimeSpan.FromSeconds(10));
```

## License

MIT
