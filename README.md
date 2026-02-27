# JacobVW.Retry

DB-backed retry service with timestamp-aware idempotency for .NET applications.

## Features

- **Pluggable storage backend** — ships with EF Core, implement `IRetryStore` for Redis/MongoDB/etc.
- **Timestamp-aware deduplication** — out-of-order events are stored as `Discarded` for auditing
- **Same-timestamp support** — multiple events at the same timestamp are all processed; `CreatedAt` is used as a tiebreaker
- **Expiry mechanism** — operations stop retrying after a configurable deadline
- **Supersede detection** — older events are marked `Superseded` when newer ones complete
- **Exponential backoff with jitter** — prevents thundering herd on retries
- **Pluggable handlers** — implement `IRetryOperationHandler` for each operation type
- **Query API** — check failed, expired, superseded, and discarded operations; get status counts
- **Re-enqueue expired** — retry expired operations with a new deadline

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
builder.Services.AddRetryService<MyDbContext>();
```

This registers:
- `IRetryStore` → `EfCoreRetryStore` (default storage backend)
- `IRetryService` → `RetryService`
- `RetryProcessorService` (background worker)

### 3. Create a handler

```csharp
public class StockUpdateHandler : IRetryOperationHandler
{
    public static string OperationName => "StockUpdate";

    public async Task HandleAsync(RetryableOperation operation, CancellationToken ct)
    {
        var payload = JsonSerializer.Deserialize<StockPayload>(operation.SerializedPayload!);
        // Do work — throw on failure to trigger retry
    }
}
```

### 4. Register your handler

```csharp
builder.Services.AddRetryHandler<StockUpdateHandler>();
```

### 5. Enqueue operations

```csharp
await retryService.EnqueueAsync(
    operationName: "StockUpdate",
    entityKey: "BMW:51357339591",
    eventTimestamp: DateTimeOffset.UtcNow,
    serializedPayload: JsonSerializer.Serialize(payload),
    maxRetries: 5,
    maxFailedDuration: TimeSpan.FromHours(12));
```

## Query API

```csharp
// Get permanently failed operations (exhausted all retries)
var failed = await retryService.GetFailedAsync(operationName: "StockUpdate");

// Get expired operations (past deadline)
var expired = await retryService.GetExpiredAsync();

// Get superseded operations (skipped — newer event already completed)
var superseded = await retryService.GetSupersededAsync();

// Get discarded operations (stale events stored for auditing)
var discarded = await retryService.GetDiscardedAsync();

// Get counts by status
var counts = await retryService.GetStatusCountsAsync();
// counts[RetryStatus.Pending], counts[RetryStatus.Failed], etc.
```

## Re-enqueue Expired Operations

```csharp
// Retry all expired operations with a new 24h deadline (default)
int count = await retryService.RetryExpiredAsync();

// Retry a single expired operation by ID
await retryService.RetryExpiredAsync(operationId: someGuid);

// Retry all expired of a specific type with a custom deadline
await retryService.RetryExpiredAsync(
    operationName: "StockUpdate",
    newMaxFailedDuration: TimeSpan.FromHours(4));
```

## Operation Statuses

| Status | Meaning |
|---|---|
| `Pending` | Waiting to be processed |
| `InProgress` | Currently being handled |
| `Completed` | Successfully processed |
| `Failed` | Handler threw — will retry if attempts remain |
| `Expired` | Past `ExpiresAt` deadline, stopped retrying |
| `Superseded` | A newer event for the same entity completed during processing |
| `Discarded` | Stale event — a strictly newer event already existed at enqueue time |

> **Superseded vs Discarded:** `Discarded` events are caught at enqueue time (never processed). `Superseded` events were queued first but a newer event completed before they were processed.

## Same-Timestamp Events

When multiple events arrive for the same entity with identical `EventTimestamp` values (e.g. a webhook provider fires multiple updates simultaneously):

1. **Enqueue** — all same-timestamp events are accepted. Only a *strictly newer* timestamp blocks enqueue.
2. **Processing** — `CreatedAt` (set automatically when the operation is created) acts as a tiebreaker. If a same-timestamp event with a later `CreatedAt` has already completed, earlier-created events are marked `Superseded`.
3. **Handlers should be idempotent** — since same-timestamp events both run, handlers should fetch the latest state from the source API rather than relying on the webhook payload.

```
Event A (timestamp=T, CreatedAt=09:00:01) ─┐
                                            ├─ Both enqueued as Pending
Event B (timestamp=T, CreatedAt=09:00:02) ─┘

If B completes first → A is Superseded (B has later CreatedAt)
If A completes first → B also completes  (B has later CreatedAt, not superseded)
```

## Custom Storage Backend

To use a different backend (Redis, MongoDB, etc.), implement `IRetryStore` and use the non-generic `AddRetryService()` overload:

```csharp
builder.Services.AddRetryService(); // no EF Core store registered
builder.Services.AddScoped<IRetryStore, RedisRetryStore>();
```

`IRetryStore` has 11 methods — 8 queries and 3 mutations. Each mutation must persist immediately (no `SaveChanges` concept).

## Configuration

```csharp
// Custom processing interval (default: 30 seconds)
builder.Services.AddRetryService(processingInterval: TimeSpan.FromSeconds(10));
```

## Supported Frameworks

- .NET 9.0
- .NET 10.0

## License

MIT
