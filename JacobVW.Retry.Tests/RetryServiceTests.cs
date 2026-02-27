using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Models;
using JacobVW.Retry.Services;
using JacobVW.Retry.Stores;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace JacobVW.Retry.Tests;

public class RetryServiceTests : IDisposable
{
    private readonly ServiceProvider _serviceProvider;
    private readonly TestRetryHandler _testHandler;
    private readonly RetryService _retryService;

    public RetryServiceTests()
    {
        _testHandler = new TestRetryHandler();

        var services = new ServiceCollection();

        // Unique DB per test instance to avoid cross-test contamination
        var dbName = $"RetryTestDb_{Guid.NewGuid()}";
        services.AddDbContext<TestDbContext>(options =>
            options.UseInMemoryDatabase(dbName));
        services.AddScoped<IRetryDbContext>(sp => sp.GetRequiredService<TestDbContext>());
        services.AddScoped<IRetryStore, EfCoreRetryStore>();

        // Register handler via registry (mirrors AddRetryHandler<T> pattern)
        var registry = new RetryHandlerRegistry();
        registry.Register(TestRetryHandler.OperationName, typeof(TestRetryHandler));
        services.AddSingleton(registry);
        services.AddSingleton(_testHandler);

        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Debug));

        _serviceProvider = services.BuildServiceProvider();
        _retryService = new RetryService(_serviceProvider, 
            _serviceProvider.GetRequiredService<ILogger<RetryService>>());
    }

    public void Dispose()
    {
        _serviceProvider.Dispose();
    }

    private async Task<TestDbContext> GetDbContext()
    {
        var scope = _serviceProvider.CreateScope();
        return scope.ServiceProvider.GetRequiredService<TestDbContext>();
    }

    /// <summary>
    /// Seeds data through a proper scope lifecycle matching how RetryService resolves IRetryDbContext.
    /// </summary>
    private async Task SeedOperationAsync(RetryableOperation operation)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<IRetryDbContext>();
        db.RetryableOperations.Add(operation);
        await db.SaveChangesAsync();
    }

    private async Task SeedOperationsAsync(params RetryableOperation[] operations)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<IRetryDbContext>();
        db.RetryableOperations.AddRange(operations);
        await db.SaveChangesAsync();
    }

    // ──────────────────────────────────────────
    // ENQUEUE TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task EnqueueAsync_NewOperation_ReturnsTrue()
    {
        // GIVEN a fresh service
        // WHEN we enqueue an operation
        var result = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: "{\"value\":1}");

        // THEN it returns true
        Assert.True(result);

        // AND it's persisted in the DB
        var db = await GetDbContext();
        var count = await db.RetryableOperations.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task EnqueueAsync_StaleEvent_SkipsAndReturnsFalse()
    {
        // GIVEN an already-enqueued newer event
        var newerTimestamp = DateTimeOffset.UtcNow;
        var olderTimestamp = newerTimestamp.AddMinutes(-10);

        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: newerTimestamp,
            serializedPayload: "{\"value\":2}");

        // WHEN we enqueue an older event for the same entity
        var result = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: olderTimestamp,
            serializedPayload: "{\"value\":1}");

        // THEN it returns false (stale)
        Assert.False(result);

        // AND both are stored — newer as Pending, older as Discarded
        var db = await GetDbContext();
        var ops = await db.RetryableOperations.OrderBy(x => x.EventTimestamp).ToListAsync();
        Assert.Equal(2, ops.Count);
        Assert.Equal(RetryStatus.Discarded, ops[0].Status);
        Assert.Equal(RetryStatus.Pending, ops[1].Status);
        Assert.Contains("Discarded", ops[0].LastError);
    }

    [Fact]
    public async Task EnqueueAsync_DifferentEntityKeys_BothEnqueued()
    {
        // GIVEN / WHEN we enqueue two operations with different entity keys
        var result1 = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null);

        var result2 = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-2",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null);

        // THEN both are enqueued
        Assert.True(result1);
        Assert.True(result2);

        var db = await GetDbContext();
        Assert.Equal(2, await db.RetryableOperations.CountAsync());
    }

    [Fact]
    public async Task EnqueueAsync_SetsExpiresAtFromMaxFailedDuration()
    {
        // GIVEN a max failed duration of 2 hours
        var before = DateTimeOffset.UtcNow;

        // WHEN we enqueue
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null,
            maxFailedDuration: TimeSpan.FromHours(2));

        // THEN ExpiresAt is ~2 hours from now
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.NotNull(op.ExpiresAt);
        Assert.True(op.ExpiresAt >= before.AddHours(2));
        Assert.True(op.ExpiresAt <= DateTimeOffset.UtcNow.AddHours(2).AddSeconds(1));
    }

    [Fact]
    public async Task EnqueueAsync_DefaultExpiresAt_Is24Hours()
    {
        // WHEN we enqueue without specifying maxFailedDuration
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null);

        // THEN ExpiresAt is ~24 hours from now
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.NotNull(op.ExpiresAt);
        var expected = DateTimeOffset.UtcNow.AddHours(24);
        Assert.True(op.ExpiresAt.Value <= expected.AddSeconds(1));
        Assert.True(op.ExpiresAt.Value >= expected.AddSeconds(-2));
    }

    // ──────────────────────────────────────────
    // PROCESS PENDING TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task ProcessPending_ExecutesHandler()
    {
        // GIVEN an enqueued operation
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: "{\"data\":true}");

        // WHEN we process pending
        await _retryService.ProcessPendingAsync();

        // THEN the handler was called once
        Assert.Equal(1, _testHandler.HandleCallCount);
        Assert.Single(_testHandler.HandledOperations);

        // AND the operation is marked completed
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Completed, op.Status);
        Assert.NotNull(op.CompletedAt);
        Assert.Equal(1, op.AttemptCount);
    }

    [Fact]
    public async Task ProcessPending_HandlerThrows_MarksPendingWithNextRetryAt()
    {
        // GIVEN an enqueued operation with max 3 retries
        _testHandler.ShouldThrow = true;
        _testHandler.ThrowMessage = "Connection refused";

        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null,
            maxRetries: 3);

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN it's marked Pending (not Failed) so it will be retried again
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Pending, op.Status);
        Assert.Equal("Connection refused", op.LastError);
        Assert.Equal(1, op.AttemptCount);
        Assert.NotNull(op.NextRetryAt); // scheduled for next retry
        Assert.True(op.NextRetryAt > DateTimeOffset.UtcNow); // in the future
    }

    [Fact]
    public async Task ProcessPending_ExhaustsRetries_PermanentlyFailed()
    {
        // GIVEN an operation with 1 max retry
        _testHandler.ShouldThrow = true;

        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null,
            maxRetries: 1);

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN it's permanently failed (no NextRetryAt)
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Failed, op.Status);
        Assert.Equal(1, op.AttemptCount);
    }

    [Fact]
    public async Task ProcessPending_MaxRetriesZero_TriesOnceAndFails()
    {
        // GIVEN an operation with MaxRetries = 0 ("try once, no retries")
        _testHandler.ShouldThrow = true;

        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null,
            maxRetries: 0);

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN it was attempted exactly once and is permanently failed
        Assert.Equal(1, _testHandler.HandleCallCount);
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Failed, op.Status);
        Assert.Equal(1, op.AttemptCount);

        // AND a second process run does NOT pick it up again
        await _retryService.ProcessPendingAsync();
        Assert.Equal(1, _testHandler.HandleCallCount); // still 1
    }

    [Fact]
    public async Task ProcessPending_SupersededByNewerEvent_SkipsOlder()
    {
        // GIVEN two events for the same entity: older first, then newer
        var olderTimestamp = DateTimeOffset.UtcNow.AddMinutes(-10);
        var newerTimestamp = DateTimeOffset.UtcNow;

        // Manually insert both (bypassing timestamp check)
        // Give the newer event an earlier NextRetryAt so it processes first
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = olderTimestamp,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1),  // processes second
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            },
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = newerTimestamp,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-2), // processes first (earlier)
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            });

        // WHEN we process — newer will complete first (ordered by NextRetryAt)
        await _retryService.ProcessPendingAsync();

        // THEN handler was called twice (once for each), but the older one
        // should be marked as superseded after the newer one completes
        var db2 = await GetDbContext();
        var ops = await db2.RetryableOperations
            .OrderBy(x => x.EventTimestamp)
            .ToListAsync();

        Assert.Equal(2, ops.Count);
        // The newer one should be Completed, the older one should be Superseded
        var older = ops.First(x => x.EventTimestamp == olderTimestamp);
        var newer = ops.First(x => x.EventTimestamp == newerTimestamp);
        Assert.Equal(RetryStatus.Completed, newer.Status);
        Assert.Equal(RetryStatus.Superseded, older.Status);
        Assert.Contains("Superseded", older.LastError);
    }

    [Fact]
    public async Task ProcessPending_NoHandler_MarksFailedWithMessage()
    {
        // GIVEN an operation with no matching handler
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "UnknownOperation",
            EntityKey = "entity-1",
            EventTimestamp = DateTimeOffset.UtcNow,
            Status = RetryStatus.Pending,
            NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1),
            MaxRetries = 3,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
        });

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN it's failed with a descriptive error
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Failed, op.Status);
        Assert.Contains("No handler registered", op.LastError);
        Assert.Equal(0, _testHandler.HandleCallCount);
    }

    // ──────────────────────────────────────────
    // EXPIRY TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task ProcessPending_ExpiredOperation_MarkedExpiredNotRetried()
    {
        // GIVEN an operation that has passed its ExpiresAt
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation",
            EntityKey = "entity-1",
            EventTimestamp = DateTimeOffset.UtcNow.AddHours(-25),
            Status = RetryStatus.Pending,
            NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1),
            MaxRetries = 3,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(-1), // expired 1 hour ago
            CreatedAt = DateTimeOffset.UtcNow.AddHours(-25)
        });

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN it's marked Expired, NOT retried
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Expired, op.Status);
        Assert.Contains("expired", op.LastError, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, _testHandler.HandleCallCount);
    }

    [Fact]
    public async Task ProcessPending_NotYetExpired_StillProcessed()
    {
        // GIVEN an operation with future ExpiresAt
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null,
            maxFailedDuration: TimeSpan.FromHours(24));

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN it's completed normally
        Assert.Equal(1, _testHandler.HandleCallCount);
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Completed, op.Status);
    }

    // ──────────────────────────────────────────
    // QUERY TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task GetFailedAsync_ReturnsPermanentlyFailedOperations()
    {
        // GIVEN operations in various states
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "ok",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Completed, MaxRetries = 3, AttemptCount = 1
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "failed-1",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Failed, MaxRetries = 3, AttemptCount = 3,
                LastError = "Permanent failure"
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "failed-2",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Failed, MaxRetries = 2, AttemptCount = 2,
                LastError = "Another failure"
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "retrying",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Failed, MaxRetries = 3, AttemptCount = 1 // still retryable
            });

        // WHEN we get failed
        var failed = await _retryService.GetFailedAsync();

        // THEN only permanently failed ones are returned (attempts >= maxRetries)
        Assert.Equal(2, failed.Count);
        Assert.All(failed, op => Assert.True(op.AttemptCount >= op.MaxRetries));
    }

    [Fact]
    public async Task GetFailedAsync_FiltersByOperationName()
    {
        // GIVEN failed operations of different types
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "StockUpdate", EntityKey = "stock-1",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Failed, MaxRetries = 1, AttemptCount = 1
            },
            new RetryableOperation
            {
                OperationName = "PriceSync", EntityKey = "price-1",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Failed, MaxRetries = 1, AttemptCount = 1
            });

        // WHEN we filter by StockUpdate
        var failed = await _retryService.GetFailedAsync(operationName: "StockUpdate");

        // THEN only StockUpdate failures are returned
        Assert.Single(failed);
        Assert.Equal("StockUpdate", failed[0].OperationName);
    }

    [Fact]
    public async Task GetExpiredAsync_ReturnsExpiredOperations()
    {
        // GIVEN mixed operations
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "expired-1",
                EventTimestamp = DateTimeOffset.UtcNow.AddDays(-2),
                Status = RetryStatus.Expired, MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddDays(-1)
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "ok",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Completed, MaxRetries = 3
            });

        // WHEN
        var expired = await _retryService.GetExpiredAsync();

        // THEN only expired ones
        Assert.Single(expired);
        Assert.Equal("expired-1", expired[0].EntityKey);
    }

    [Fact]
    public async Task GetStatusCountsAsync_ReturnsCorrectCounts()
    {
        // GIVEN operations in various states
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "1",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Pending
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "2",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Pending
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "3",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Completed
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "4",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Failed
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "5",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Expired
            });

        // WHEN
        var counts = await _retryService.GetStatusCountsAsync();

        // THEN
        Assert.Equal(2, counts[RetryStatus.Pending]);
        Assert.Equal(1, counts[RetryStatus.Completed]);
        Assert.Equal(1, counts[RetryStatus.Failed]);
        Assert.Equal(1, counts[RetryStatus.Expired]);
    }

    [Fact]
    public async Task GetStatusCountsAsync_IncludesSuperseded()
    {
        // GIVEN operations with Superseded status
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "1",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Superseded
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "2",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Completed
            });

        // WHEN
        var counts = await _retryService.GetStatusCountsAsync();

        // THEN
        Assert.Equal(1, counts[RetryStatus.Superseded]);
        Assert.Equal(1, counts[RetryStatus.Completed]);
    }

    // ──────────────────────────────────────────
    // TIMESTAMP ORDERING TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task EnqueueAsync_NewerEventAfterCompletion_Enqueues()
    {
        // GIVEN a completed operation
        var firstTimestamp = DateTimeOffset.UtcNow.AddMinutes(-5);
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: firstTimestamp,
            serializedPayload: null);
        await _retryService.ProcessPendingAsync();

        // WHEN we enqueue a newer event
        var newerTimestamp = DateTimeOffset.UtcNow;
        var result = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: newerTimestamp,
            serializedPayload: null);

        // THEN it's accepted (newer than completed)
        Assert.True(result);

        var db = await GetDbContext();
        Assert.Equal(2, await db.RetryableOperations.CountAsync());
    }

    [Fact]
    public async Task EnqueueAsync_StaleEventAfterCompletion_IsBlocked()
    {
        // GIVEN a completed operation at T1
        var completedTimestamp = DateTimeOffset.UtcNow.AddMinutes(-5);
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: completedTimestamp,
            serializedPayload: null);
        await _retryService.ProcessPendingAsync();

        // WHEN a stale event (older timestamp) arrives — e.g. a delayed webhook replay
        var staleTimestamp = completedTimestamp.AddMinutes(-10);
        var result = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: staleTimestamp,
            serializedPayload: null);

        // THEN it is blocked (completed record at newer-or-equal timestamp exists)
        Assert.False(result);

        // AND it's stored as Discarded, not re-processed
        var db = await GetDbContext();
        var ops = await db.RetryableOperations.ToListAsync();
        Assert.Equal(2, ops.Count);
        Assert.Single(ops, x => x.Status == RetryStatus.Discarded);
        Assert.Single(ops, x => x.Status == RetryStatus.Completed);
        Assert.Equal(1, _testHandler.HandleCallCount); // only processed once
    }

    [Fact]
    public async Task EnqueueAsync_NewEventAfterPermanentFailure_IsAccepted()
    {
        // GIVEN a permanently failed operation (exhausted all retries)
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation",
            EntityKey = "entity-1",
            EventTimestamp = DateTimeOffset.UtcNow.AddMinutes(-5),
            Status = RetryStatus.Failed,
            MaxRetries = 1,
            AttemptCount = 1, // exhausted
            LastError = "Permanent failure"
        });

        // WHEN a new event arrives at the same timestamp
        var result = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow.AddMinutes(-5),
            serializedPayload: null);

        // THEN it IS accepted — permanently-failed records don't block new attempts
        Assert.True(result);
    }

    [Fact]
    public async Task EnqueueAsync_DifferentOperationNames_IndependentTimestamps()
    {
        // GIVEN a StockUpdate event at time T
        var timestamp = DateTimeOffset.UtcNow;
        await _retryService.EnqueueAsync(
            operationName: "StockUpdate",
            entityKey: "entity-1",
            eventTimestamp: timestamp,
            serializedPayload: null);

        // WHEN we enqueue a PriceSync for the same entity at an earlier time
        var olderTimestamp = timestamp.AddMinutes(-5);
        var result = await _retryService.EnqueueAsync(
            operationName: "PriceSync",
            entityKey: "entity-1",
            eventTimestamp: olderTimestamp,
            serializedPayload: null);

        // THEN it's accepted (different operation name = independent)
        Assert.True(result);
    }

    // ──────────────────────────────────────────
    // SUPERSEDED QUERY TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task GetSupersededAsync_ReturnsSupersededOperations()
    {
        // GIVEN mixed operations including superseded
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "old-1",
                EventTimestamp = DateTimeOffset.UtcNow.AddMinutes(-10),
                Status = RetryStatus.Superseded,
                LastError = "Superseded by newer event"
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "old-1",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Completed
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "other",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Pending
            });

        // WHEN
        var superseded = await _retryService.GetSupersededAsync();

        // THEN only superseded ones
        Assert.Single(superseded);
        Assert.Equal("old-1", superseded[0].EntityKey);
        Assert.Equal(RetryStatus.Superseded, superseded[0].Status);
    }

    [Fact]
    public async Task GetSupersededAsync_FiltersByOperationName()
    {
        // GIVEN superseded operations of different types
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "StockUpdate", EntityKey = "stock-old",
                EventTimestamp = DateTimeOffset.UtcNow.AddMinutes(-10),
                Status = RetryStatus.Superseded
            },
            new RetryableOperation
            {
                OperationName = "PriceSync", EntityKey = "price-old",
                EventTimestamp = DateTimeOffset.UtcNow.AddMinutes(-10),
                Status = RetryStatus.Superseded
            });

        // WHEN filtering by StockUpdate
        var superseded = await _retryService.GetSupersededAsync(operationName: "StockUpdate");

        // THEN only StockUpdate superseded returned
        Assert.Single(superseded);
        Assert.Equal("StockUpdate", superseded[0].OperationName);
    }

    // ──────────────────────────────────────────
    // RETRY EXPIRED TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task RetryExpiredAsync_ReEnqueuesAllExpired()
    {
        // GIVEN two expired operations
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "exp-1",
                EventTimestamp = DateTimeOffset.UtcNow.AddDays(-2),
                Status = RetryStatus.Expired, MaxRetries = 3, AttemptCount = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddDays(-1),
                LastError = "Operation expired"
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "exp-2",
                EventTimestamp = DateTimeOffset.UtcNow.AddDays(-1),
                Status = RetryStatus.Expired, MaxRetries = 3, AttemptCount = 2,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(-1),
                LastError = "Operation expired"
            });

        // WHEN we retry all expired
        var count = await _retryService.RetryExpiredAsync();

        // THEN both are re-enqueued
        Assert.Equal(2, count);

        // AND they are reset to Pending with fresh state
        var db = await GetDbContext();
        var ops = await db.RetryableOperations.ToListAsync();
        Assert.All(ops, op =>
        {
            Assert.Equal(RetryStatus.Pending, op.Status);
            Assert.Equal(0, op.AttemptCount);
            Assert.Null(op.LastError);
            Assert.NotNull(op.ExpiresAt);
            Assert.True(op.ExpiresAt > DateTimeOffset.UtcNow);
            Assert.NotNull(op.NextRetryAt);
        });
    }

    [Fact]
    public async Task RetryExpiredAsync_ById_OnlyRetiresSingleOperation()
    {
        // GIVEN two expired operations
        var targetId = Guid.NewGuid();
        await SeedOperationsAsync(
            new RetryableOperation
            {
                Id = targetId,
                OperationName = "TestOperation", EntityKey = "exp-target",
                EventTimestamp = DateTimeOffset.UtcNow.AddDays(-2),
                Status = RetryStatus.Expired, MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddDays(-1)
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "exp-other",
                EventTimestamp = DateTimeOffset.UtcNow.AddDays(-1),
                Status = RetryStatus.Expired, MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(-1)
            });

        // WHEN we retry only the target
        var count = await _retryService.RetryExpiredAsync(operationId: targetId);

        // THEN only 1 re-enqueued
        Assert.Equal(1, count);

        // AND only the target is Pending, other stays Expired
        var db = await GetDbContext();
        var target = await db.RetryableOperations.SingleAsync(x => x.Id == targetId);
        var other = await db.RetryableOperations.SingleAsync(x => x.EntityKey == "exp-other");
        Assert.Equal(RetryStatus.Pending, target.Status);
        Assert.Equal(RetryStatus.Expired, other.Status);
    }

    [Fact]
    public async Task RetryExpiredAsync_ByOperationName_OnlyRetriesMatchingName()
    {
        // GIVEN expired operations of different types
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "StockUpdate", EntityKey = "stock-exp",
                EventTimestamp = DateTimeOffset.UtcNow.AddDays(-1),
                Status = RetryStatus.Expired, MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(-1)
            },
            new RetryableOperation
            {
                OperationName = "PriceSync", EntityKey = "price-exp",
                EventTimestamp = DateTimeOffset.UtcNow.AddDays(-1),
                Status = RetryStatus.Expired, MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(-1)
            });

        // WHEN we retry only StockUpdate expired
        var count = await _retryService.RetryExpiredAsync(operationName: "StockUpdate");

        // THEN only 1 re-enqueued
        Assert.Equal(1, count);

        var db = await GetDbContext();
        var stock = await db.RetryableOperations.SingleAsync(x => x.OperationName == "StockUpdate");
        var price = await db.RetryableOperations.SingleAsync(x => x.OperationName == "PriceSync");
        Assert.Equal(RetryStatus.Pending, stock.Status);
        Assert.Equal(RetryStatus.Expired, price.Status);
    }

    [Fact]
    public async Task RetryExpiredAsync_CustomDuration_SetsNewExpiresAt()
    {
        // GIVEN an expired operation
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation", EntityKey = "exp-custom",
            EventTimestamp = DateTimeOffset.UtcNow.AddDays(-1),
            Status = RetryStatus.Expired, MaxRetries = 3,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(-1)
        });

        var before = DateTimeOffset.UtcNow;

        // WHEN we retry with a 4-hour window
        await _retryService.RetryExpiredAsync(newMaxFailedDuration: TimeSpan.FromHours(4));

        // THEN ExpiresAt is ~4 hours from now
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.True(op.ExpiresAt >= before.AddHours(4));
        Assert.True(op.ExpiresAt <= DateTimeOffset.UtcNow.AddHours(4).AddSeconds(1));
    }

    [Fact]
    public async Task RetryExpiredAsync_NoMatches_ReturnsZero()
    {
        // GIVEN no expired operations (only completed)
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation", EntityKey = "ok",
            EventTimestamp = DateTimeOffset.UtcNow,
            Status = RetryStatus.Completed, MaxRetries = 3
        });

        // WHEN we try to retry expired
        var count = await _retryService.RetryExpiredAsync();

        // THEN nothing is retried
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task RetryExpiredAsync_DoesNotAffectNonExpiredStatuses()
    {
        // GIVEN operations in various non-expired statuses
        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "pending",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Pending
            },
            new RetryableOperation
            {
                // Mid-retry (pending, still has attempts remaining) — should NOT be affected
                OperationName = "TestOperation", EntityKey = "pending-mid-retry",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Pending, MaxRetries = 3, AttemptCount = 1,
                NextRetryAt = DateTimeOffset.UtcNow.AddMinutes(5)
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "completed",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Completed
            },
            new RetryableOperation
            {
                OperationName = "TestOperation", EntityKey = "superseded",
                EventTimestamp = DateTimeOffset.UtcNow, Status = RetryStatus.Superseded
            });

        // WHEN
        var count = await _retryService.RetryExpiredAsync();

        // THEN none are affected
        Assert.Equal(0, count);

        var db = await GetDbContext();
        var ops = await db.RetryableOperations.ToListAsync();
        Assert.Equal(RetryStatus.Pending, ops.Single(x => x.EntityKey == "pending").Status);
        Assert.Equal(RetryStatus.Pending, ops.Single(x => x.EntityKey == "pending-mid-retry").Status);
        Assert.Equal(RetryStatus.Completed, ops.Single(x => x.EntityKey == "completed").Status);
        Assert.Equal(RetryStatus.Superseded, ops.Single(x => x.EntityKey == "superseded").Status);
    }

    [Fact]
    public async Task RetryExpiredAsync_ThenProcessPending_ExecutesHandler()
    {
        // GIVEN an expired operation
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation", EntityKey = "exp-then-process",
            EventTimestamp = DateTimeOffset.UtcNow.AddDays(-1),
            Status = RetryStatus.Expired, MaxRetries = 3, AttemptCount = 3,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(-1),
            LastError = "Operation expired"
        });

        // WHEN we retry it, then process pending
        await _retryService.RetryExpiredAsync();
        await _retryService.ProcessPendingAsync();

        // THEN the handler was called and operation completed
        Assert.Equal(1, _testHandler.HandleCallCount);
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Completed, op.Status);
        Assert.Equal(1, op.AttemptCount); // reset from 3 to 0, then incremented to 1
    }

    // ──────────────────────────────────────────
    // DISCARDED EVENT TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task GetDiscardedAsync_ReturnsDiscardedOperations()
    {
        // GIVEN a newer event followed by a stale one (which gets discarded)
        var newerTimestamp = DateTimeOffset.UtcNow;
        var olderTimestamp = newerTimestamp.AddMinutes(-10);

        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: newerTimestamp,
            serializedPayload: null);

        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: olderTimestamp,
            serializedPayload: null);

        // WHEN we query discarded
        var discarded = await _retryService.GetDiscardedAsync();

        // THEN only the stale one is returned
        Assert.Single(discarded);
        Assert.Equal(RetryStatus.Discarded, discarded[0].Status);
        Assert.Equal(olderTimestamp, discarded[0].EventTimestamp);
    }

    [Fact]
    public async Task GetDiscardedAsync_FiltersByOperationName()
    {
        // GIVEN discarded events of different types
        await _retryService.EnqueueAsync("StockUpdate", "e1", DateTimeOffset.UtcNow, null);
        await _retryService.EnqueueAsync("StockUpdate", "e1", DateTimeOffset.UtcNow.AddMinutes(-5), null);

        await _retryService.EnqueueAsync("PriceSync", "e2", DateTimeOffset.UtcNow, null);
        await _retryService.EnqueueAsync("PriceSync", "e2", DateTimeOffset.UtcNow.AddMinutes(-5), null);

        // WHEN filtering
        var discarded = await _retryService.GetDiscardedAsync(operationName: "StockUpdate");

        // THEN only StockUpdate discarded returned
        Assert.Single(discarded);
        Assert.Equal("StockUpdate", discarded[0].OperationName);
    }

    [Fact]
    public async Task EnqueueAsync_DiscardedEvent_PreservesPayload()
    {
        // GIVEN a newer event
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: "{\"newer\":true}");

        // WHEN we enqueue a stale event with payload
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow.AddMinutes(-10),
            serializedPayload: "{\"stale\":true}");

        // THEN the discarded record preserves the payload for auditing
        var discarded = await _retryService.GetDiscardedAsync();
        Assert.Single(discarded);
        Assert.Equal("{\"stale\":true}", discarded[0].SerializedPayload);
    }

    [Fact]
    public async Task EnqueueAsync_DiscardedEvent_NotProcessed()
    {
        // GIVEN a newer event and a discarded stale one
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null);

        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow.AddMinutes(-10),
            serializedPayload: null);

        // WHEN we process pending
        await _retryService.ProcessPendingAsync();

        // THEN handler was called once (only the Pending one, not Discarded)
        Assert.Equal(1, _testHandler.HandleCallCount);

        // AND the discarded one is still Discarded
        var discarded = await _retryService.GetDiscardedAsync();
        Assert.Single(discarded);
        Assert.Equal(RetryStatus.Discarded, discarded[0].Status);
    }

    // ──────────────────────────────────────────
    // SAME-TIMESTAMP EVENT TESTS
    // ──────────────────────────────────────────

    [Fact]
    public async Task EnqueueAsync_SameTimestamp_BothEnqueued()
    {
        // GIVEN an event at timestamp T
        var timestamp = DateTimeOffset.UtcNow;
        var result1 = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: timestamp,
            serializedPayload: "{\"first\":true}");

        // WHEN a second event arrives at the same timestamp
        var result2 = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: timestamp,
            serializedPayload: "{\"second\":true}");

        // THEN both are enqueued (not discarded)
        Assert.True(result1);
        Assert.True(result2);

        var db = await GetDbContext();
        var ops = await db.RetryableOperations.ToListAsync();
        Assert.Equal(2, ops.Count);
        Assert.All(ops, op => Assert.Equal(RetryStatus.Pending, op.Status));
    }

    [Fact]
    public async Task ProcessPending_SameTimestamp_LaterCreatedSupersedes()
    {
        // GIVEN two events with the same EventTimestamp but different CreatedAt
        var timestamp = DateTimeOffset.UtcNow.AddMinutes(-5);
        var earlierCreated = DateTimeOffset.UtcNow.AddMinutes(-2);
        var laterCreated = DateTimeOffset.UtcNow.AddMinutes(-1);

        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = timestamp,
                CreatedAt = earlierCreated,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1), // processes second
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            },
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = timestamp,
                CreatedAt = laterCreated,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-2), // processes first
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            });

        // WHEN we process — later-created completes first
        await _retryService.ProcessPendingAsync();

        // THEN earlier-created is superseded by the later-created completed event
        var db = await GetDbContext();
        var ops = await db.RetryableOperations
            .OrderBy(x => x.CreatedAt)
            .ToListAsync();

        var earlier = ops[0];
        var later = ops[1];

        Assert.Equal(RetryStatus.Superseded, earlier.Status);
        Assert.Equal(RetryStatus.Completed, later.Status);
    }

    [Fact]
    public async Task ProcessPending_SameTimestamp_EarlierCreatedCompletesFirst_BothComplete()
    {
        // GIVEN two events with the same EventTimestamp but different CreatedAt
        var timestamp = DateTimeOffset.UtcNow.AddMinutes(-5);
        var earlierCreated = DateTimeOffset.UtcNow.AddMinutes(-2);
        var laterCreated = DateTimeOffset.UtcNow.AddMinutes(-1);

        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = timestamp,
                CreatedAt = earlierCreated,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-2), // processes first
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            },
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = timestamp,
                CreatedAt = laterCreated,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1), // processes second
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            });

        // WHEN we process — earlier-created completes first
        await _retryService.ProcessPendingAsync();

        // THEN later-created is NOT superseded (it has the newer CreatedAt)
        // Both end up Completed since the handler always API-fetches fresh data
        var db = await GetDbContext();
        var ops = await db.RetryableOperations
            .OrderBy(x => x.CreatedAt)
            .ToListAsync();

        var earlier = ops[0];
        var later = ops[1];

        Assert.Equal(RetryStatus.Completed, earlier.Status);
        Assert.Equal(RetryStatus.Completed, later.Status);
    }

    [Fact]
    public async Task EnqueueAsync_SameTimestamp_AfterCompletion_StillEnqueued()
    {
        // GIVEN an event at timestamp T that has already been processed to Completed
        var timestamp = DateTimeOffset.UtcNow.AddMinutes(-5);
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: timestamp,
            serializedPayload: "{\"first\":true}");
        await _retryService.ProcessPendingAsync();

        // WHEN a second event arrives at the same timestamp
        var result = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: timestamp,
            serializedPayload: "{\"second\":true}");

        // THEN it is still enqueued (same timestamp is not blocked by >)
        Assert.True(result);

        var db = await GetDbContext();
        var ops = await db.RetryableOperations.ToListAsync();
        Assert.Equal(2, ops.Count);
        Assert.Single(ops, x => x.Status == RetryStatus.Completed);
        Assert.Single(ops, x => x.Status == RetryStatus.Pending);
    }

    [Fact]
    public async Task EnqueueAsync_SameTimestamp_WhileInProgress_StillEnqueued()
    {
        // GIVEN an event at timestamp T that is currently InProgress
        var timestamp = DateTimeOffset.UtcNow.AddMinutes(-5);
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation",
            EntityKey = "entity-1",
            EventTimestamp = timestamp,
            Status = RetryStatus.InProgress,
            AttemptCount = 1,
            MaxRetries = 3,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(24),
            UpdatedAt = DateTimeOffset.UtcNow // recently started, not stuck
        });

        // WHEN a second event arrives at the same timestamp
        var result = await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: timestamp,
            serializedPayload: "{\"second\":true}");

        // THEN it is still enqueued (same timestamp is not blocked by >)
        Assert.True(result);

        var db = await GetDbContext();
        var ops = await db.RetryableOperations.ToListAsync();
        Assert.Equal(2, ops.Count);
        Assert.Single(ops, x => x.Status == RetryStatus.InProgress);
        Assert.Single(ops, x => x.Status == RetryStatus.Pending);
    }

    [Fact]
    public async Task ProcessPending_SameTimestamp_EarlierCreatedFailsPermanently_LaterCreatedStillCompletes()
    {
        // GIVEN two events with the same EventTimestamp
        // The earlier-created one has already permanently failed
        var timestamp = DateTimeOffset.UtcNow.AddMinutes(-5);
        var earlierCreated = DateTimeOffset.UtcNow.AddMinutes(-2);
        var laterCreated = DateTimeOffset.UtcNow.AddMinutes(-1);

        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = timestamp,
                CreatedAt = earlierCreated,
                Status = RetryStatus.Failed,  // permanently failed
                AttemptCount = 3,
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24),
                LastError = "Permanent failure"
            },
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = timestamp,
                CreatedAt = laterCreated,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1),
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            });

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN later-created completes (Failed does NOT supersede)
        var db = await GetDbContext();
        var ops = await db.RetryableOperations
            .OrderBy(x => x.CreatedAt)
            .ToListAsync();

        Assert.Equal(RetryStatus.Failed, ops[0].Status);     // earlier stays Failed
        Assert.Equal(RetryStatus.Completed, ops[1].Status);   // later completes normally
    }

    [Fact]
    public async Task ProcessPending_SameTimestamp_LaterCreatedFailsPermanently_EarlierCreatedStillCompletes()
    {
        // GIVEN two events with the same EventTimestamp
        // The later-created one has already permanently failed
        var timestamp = DateTimeOffset.UtcNow.AddMinutes(-5);
        var earlierCreated = DateTimeOffset.UtcNow.AddMinutes(-2);
        var laterCreated = DateTimeOffset.UtcNow.AddMinutes(-1);

        await SeedOperationsAsync(
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = timestamp,
                CreatedAt = earlierCreated,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1),
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            },
            new RetryableOperation
            {
                OperationName = "TestOperation",
                EntityKey = "entity-1",
                EventTimestamp = timestamp,
                CreatedAt = laterCreated,
                Status = RetryStatus.Failed,  // permanently failed
                AttemptCount = 3,
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24),
                LastError = "Permanent failure"
            });

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN earlier-created completes (Failed does NOT supersede)
        var db = await GetDbContext();
        var ops = await db.RetryableOperations
            .OrderBy(x => x.CreatedAt)
            .ToListAsync();

        Assert.Equal(RetryStatus.Completed, ops[0].Status);  // earlier completes normally
        Assert.Equal(RetryStatus.Failed, ops[1].Status);      // later stays Failed
    }

    // ──────────────────────────────────────────
    // BUG REGRESSION TESTS
    // ──────────────────────────────────────────

    /// <summary>
    /// Regression: previously a COMPLETED operation would retain the LastError
    /// from a prior failed attempt, causing the UI to display an error on a
    /// successfully completed row.
    /// </summary>
    [Fact]
    public async Task ProcessPending_SuccessAfterPriorFailure_ClearsLastError()
    {
        // GIVEN an operation that previously failed (has a stale LastError)
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation",
            EntityKey = "entity-1",
            EventTimestamp = DateTimeOffset.UtcNow.AddMinutes(-5),
            Status = RetryStatus.Pending,
            NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1),
            MaxRetries = 3,
            AttemptCount = 1,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(24),
            LastError = "Shopify price update failed for SKU=SD221452" // stale error from attempt 1
        });

        // Handler succeeds this time
        _testHandler.ShouldThrow = false;

        // WHEN we process
        await _retryService.ProcessPendingAsync();

        // THEN operation is Completed AND LastError is cleared
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Completed, op.Status);
        Assert.Null(op.LastError); // ← was previously non-null (the bug)
        Assert.NotNull(op.CompletedAt);
    }

    /// <summary>
    /// Regression: previously the mid-retry status was set to Failed (not Pending),
    /// so the processor would never pick it up again — the operation was silently
    /// abandoned despite remaining attempts.
    /// </summary>
    [Fact]
    public async Task ProcessPending_TransientFailure_OperationIsPickedUpOnNextRun()
    {
        // GIVEN an operation with 2 max retries
        await _retryService.EnqueueAsync(
            operationName: "TestOperation",
            entityKey: "entity-1",
            eventTimestamp: DateTimeOffset.UtcNow,
            serializedPayload: null,
            maxRetries: 2);

        // WHEN attempt 1 fails transiently
        _testHandler.ShouldThrow = true;
        await _retryService.ProcessPendingAsync();

        // THEN status must be Pending so the processor will pick it up again
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Pending, op.Status); // ← was Failed (the bug)
        Assert.Equal(1, op.AttemptCount);

        // AND when the next run fires (after NextRetryAt), it should succeed
        _testHandler.ShouldThrow = false;

        // Fast-forward NextRetryAt so the processor picks it up now
        op.NextRetryAt = DateTimeOffset.UtcNow.AddSeconds(-1);
        await using (var scope = _serviceProvider.CreateAsyncScope())
        {
            var ctx = scope.ServiceProvider.GetRequiredService<IRetryDbContext>();
            ctx.RetryableOperations.Update(op);
            await ctx.SaveChangesAsync();
        }

        await _retryService.ProcessPendingAsync();

        // THEN the operation eventually completes (was not silently abandoned)
        var db2 = await GetDbContext();
        var op2 = await db2.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Completed, op2.Status);
        Assert.Equal(2, op2.AttemptCount);
        Assert.Null(op2.LastError);
    }

    /// <summary>
    /// Regression: Pending operations with a future NextRetryAt must NOT be
    /// processed early, even though they are in Pending status.
    /// </summary>
    [Fact]
    public async Task ProcessPending_PendingWithFutureNextRetryAt_IsNotProcessed()
    {
        // GIVEN an operation waiting for its backoff window (e.g. after a transient failure)
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation",
            EntityKey = "entity-1",
            EventTimestamp = DateTimeOffset.UtcNow.AddMinutes(-10),
            Status = RetryStatus.Pending,
            NextRetryAt = DateTimeOffset.UtcNow.AddMinutes(5), // ← still in the future
            MaxRetries = 3,
            AttemptCount = 1,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(24),
            LastError = "Shopify price update failed for SKU=SD221452"
        });

        // WHEN we run the processor now
        await _retryService.ProcessPendingAsync();

        // THEN the handler was NOT called (NextRetryAt hasn't passed yet)
        Assert.Equal(0, _testHandler.HandleCallCount);

        // AND the operation is still Pending with the same error
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Pending, op.Status);
        Assert.Equal(1, op.AttemptCount);
    }

    /// <summary>
    /// Regression: an operation left in InProgress (e.g. after a process crash)
    /// must be detected and reset to Pending so it is re-executed.
    /// </summary>
    [Fact]
    public async Task ProcessPending_StuckInProgress_IsResetAndReprocessed()
    {
        // GIVEN an operation that was set to InProgress 10 minutes ago and never updated
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation",
            EntityKey = "entity-stuck",
            EventTimestamp = DateTimeOffset.UtcNow.AddMinutes(-15),
            Status = RetryStatus.InProgress,
            AttemptCount = 1,
            MaxRetries = 3,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(24),
            // UpdatedAt in the past — older than the 5-min default threshold
            UpdatedAt = DateTimeOffset.UtcNow.AddMinutes(-10)
        });

        // Handler succeeds on recovery
        _testHandler.ShouldThrow = false;

        // WHEN the processor runs
        await _retryService.ProcessPendingAsync();

        // THEN the stuck op is recovered: reset to Pending, then immediately processed to Completed
        Assert.Equal(1, _testHandler.HandleCallCount);

        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Completed, op.Status);
        Assert.Null(op.LastError);
    }

    /// <summary>
    /// A recently-started InProgress operation (within the stuck threshold)
    /// must NOT be reset — it may still be running.
    /// </summary>
    [Fact]
    public async Task ProcessPending_RecentInProgress_IsLeftAlone()
    {
        // GIVEN an operation that only just started (30 seconds ago)
        await SeedOperationAsync(new RetryableOperation
        {
            OperationName = "TestOperation",
            EntityKey = "entity-recent",
            EventTimestamp = DateTimeOffset.UtcNow.AddMinutes(-1),
            Status = RetryStatus.InProgress,
            AttemptCount = 1,
            MaxRetries = 3,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(24),
            UpdatedAt = DateTimeOffset.UtcNow.AddSeconds(-30) // well within the 5-min threshold
        });

        // WHEN the processor runs
        await _retryService.ProcessPendingAsync();

        // THEN the handler is NOT called and status is still InProgress
        Assert.Equal(0, _testHandler.HandleCallCount);

        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.InProgress, op.Status);
    }

    /// <summary>
    /// Regression: if the handler's DI construction throws, the exception was
    /// previously unhandled (outside the try/catch), leaving the operation in
    /// InProgress with AttemptCount incremented but no LastError and no handler
    /// invocation. Now it is caught, LastError is set, and status is updated.
    /// </summary>
    [Fact]
    public async Task ProcessPending_HandlerConstructionThrows_RecordedAsFailed()
    {
        // GIVEN a service setup where the handler registration throws on construction
        var services = new ServiceCollection();
        var dbName = $"RetryTestDb_BrokenHandler_{Guid.NewGuid()}";
        services.AddDbContext<TestDbContext>(opts => opts.UseInMemoryDatabase(dbName));
        services.AddScoped<IRetryDbContext>(sp => sp.GetRequiredService<TestDbContext>());
        services.AddScoped<IRetryStore, EfCoreRetryStore>();

        var registry = new RetryHandlerRegistry();
        registry.Register("BrokenOperation", typeof(BrokenConstructorHandler));
        services.AddSingleton(registry);
        // BrokenConstructorHandler is NOT registered in DI — GetRequiredService will throw
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Debug));

        await using var sp = services.BuildServiceProvider();
        var retryService = new RetryService(sp, sp.GetRequiredService<ILogger<RetryService>>());

        // Seed a pending operation for the broken handler
        await using (var scope = sp.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<IRetryDbContext>();
            db.RetryableOperations.Add(new RetryableOperation
            {
                OperationName = "BrokenOperation",
                EntityKey = "entity-1",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Pending,
                NextRetryAt = DateTimeOffset.UtcNow.AddHours(-1),
                MaxRetries = 3,
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(24)
            });
            await db.SaveChangesAsync();
        }

        // WHEN we process — handler DI resolution will throw
        await retryService.ProcessPendingAsync();

        // THEN the operation is NOT stuck in InProgress — it's Pending (retryable)
        // with a LastError explaining what went wrong
        await using var verifyScope = sp.CreateAsyncScope();
        var verifyDb = verifyScope.ServiceProvider.GetRequiredService<TestDbContext>();
        var op = await verifyDb.RetryableOperations.SingleAsync();
        Assert.NotEqual(RetryStatus.InProgress, op.Status); // not stuck
        Assert.Equal(1, op.AttemptCount);                   // attempt was counted
        Assert.NotNull(op.LastError);                       // error was recorded
    }
}

/// <summary>Helper: a handler type intentionally NOT registered in DI to simulate construction failure.</summary>
internal class BrokenConstructorHandler : IRetryOperationHandler
{
    public static string OperationName => "BrokenOperation";
    public Task HandleAsync(RetryableOperation operation, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}
