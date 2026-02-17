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
        services.AddSingleton<IRetryOperationHandler>(_testHandler);
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
    public async Task ProcessPending_HandlerThrows_MarksFailedWithRetry()
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

        // THEN it's marked failed with error message
        var db = await GetDbContext();
        var op = await db.RetryableOperations.SingleAsync();
        Assert.Equal(RetryStatus.Failed, op.Status);
        Assert.Equal("Connection refused", op.LastError);
        Assert.Equal(1, op.AttemptCount);
        Assert.NotNull(op.NextRetryAt); // scheduled for retry
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
                OperationName = "TestOperation", EntityKey = "failed",
                EventTimestamp = DateTimeOffset.UtcNow,
                Status = RetryStatus.Failed, MaxRetries = 3, AttemptCount = 3
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
        Assert.Equal(RetryStatus.Failed, ops.Single(x => x.EntityKey == "failed").Status);
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
}
