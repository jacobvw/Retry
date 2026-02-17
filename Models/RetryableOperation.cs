using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace JacobVW.Retry.Models;

/// <summary>
/// Represents a retryable operation persisted in the database.
/// Tracks operation state, retry attempts, and timestamps
/// for ordering/deduplication.
/// </summary>
public class RetryableOperation
{
    public Guid Id { get; set; } = Guid.NewGuid();
    
    /// <summary>
    /// The type of operation (e.g. "StockUpdate", "PriceSync").
    /// Used to route to the correct IRetryOperationHandler.
    /// </summary>
    public string OperationName { get; set; } = string.Empty;
    
    /// <summary>
    /// Composite key identifying the entity being operated on.
    /// Format is handler-specific (e.g. "BMW:51357339591:Used").
    /// Used for timestamp-based deduplication.
    /// </summary>
    public string EntityKey { get; set; } = string.Empty;
    
    /// <summary>
    /// The timestamp of the source event. Used to discard
    /// out-of-order updates: if a newer event for the same
    /// EntityKey has already been processed, this one is skipped.
    /// </summary>
    public DateTimeOffset EventTimestamp { get; set; }
    
    /// <summary>
    /// JSON-serialized payload for the handler to deserialize
    /// and process. The schema is handler-specific.
    /// </summary>
    public string? SerializedPayload { get; set; }
    
    public int AttemptCount { get; set; }
    public int MaxRetries { get; set; } = 3;
    
    /// <summary>
    /// When to next attempt processing. Calculated using
    /// exponential backoff with jitter after each failure.
    /// </summary>
    public DateTimeOffset? NextRetryAt { get; set; }
    
    public DateTimeOffset? CompletedAt { get; set; }
    public string? LastError { get; set; }
    public RetryStatus Status { get; set; } = RetryStatus.Pending;
    
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
}

public class RetryableOperationConfiguration : IEntityTypeConfiguration<RetryableOperation>
{
    public void Configure(EntityTypeBuilder<RetryableOperation> builder)
    {
        builder.ToTable("RetryableOperations");
        builder.HasKey(x => x.Id);
        
        builder.Property(x => x.OperationName)
            .IsRequired()
            .HasMaxLength(128);
        
        builder.Property(x => x.EntityKey)
            .IsRequired()
            .HasMaxLength(512);
        
        builder.Property(x => x.SerializedPayload)
            .HasColumnType("text");
        
        builder.Property(x => x.LastError)
            .HasColumnType("text");
        
        // Index for timestamp-based deduplication lookups
        builder.HasIndex(x => new { x.EntityKey, x.EventTimestamp })
            .HasDatabaseName("IX_RetryableOperations_EntityKey_EventTimestamp");
        
        // Index for processing pending/failed operations
        builder.HasIndex(x => new { x.Status, x.NextRetryAt })
            .HasDatabaseName("IX_RetryableOperations_Status_NextRetryAt");
        
        // Index for operation name routing
        builder.HasIndex(x => x.OperationName)
            .HasDatabaseName("IX_RetryableOperations_OperationName");
    }
}
