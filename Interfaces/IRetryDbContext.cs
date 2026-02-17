using JacobVW.Retry.Models;
using Microsoft.EntityFrameworkCore;

namespace JacobVW.Retry.Interfaces;

/// <summary>
/// Abstraction over the DbContext so JacobVW.Retry doesn't
/// own the context. Consuming projects implement this on
/// their existing DbContext.
/// </summary>
public interface IRetryDbContext
{
    DbSet<RetryableOperation> RetryableOperations { get; set; }
    Task<int> SaveChangesAsync(CancellationToken cancellationToken = default);
}
