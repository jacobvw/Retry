using JacobVW.Retry.Interfaces;
using JacobVW.Retry.Models;
using Microsoft.EntityFrameworkCore;

namespace JacobVW.Retry.Tests;

/// <summary>
/// In-memory test DbContext implementing IRetryDbContext
/// </summary>
public class TestDbContext : DbContext, IRetryDbContext
{
    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options) { }
    
    public DbSet<RetryableOperation> RetryableOperations { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new RetryableOperationConfiguration());
    }
}
