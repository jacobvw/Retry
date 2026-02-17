namespace JacobVW.Retry.Models;

public enum RetryStatus
{
    Pending,
    InProgress,
    Completed,
    Failed,
    Expired,
    Superseded,
    Discarded
}
