namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Responsible for recording and resolving cleanse issues.
/// </summary>
public interface IIssueRecorder
{
    /// <summary>
    /// Records an issue, creating it if new or reactivating if previously resolved.
    /// </summary>
    /// <param name="thumbprint">Unique identifier for the issue.</param>
    /// <param name="issueCode">The issue code.</param>
    /// <param name="lidFullIdentifier">The LID full identifier associated with the issue.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result indicating what action was taken.</returns>
    Task<IssueRecordResult> RecordIssueAsync(
        string thumbprint,
        string issueCode,
        LidFullIdentifier lidFullIdentifier,
        CancellationToken ct);

    /// <summary>
    /// Resolves an issue if it exists and is currently active.
    /// </summary>
    /// <param name="thumbprint">Unique identifier for the issue.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result indicating what action was taken.</returns>
    Task<IssueRecordResult> ResolveIssueIfExistsAsync(string thumbprint, CancellationToken ct);
}
