namespace KeeperData.Core.Reports.Strategies;

/// <summary>
/// Delegate for reporting analysis progress.
/// </summary>
/// <param name="recordsAnalyzed">Number of records analyzed so far.</param>
/// <param name="totalRecords">Total number of records to analyze.</param>
/// <param name="issuesFound">Number of issues found so far.</param>
/// <param name="issuesResolved">Number of issues resolved so far.</param>
public delegate Task ProgressCallback(int recordsAnalyzed, int totalRecords, int issuesFound, int issuesResolved);
