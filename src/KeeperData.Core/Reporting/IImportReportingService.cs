using KeeperData.Core.Reporting.Dtos;

namespace KeeperData.Core.Reporting;

public interface IImportReportingService
{
    // Import-level operations
    Task<ImportReport> StartImportAsync(Guid importId, string sourceType, CancellationToken ct);
    Task UpsertImportReportAsync(ImportReport report, CancellationToken ct);

    // File-level operations
    Task<bool> IsFileProcessedAsync(string fileKey, string etag, CancellationToken ct);
    Task<bool> IsFileIngestedAsync(string fileKey, string etag, CancellationToken ct);
    Task RecordFileAcquisitionAsync(Guid importId, FileAcquisitionRecord record, CancellationToken ct);
    Task RecordFileIngestionAsync(Guid importId, FileIngestionRecord record, CancellationToken ct);

    // Record-level operations
    Task RecordLineageEventAsync(RecordLineageEvent lineageEvent, CancellationToken ct);
    Task RecordLineageEventsBatchAsync(IEnumerable<RecordLineageEvent> lineageEvents, CancellationToken ct);

    // Reporting queries
    Task<ImportReport?> GetImportReportAsync(Guid importId, CancellationToken ct);
    Task<IReadOnlyList<FileProcessingReport>> GetFileReportsAsync(Guid importId, CancellationToken ct);
    Task<RecordLifecycle?> GetRecordLifecycleAsync(string collectionName, string recordId, CancellationToken ct);
    Task<IReadOnlyList<RecordLineageEvent>> GetRecordLineageAsync(string collectionName, string recordId, CancellationToken ct);
    Task<IReadOnlyList<ImportSummary>> GetImportSummariesAsync(int skip = 0, int top = 10, CancellationToken ct = default);

    /// <summary>
    /// Gets paginated lineage events for a specific record in chronological order.
    /// </summary>
    /// <param name="collectionName">The collection name</param>
    /// <param name="recordId">The record ID</param>
    /// <param name="skip">Number of events to skip (default: 0)</param>
    /// <param name="top">Number of events to return (default: 10, max: 100)</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Paginated list of lineage events with total count</returns>
    Task<PaginatedLineageEvents> GetRecordLineageEventsPaginatedAsync(
        string collectionName,
        string recordId,
        int skip = 0,
        int top = 10,
        CancellationToken ct = default);
}