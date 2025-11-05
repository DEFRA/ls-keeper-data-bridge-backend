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
}