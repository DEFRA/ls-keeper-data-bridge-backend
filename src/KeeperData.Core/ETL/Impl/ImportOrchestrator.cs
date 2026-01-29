using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Telemetry;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.ETL.Impl;

[ExcludeFromCodeCoverage(Justification = "Import orchestrator with complex pipeline dependencies - covered by integration tests.")]
public class ImportOrchestrator(
    IAcquisitionPipeline acquisitionPipeline,
    IIngestionPipeline ingestionPipeline,
    IImportReportingService reportingService,
    IApplicationMetrics metrics,
    ILogger<ImportOrchestrator> logger) : IImportOrchestrator
{
    public async Task StartAsync(Guid importId, string sourceType, CancellationToken ct)
    {
        var overallStopwatch = System.Diagnostics.Stopwatch.StartNew();

        metrics.RecordRequest(MetricNames.Import, MetricNames.Operations.ImportRequests);
        metrics.RecordCount(MetricNames.Import, 1,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportRequests),
            (MetricNames.CommonTags.SourceType, sourceType),
            ("import_id", importId.ToString()));

        var report = await reportingService.StartImportAsync(importId, sourceType, ct);

        try
        {
            await acquisitionPipeline.StartAsync(report, ct);
            await ingestionPipeline.StartAsync(report, ct);

            report.Status = ImportStatus.Completed;
            report.CompletedAtUtc = DateTime.UtcNow;
            await reportingService.UpsertImportReportAsync(report, ct);

            overallStopwatch.Stop();

            metrics.RecordRequest(MetricNames.Import, MetricNames.Operations.ImportCompletions);
            metrics.RecordValue(MetricNames.Import, overallStopwatch.ElapsedMilliseconds,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportDuration),
                (MetricNames.CommonTags.SourceType, sourceType));
            metrics.RecordCount(MetricNames.Import, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportCompletions),
                (MetricNames.CommonTags.SourceType, sourceType),
                (MetricNames.CommonTags.Status, "success"));

            var totalRecords = (report.IngestionPhase?.RecordsCreated ?? 0) +
                              (report.IngestionPhase?.RecordsUpdated ?? 0) +
                              (report.IngestionPhase?.RecordsDeleted ?? 0);
            if (totalRecords > 0 && overallStopwatch.ElapsedMilliseconds > 0)
            {
                var recordsPerMinute = (totalRecords * 60000.0) / overallStopwatch.ElapsedMilliseconds;
                metrics.RecordValue(MetricNames.Import, recordsPerMinute,
                    (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportRecordsPerMinute),
                    (MetricNames.CommonTags.SourceType, sourceType));
            }

            metrics.RecordCount(MetricNames.Import, totalRecords,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportTotalRecords),
                (MetricNames.CommonTags.SourceType, sourceType));
            metrics.RecordCount(MetricNames.Import, report.IngestionPhase?.FilesProcessed ?? 0,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportTotalFiles),
                (MetricNames.CommonTags.SourceType, sourceType));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Import failed for ImportId: {ImportId}", importId);

            report.Status = ImportStatus.Failed;
            report.CompletedAtUtc = DateTime.UtcNow;
            report.Error = ex.Message;
            await reportingService.UpsertImportReportAsync(report, ct);

            overallStopwatch.Stop();

            metrics.RecordRequest(MetricNames.Import, MetricNames.Operations.ImportErrors);
            metrics.RecordValue(MetricNames.Import, overallStopwatch.ElapsedMilliseconds,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportDuration),
                (MetricNames.CommonTags.SourceType, sourceType));
            metrics.RecordCount(MetricNames.Import, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportCompletions),
                (MetricNames.CommonTags.SourceType, sourceType),
                (MetricNames.CommonTags.Status, "failed"));
            metrics.RecordCount(MetricNames.Import, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ImportErrors),
                (MetricNames.CommonTags.SourceType, sourceType),
                (MetricNames.CommonTags.ErrorType, ex.GetType().Name));

            throw;
        }
    }
}