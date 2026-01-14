using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Telemetry;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.ETL.Impl;

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

        metrics.RecordRequest("import_orchestrator", "started");
        metrics.RecordCount("import_requests", 1,
            ("source_type", sourceType),
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

            metrics.RecordRequest("import_orchestrator", "completed");
            metrics.RecordDuration("import_orchestrator", overallStopwatch.ElapsedMilliseconds);
            metrics.RecordCount("import_completions", 1,
                ("source_type", sourceType),
                ("status", "success"));

            var totalRecords = (report.IngestionPhase?.RecordsCreated ?? 0) +
                              (report.IngestionPhase?.RecordsUpdated ?? 0) +
                              (report.IngestionPhase?.RecordsDeleted ?? 0);
            if (totalRecords > 0 && overallStopwatch.ElapsedMilliseconds > 0)
            {
                var recordsPerMinute = (totalRecords * 60000.0) / overallStopwatch.ElapsedMilliseconds;
                metrics.RecordValue("import_records_per_minute", recordsPerMinute,
                    ("source_type", sourceType));
            }

            metrics.RecordCount("import_total_records", totalRecords,
                ("source_type", sourceType));
            metrics.RecordCount("import_total_files", report.IngestionPhase?.FilesProcessed ?? 0,
                ("source_type", sourceType));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Import failed for ImportId: {ImportId}", importId);

            report.Status = ImportStatus.Failed;
            report.CompletedAtUtc = DateTime.UtcNow;
            report.Error = ex.Message;
            await reportingService.UpsertImportReportAsync(report, ct);

            overallStopwatch.Stop();

            metrics.RecordRequest("import_orchestrator", "failed");
            metrics.RecordDuration("import_orchestrator", overallStopwatch.ElapsedMilliseconds);
            metrics.RecordCount("import_completions", 1,
                ("source_type", sourceType),
                ("status", "failed"));
            metrics.RecordCount("import_errors", 1,
                ("source_type", sourceType),
                ("error_type", ex.GetType().Name));

            throw;
        }
    }
}