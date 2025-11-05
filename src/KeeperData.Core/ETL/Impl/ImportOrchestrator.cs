using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Reporting;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.ETL.Impl;

public class ImportOrchestrator(
    IAcquisitionPipeline acquisitionPipeline,
    IIngestionPipeline ingestionPipeline,
    IImportReportingService reportingService,
    ILogger<ImportOrchestrator> logger) : IImportOrchestrator
{
    public async Task StartAsync(Guid importId, string sourceType, CancellationToken ct)
    {
        var report = await reportingService.StartImportAsync(importId, sourceType, ct);

        try
        {
            await acquisitionPipeline.StartAsync(report, ct);
            await ingestionPipeline.StartAsync(report, ct);

            report.Status = ImportStatus.Completed;
            report.CompletedAtUtc = DateTime.UtcNow;
            await reportingService.UpsertImportReportAsync(report, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Import failed for ImportId: {ImportId}", importId);

            report.Status = ImportStatus.Failed;
            report.CompletedAtUtc = DateTime.UtcNow;
            report.Error = ex.Message;
            await reportingService.UpsertImportReportAsync(report, ct);

            throw;
        }
    }
}