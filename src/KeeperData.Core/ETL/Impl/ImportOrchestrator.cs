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
        // Start import reporting
        await reportingService.StartImportAsync(importId, sourceType, ct);
        
        try
        {
            await acquisitionPipeline.StartAsync(importId, sourceType, ct);
            await ingestionPipeline.StartAsync(importId, ct);
            
            // Complete import successfully
            await reportingService.CompleteImportAsync(importId, ImportStatus.Completed, null, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Import failed for ImportId: {ImportId}", importId);
            
            // Mark import as failed
            await reportingService.CompleteImportAsync(importId, ImportStatus.Failed, ex.Message, ct);
            throw;
        }
    }
}
