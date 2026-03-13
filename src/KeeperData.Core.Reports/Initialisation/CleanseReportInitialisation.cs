using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Cleanse.Export.Index;
using KeeperData.Core.Reports.Cleanse.Operations.Index;
using KeeperData.Core.Reports.Issues.Index;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Reports.Initialisation;

/// <summary>
/// Default implementation that runs all one-time initialisation tasks
/// for the cleanse reports module.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Initialisation orchestrator - covered by integration tests.")]
public class CleanseReportInitialisation(
    IIssueIndexManager issueIndexManager,
    IIssueHistoryIndexManager issueHistoryIndexManager,
    ICleanseOperationsIndexManager cleanseOperationsIndexManager,
    ICleanseExportOperationsIndexManager cleanseExportOperationsIndexManager,
    ILogger<CleanseReportInitialisation> logger) : ICleanseReportInitialisation
{
    public async Task InitialiseAsync(CancellationToken ct = default)
    {
        logger.LogInformation("Initialising cleanse reports module...");

        await issueIndexManager.EnsureIndexesAsync(ct);
        await issueHistoryIndexManager.EnsureIndexesAsync(ct);
        await cleanseOperationsIndexManager.EnsureIndexesAsync(ct);
        await cleanseExportOperationsIndexManager.EnsureIndexesAsync(ct);

        logger.LogInformation("Cleanse reports module initialised successfully");
    }
}
