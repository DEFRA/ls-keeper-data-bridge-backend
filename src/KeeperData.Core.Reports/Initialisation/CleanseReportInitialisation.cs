using System.Diagnostics.CodeAnalysis;
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
    ILogger<CleanseReportInitialisation> logger) : ICleanseReportInitialisation
{
    public async Task InitialiseAsync(CancellationToken ct = default)
    {
        logger.LogInformation("Initialising cleanse reports module...");

        await issueIndexManager.EnsureIndexesAsync(ct);

        logger.LogInformation("Cleanse reports module initialised successfully");
    }
}
