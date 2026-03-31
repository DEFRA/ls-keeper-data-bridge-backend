using System.Diagnostics;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Operations;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using KeeperData.Core.Throttling;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;

public abstract class CleanseAnalysisEngineBase(IPreloadedCtsSamDataService dataService, IIssueCommandService issueCommandService,
    IThrottler throttler, ILogger logger)
{
    private const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss";

    protected IIssueCommandService IssueCommandService { get; } = issueCommandService;
    protected IThrottler Throttler { get; } = throttler;
    protected ILogger Logger { get; } = logger;

    protected delegate Task RecordProcessor(string id,
        string operationId, AnalysisMetrics metrics, CancellationToken ct, OperationScope? scope = null);

    protected delegate Task<QueryResult> Fetch(int skip, int batchSize, CancellationToken ct);

    protected record PumpContext(
        int TotalRecords,
        string OperationId,
        AnalysisMetrics Metrics,
        Fetch Fetcher,
        RecordProcessor RecordProcessor,
        string IdFieldKey,
        string PumpName = "Pump",
        OperationScope? Scope = null,
        Func<bool>? IsCancellationRequested = null);

    protected abstract Task ProcessCtsPrimaryRecordAsync(string id,
        string operationId, AnalysisMetrics metrics, CancellationToken ct, OperationScope? scope = null);

    protected abstract Task ProcessSamPrimaryRecordAsync(string id,
        string operationId, AnalysisMetrics metrics, CancellationToken ct, OperationScope? scope = null);


    protected async Task PumpAsync(PumpContext context, CancellationToken ct)
    {
        Trace.TraceInformation($"KRDSBRIDGE | PumpAsync | BEGIN, pump={context.PumpName}, totalRecords={context.TotalRecords}, operationId={context.OperationId}");
        var pumpStopwatch = Stopwatch.StartNew();
        var skip = 0;
        var baseRecordsAnalyzed = context.Metrics.RecordsAnalyzed;
        var scope = context.Scope;

        while (!ct.IsCancellationRequested)
        {
            var settings = Throttler.Settings.CleanseAnalysis;

            var (batch, fetchMs) = await Timed.RunAsync(() => context.Fetcher(skip, settings.PumpBatchSize, ct));
            scope?.TrackElapsed("fetching", fetchMs);
            Trace.TraceInformation($"KRDSBRIDGE | PumpAsync | {context.PumpName} | Fetched batch, skip={skip}, batchSize={batch.Data.Count}, fetchDuration={fetchMs}ms");

            if (batch.Data.Count == 0)
            {
                break;
            }

            var processMs = await Timed.RunAsync(() => ProcessBatchAsync(batch, context, ct));
            scope?.TrackElapsed("record_processing", processMs);
            Trace.TraceInformation($"KRDSBRIDGE | PumpAsync | {context.PumpName} | Batch processed, count={batch.Data.Count}, processingDuration={processMs}ms");

            skip += batch.Data.Count;
            context.Metrics.RecordsAnalyzed = baseRecordsAnalyzed + skip;
            scope?.UpdateProgress(skip);

            // Check for external cancellation requests (e.g. user-initiated)
            if (context.IsCancellationRequested?.Invoke() == true)
            {
                throw new OperationCanceledException("Cancellation requested by user.");
            }
        }

        pumpStopwatch.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | PumpAsync | END, pump={context.PumpName}, recordsProcessed={skip}, totalAnalyzed={context.Metrics.RecordsAnalyzed}, issues={context.Metrics.IssuesFound}, duration={pumpStopwatch.ElapsedMilliseconds}ms");
    }

    public async Task<AnalysisMetrics> ExecuteAsync(string operationId, CancellationToken ct,
        OperationScope? scope = null, Func<bool>? isCancellationRequested = null)
    {
        Trace.TraceInformation($"KRDSBRIDGE | ExecuteAsync | BEGIN, operationId={operationId}");
        var executeStopwatch = Stopwatch.StartNew();
        var metrics = new AnalysisMetrics();

        // Pre-load all CTS and SAM data into memory before pumping
        Trace.TraceInformation("KRDSBRIDGE | ExecuteAsync | Starting data preload");
        var preloadScope = scope?.CreateChild(OperationPhases.Preload);
        await preloadScope.RunAsync(
            () => dataService.PreloadAsync(ct, preloadScope, isCancellationRequested),
            "Preload cancelled", "Preload failed");
        Trace.TraceInformation($"KRDSBRIDGE | ExecuteAsync | Preload complete, elapsed={executeStopwatch.ElapsedMilliseconds}ms");

        var ctsTotalRecords = dataService.GetCtsCphHoldingsCount();
        var samTotalRecords = dataService.GetSamCphHoldingsCount();
        var totalRecords = ctsTotalRecords + samTotalRecords;
        Trace.TraceInformation($"KRDSBRIDGE | ExecuteAsync | Counts retrieved, ctsRecords={ctsTotalRecords}, samRecords={samTotalRecords}, total={totalRecords}");

        // Set the authoritative total on the Analysis scope now that preload counts are known.
        // Includes preload records + pump records so progress is tracked across all phases.
        var preloadRecordCount = dataService.GetTotalPreloadedRecordCount();
        scope?.UpdateTotal(preloadRecordCount + totalRecords, "Analyzing records...");

        // iterate CTS CPH records
        Trace.TraceInformation("KRDSBRIDGE | ExecuteAsync | Starting CTS Pump");
        var ctsPumpScope = scope?.CreateChild("CTS Pump");
        ctsPumpScope?.Start(ctsTotalRecords, "Processing CTS records");
        await ctsPumpScope.RunAsync(
            () => PumpAsync(new PumpContext(totalRecords, operationId, metrics,
                (skip, batchSize, token) => Task.FromResult(dataService.ListCtsCphHoldings(skip, batchSize)),
                ProcessCtsPrimaryRecordAsync,
                DataFields.CtsCphHoldingFields.LidFullIdentifier, "CTS Pump", ctsPumpScope, isCancellationRequested), ct),
            "CTS Pump cancelled", "CTS Pump failed");
        Trace.TraceInformation($"KRDSBRIDGE | ExecuteAsync | CTS Pump complete, elapsed={executeStopwatch.ElapsedMilliseconds}ms");

        // iterate SAM CPH records
        Trace.TraceInformation("KRDSBRIDGE | ExecuteAsync | Starting SAM Pump");
        var samPumpScope = scope?.CreateChild("SAM Pump");
        samPumpScope?.Start(samTotalRecords, "Processing SAM records");
        await samPumpScope.RunAsync(
            () => PumpAsync(new PumpContext(totalRecords, operationId, metrics,
                (skip, batchSize, token) => Task.FromResult(dataService.ListSamCphHoldings(skip, batchSize)),
                ProcessSamPrimaryRecordAsync,
                DataFields.SamCphHoldingFields.Cph, "SAM Pump", samPumpScope, isCancellationRequested), ct),
            "SAM Pump cancelled", "SAM Pump failed");

        executeStopwatch.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | ExecuteAsync | END, operationId={operationId}, records={metrics.RecordsAnalyzed}, issues={metrics.IssuesFound}, duration={executeStopwatch.ElapsedMilliseconds}ms");
        return metrics;
    }

    protected static async Task ProcessBatchAsync(QueryResult batch, PumpContext context, CancellationToken ct)
    {
        foreach (var record in batch.Data)
        {
            var id = record[context.IdFieldKey]?.ToString();
            if (id != null)
            {
                await context.RecordProcessor(id, context.OperationId, context.Metrics, ct, context.Scope);
            }
        }
    }

    protected static LidFullIdentifier? ParseLidFullIdentifier(IDictionary<string, object?> record)
        => LidFullIdentifier.TryParse(record[DataFields.CtsCphHoldingFields.LidFullIdentifier]?.ToString());

    /// <summary>
    /// Determines whether the specified holding record is currently active based on its effective end date.
    /// </summary>
    /// <remarks>This method evaluates the effective end date of the holding record against the current UTC
    /// time to determine if the record is considered active.</remarks>
    /// <param name="record">A dictionary containing the holding record data. Must include an entry for the effective end date under the key
    /// specified by DataFields.CtsCphHoldingFields.LocEffectiveTo.</param>
    /// <returns>true if the record is active, meaning the effective end date is either not set or is in the future; otherwise,
    /// false.</returns>
    protected static bool IsCtsCphHoldingRecordActive(IDictionary<string, object?> record)
        => record[DataFields.CtsCphHoldingFields.LocEffectiveTo]?.ToString().ToDateTime(DateTimeFormat) switch
        {
            null => true,
            var effectiveTo => effectiveTo > DateTime.UtcNow
        };

}