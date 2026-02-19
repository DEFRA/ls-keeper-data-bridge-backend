using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;

public abstract class CleanseAnalysisEngineBase(ICtsSamQueryService dataService, IIssueCommandService issueCommandService)
{
    private const int BatchSize = 100;
    private const int ProgressUpdateInterval = 100;
    private const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss";

    protected IIssueCommandService IssueCommandService { get; } = issueCommandService;

    protected delegate Task RecordProcessor(string id,
        string operationId, AnalysisMetrics metrics, CancellationToken ct);

    protected delegate Task<QueryResult> Fetch(int skip, int batchSize, CancellationToken ct);

    protected abstract Task ProcessCtsPrimaryRecordAsync(string id,
        string operationId, AnalysisMetrics metrics, CancellationToken ct);

    protected abstract Task ProcessSamPrimaryRecordAsync(string id,
        string operationId, AnalysisMetrics metrics, CancellationToken ct);

    public async Task<AnalysisMetrics> ExecuteAsync(string operationId, ProgressCallback progressCallback, CancellationToken ct)
    {
        var metrics = new AnalysisMetrics();

        var ctsTotalRecords = await dataService.GetCtsCphHoldingsCountAsync(ct);
        var samTotalRecords = await dataService.GetSamCphHoldingsCountAsync(ct);
        var totalRecords = ctsTotalRecords + samTotalRecords;

        await progressCallback(0, totalRecords, 0, 0);

        // iterate CTS CPH records
        await PumpAsync(totalRecords, operationId, metrics, progressCallback, dataService.ListCtsCphHoldingsAsync, 
            ProcessCtsPrimaryRecordAsync, DataFields.CtsCphHoldingFields.LidFullIdentifier, ct);

        // iterate SAM CPH records
        await PumpAsync(totalRecords, operationId, metrics, progressCallback, dataService.ListSamCphHoldingsAsync, 
            ProcessSamPrimaryRecordAsync, DataFields.SamCphHoldingFields.Cph, ct);

        return metrics;
    }



    protected async Task PumpAsync(int totalRecords, string operationId, AnalysisMetrics metrics,
        ProgressCallback progressCallback, Fetch fetcher, RecordProcessor recordProcessor, string idFieldKey, CancellationToken ct)
    {
        var skip = 0;
        while (true && !ct.IsCancellationRequested)
        {
            var batch = await fetcher(skip, BatchSize, ct);

            if (batch.Data.Count == 0)
            {
                break;
            }

            await ProcessBatchAsync(batch, operationId, metrics, recordProcessor, idFieldKey, ct);

            skip += batch.Data.Count;
            metrics.RecordsAnalyzed = skip;

            if (ShouldUpdateProgress(metrics.RecordsAnalyzed))
            {
                await progressCallback(metrics.RecordsAnalyzed, totalRecords, metrics.IssuesFound, metrics.IssuesResolved);
            }
        }
    }

    protected async Task ProcessBatchAsync(QueryResult batch, string operationId,
        AnalysisMetrics metrics, RecordProcessor processor, string idFieldKey, CancellationToken ct)
    {
        foreach (var record in batch.Data)
        {
            var id = record[idFieldKey]?.ToString();
            if (id != null)
            {
                await processor(id, operationId, metrics, ct);
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


    protected static bool ShouldUpdateProgress(int recordsAnalyzed)
        => recordsAnalyzed % ProgressUpdateInterval == 0;
}