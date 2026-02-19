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

    protected record PumpContext(
        int TotalRecords,
        string OperationId,
        AnalysisMetrics Metrics,
        ProgressCallback ProgressCallback,
        Fetch Fetcher,
        RecordProcessor RecordProcessor,
        string IdFieldKey);

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
        await PumpAsync(new PumpContext(totalRecords, operationId, metrics, progressCallback,
            dataService.ListCtsCphHoldingsAsync, ProcessCtsPrimaryRecordAsync,
            DataFields.CtsCphHoldingFields.LidFullIdentifier), ct);

        // iterate SAM CPH records
        await PumpAsync(new PumpContext(totalRecords, operationId, metrics, progressCallback,
            dataService.ListSamCphHoldingsAsync, ProcessSamPrimaryRecordAsync,
            DataFields.SamCphHoldingFields.Cph), ct);

        return metrics;
    }



    protected async Task PumpAsync(PumpContext context, CancellationToken ct)
    {
        var skip = 0;
        while (true && !ct.IsCancellationRequested)
        {
            var batch = await context.Fetcher(skip, BatchSize, ct);

            if (batch.Data.Count == 0)
            {
                break;
            }

            await ProcessBatchAsync(batch, context, ct);

            skip += batch.Data.Count;
            context.Metrics.RecordsAnalyzed = skip;

            if (ShouldUpdateProgress(context.Metrics.RecordsAnalyzed))
            {
                await context.ProgressCallback(context.Metrics.RecordsAnalyzed, context.TotalRecords, context.Metrics.IssuesFound, context.Metrics.IssuesResolved);
            }
        }
    }

    protected static async Task ProcessBatchAsync(QueryResult batch, PumpContext context, CancellationToken ct)
    {
        foreach (var record in batch.Data)
        {
            var id = record[context.IdFieldKey]?.ToString();
            if (id != null)
            {
                await context.RecordProcessor(id, context.OperationId, context.Metrics, ct);
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