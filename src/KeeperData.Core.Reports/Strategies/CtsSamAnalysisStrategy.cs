using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Strategies.Rules;

namespace KeeperData.Core.Reports.Strategies;

/// <summary>
/// Analysis strategy that compares CTS CPH Holdings against SAM data.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Analysis strategy with complex database query dependencies - covered by integration tests.")]
public sealed class CtsSamAnalysisStrategy : ICleanseAnalysisStrategy
{
    private const int BatchSize = 100;
    private const int ProgressUpdateInterval = 100;
    private const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss";

    private readonly RecordIdGenerator _recordIdGenerator = new();

    /// <inheritdoc />
    public string Name => "CTS-SAM Analysis";

    /// <inheritdoc />
    public async Task<StrategyMetrics> ExecuteAsync(
        IAnalysisContext context,
        IIssueRecorder issueRecorder,
        ProgressCallback progressCallback,
        CancellationToken ct)
    {
        var pipeline = BuildPipeline();
        var metrics = new StrategyMetrics();

        var totalRecords = await CountTotalRecordsAsync(context, ct);
        await progressCallback(0, totalRecords, 0, 0);

        var skip = 0;

        while (true)
        {
            var batch = await FetchRecordBatchAsync(context, skip, ct);

            if (batch.Data.Count == 0)
            {
                break;
            }

            await ProcessBatchAsync(batch, pipeline, context, issueRecorder, metrics, ct);

            skip += batch.Data.Count;
            metrics.RecordsAnalyzed = skip;

            if (ShouldUpdateProgress(metrics.RecordsAnalyzed))
            {
                await progressCallback(metrics.RecordsAnalyzed, totalRecords, metrics.IssuesFound, metrics.IssuesResolved);
            }
        }

        return metrics;
    }

    private static IRulePipeline<CtsSamRuleInput> BuildPipeline()
    {
        return RulePipelineBuilder<CtsSamRuleInput>.Create()
            .AddRule(new SamCphRecordDoesNotExistRule())
                .StopOnIssue()
            .AddRule(new SamCphRecordNoEmailAddressesRule())
                .ContinueAlways()
            .Build();
    }

    private static async Task<int> CountTotalRecordsAsync(IAnalysisContext context, CancellationToken ct)
    {
        var result = await context.QueryAsync(QueryParametersFactory.GetEnglishCtsCphHoldingsCount(context.DataSets), ct);
        return (int)(result.TotalCount ?? 0);
    }

    private static async Task<QueryResult> FetchRecordBatchAsync(IAnalysisContext context, int skip, CancellationToken ct)
    {
        return await context.QueryAsync(QueryParametersFactory.GetEnglishCtsCphHoldings(context.DataSets, skip, BatchSize), ct);
    }

    private async Task ProcessBatchAsync(
        QueryResult batch,
        IRulePipeline<CtsSamRuleInput> pipeline,
        IAnalysisContext context,
        IIssueRecorder issueRecorder,
        StrategyMetrics metrics,
        CancellationToken ct)
    {
        foreach (var record in batch.Data)
        {
            await ProcessRecordAsync(record, pipeline, context, issueRecorder, metrics, ct);
        }
    }

    private async Task ProcessRecordAsync(IDictionary<string, object?> record, IRulePipeline<CtsSamRuleInput> pipeline, 
        IAnalysisContext context, IIssueRecorder issueRecorder, StrategyMetrics metrics, CancellationToken ct)
    {
        var lidFullIdentifier = ParseLidFullIdentifier(record);

        if (lidFullIdentifier is null || !IsValidCountyCode(lidFullIdentifier) || !IsCtsCphHoldingRecordActive(record))
        {
            return;
        }

        var thumbprint = GenerateThumbprint(lidFullIdentifier);

        var input = new CtsSamRuleInput
        {
            CtsRecord = record,
            LidFullIdentifier = lidFullIdentifier,
            Thumbprint = thumbprint
        };

        var results = await pipeline.ExecuteAsync(input, context, ct);

        await ProcessPipelineResultsAsync(results, input, issueRecorder, metrics, ct);
    }

    private static async Task ProcessPipelineResultsAsync(IReadOnlyList<PipelineRuleResult> results, CtsSamRuleInput input, 
        IIssueRecorder issueRecorder, StrategyMetrics metrics, CancellationToken ct)
    {
        var hasAnyIssue = false;

        foreach (var pipelineResult in results)
        {
            if (pipelineResult.Result.HasIssue && pipelineResult.Result.IssueCode is not null)
            {
                hasAnyIssue = true;
                var recordResult = await issueRecorder.RecordIssueAsync(
                    input.Thumbprint,
                    pipelineResult.Result.IssueCode,
                    input.LidFullIdentifier,
                    ct);

                if (recordResult is IssueRecordResult.Created or IssueRecordResult.Reactivated)
                {
                    metrics.IssuesFound++;
                }
            }
        }

        // If no issues were found by any rule, resolve any existing issue
        if (!hasAnyIssue)
        {
            var resolveResult = await issueRecorder.ResolveIssueIfExistsAsync(input.Thumbprint, ct);
            if (resolveResult == IssueRecordResult.Resolved)
            {
                metrics.IssuesResolved++;
            }
        }
    }

    private static LidFullIdentifier? ParseLidFullIdentifier(IDictionary<string, object?> record)
        => LidFullIdentifier.TryParse(record[DataFields.CtsCphHoldingFields.LidFullIdentifier]?.ToString());

    private static bool IsCtsCphHoldingRecordActive(IDictionary<string, object?> record)
        => record[DataFields.CtsCphHoldingFields.LocEffectiveTo]?.ToString().ToDateTime(DateTimeFormat) switch
        {
            null => true,
            var effectiveTo => effectiveTo > DateTime.UtcNow
        };

    private static bool IsValidCountyCode(LidFullIdentifier lidFullIdentifier)
        => lidFullIdentifier.Cph.CountyCode.ToInteger() is >= 1 and <= 51;

    private string GenerateThumbprint(LidFullIdentifier lidFullIdentifier)
        => _recordIdGenerator.GenerateId(lidFullIdentifier.Value);

    private static bool ShouldUpdateProgress(int recordsAnalyzed)
        => recordsAnalyzed % ProgressUpdateInterval == 0;
}
