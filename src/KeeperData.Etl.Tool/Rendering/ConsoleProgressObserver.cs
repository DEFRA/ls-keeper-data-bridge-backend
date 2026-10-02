using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;
using System.Globalization;

namespace KeeperData.Etl.Tool.Rendering;

/// <summary>Reports progress to stdout as the run goes, so a long pipeline shows where it is rather
/// than only what it produced. Advisory, like every observer: nothing here can fail the run.</summary>
internal sealed class ConsoleProgressObserver : IPipelineRunObserver
{
    private IReadOnlyList<string> _stages = [];
    private int _completed;

    public Task RunStartingAsync(IPipelineContext context, IReadOnlyList<string> stageNames, CancellationToken cancellationToken)
    {
        _stages = stageNames;
        _completed = 0;

        Console.WriteLine($"Running {stageNames.Count} stage(s): {string.Join(" -> ", stageNames)}");
        Console.WriteLine();

        return Task.CompletedTask;
    }

    public Task StageStartingAsync(IPipelineContext context, string stageName, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[{_completed + 1}/{_stages.Count}] {stageName} ...");

        return Task.CompletedTask;
    }

    public Task StageCompletedAsync(
        IPipelineContext context,
        string stageName,
        IReadOnlyList<object> items,
        TimeSpan elapsed,
        CancellationToken cancellationToken)
    {
        _completed++;

        Console.WriteLine(
            $"[{_completed}/{_stages.Count}] {stageName} done - {items.Count} item(s) in {elapsed.TotalSeconds:N1}s{Detail(items)}");

        WarnOnMissingBaseline(items);

        Console.WriteLine();

        return Task.CompletedTask;
    }

    /// <summary>A dataset whose baseline falls outside the lookback still discovers its deltas, and
    /// the merge then applies them to an empty state and reports a snapshot as though nothing were
    /// wrong. The result is a plausible but partial read model, so it is worth saying out loud.</summary>
    private static void WarnOnMissingBaseline(IReadOnlyList<object> items)
    {
        foreach (var set in items.OfType<DiscoveredFileSet>())
        {
            if (set.Definition.BaselineKeyPattern is null || set.Files.Count == 0) continue;

            if (set.Files.Any(file => DataSetFileNaming.MatchesBaseline(set.Definition, file.StorageObject.Key))) continue;

            Console.WriteLine(
                $"  WARNING  {set.Definition.Name}: {set.Files.Count} delta(s) but no baseline in the lookback window. " +
                "The snapshot will hold only what the deltas carry. Widen --lookback-days.");
        }
    }

    public Task RunCompletedAsync(IPipelineContext context, TimeSpan elapsed, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Pipeline completed in {elapsed.TotalSeconds:N1}s.");

        return Task.CompletedTask;
    }

    public Task RunFailedAsync(IPipelineContext context, Exception exception, CancellationToken cancellationToken)
    {
        Console.Error.WriteLine($"Pipeline failed during '{_stages.ElementAtOrDefault(_completed) ?? "unknown"}'.");

        return Task.CompletedTask;
    }

    /// <summary>What the stage produced, where the payload says something a count does not: the
    /// datasets touched, or the row counts the later stages carry.</summary>
    private static string Detail(IReadOnlyList<object> items)
    {
        if (items.OfType<SqliteExportFile>().FirstOrDefault() is { } export)
        {
            var rows = export.Tables.Sum(table => table.RowCount);

            return $" - {export.Tables.Count} table(s), {rows.ToString("N0", CultureInfo.InvariantCulture)} row(s)";
        }

        if (items.OfType<StagingDatabase>().FirstOrDefault() is { } staging)
        {
            return $" - {staging.Tables.Count} table(s)";
        }

        var datasets = items
            .Select(DataSetName)
            .OfType<string>()
            .Distinct(StringComparer.Ordinal)
            .ToList();

        return datasets.Count == 0 ? string.Empty : $" - {string.Join(", ", datasets)}";
    }

    private static string? DataSetName(object item) => item switch
    {
        DiscoveredFile payload => payload.Definition.Name,
        DiscoveredFileSet payload => payload.Definition.Name,
        RawFileSet payload => payload.Definition.Name,
        NormalisedFileSet payload => payload.Definition.Name,
        OptimisedFileSet payload => payload.Definition.Name,
        SnapshotFile payload => payload.Definition.Name,
        _ => null
    };
}
