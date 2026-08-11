using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Status;

/// <summary>Turns what each stage produced into import status.
///
/// Every field the status API reports already exists in the stage payloads; this is the only place
/// that knows how to read them, which keeps the stages unaware that status exists at all.</summary>
public sealed class EtlImportStatusObserver(
    IEtlImportStatusStore store,
    ILogger<EtlImportStatusObserver> logger) : IPipelineRunObserver
{
    public Task RunStartingAsync(IPipelineContext context, IReadOnlyList<string> stageNames, CancellationToken cancellationToken)
        => store.MarkRunningAsync(ImportId(context), stageNames, cancellationToken);

    public Task StageCompletedAsync(
        IPipelineContext context,
        string stageName,
        IReadOnlyList<object> items,
        TimeSpan elapsed,
        CancellationToken cancellationToken)
    {
        var datasets = items
            .Select(Map)
            .OfType<EtlImportDatasetProgress>()
            .GroupBy(d => d.Dataset)
            .Select(Coalesce)
            .ToList();

        var duckDbKey = items.OfType<StagingDatabase>().FirstOrDefault()?.Key;

        var progress = new EtlImportStageProgress(stageName, items.Count, elapsed, datasets, duckDbKey);

        return store.RecordStageAsync(ImportId(context), progress, cancellationToken);
    }

    public Task RunCompletedAsync(IPipelineContext context, TimeSpan elapsed, CancellationToken cancellationToken)
        => store.MarkSucceededAsync(ImportId(context), cancellationToken);

    public Task RunFailedAsync(IPipelineContext context, Exception exception, CancellationToken cancellationToken)
    {
        var importId = ImportId(context);

        // Full detail goes to the log; the document gets the message only, so nothing that might
        // carry a salt, password or presigned URL is stored or served to a caller.
        logger.LogError(exception, "ETL import failed (importId={ImportId})", importId);

        return store.MarkFailedAsync(importId, SafeSummary(exception), cancellationToken);
    }

    /// <summary>The innermost message, which is the one that says what actually went wrong; the
    /// wrapper only says the pipeline failed.
    ///
    /// Unless a stage has already explained the failure, in which case that explanation wins: an
    /// <see cref="IEtlDiagnosableException"/> exists precisely because its inner exception is
    /// technically accurate and useless to read. Its message is reported as written, without a type
    /// name in front of it, because it was written to be read.</summary>
    private static string SafeSummary(Exception exception)
    {
        var cause = exception;
        Exception? diagnosable = null;

        while (true)
        {
            if (cause is IEtlDiagnosableException)
            {
                diagnosable = cause;
            }

            if (cause.InnerException is null)
            {
                break;
            }

            cause = cause.InnerException;
        }

        return diagnosable is not null
            ? diagnosable.Message
            : $"{cause.GetType().Name}: {cause.Message}";
    }

    private static EtlImportDatasetProgress? Map(object item) => item switch
    {
        DiscoveredFileSet discovered => new EtlImportDatasetProgress(discovered.Definition.Name)
        {
            SourceFiles = [.. discovered.Files.Select(f => (f.StorageObject.Key, f.StorageObject.Size))]
        },

        RawFileSet raw => new EtlImportDatasetProgress(raw.Definition.Name)
        {
            RawKeys = raw.Files
        },

        NormalisedFileSet normalised => new EtlImportDatasetProgress(normalised.Definition.Name)
        {
            NormalisedKeys = normalised.Files
        },

        SnapshotFile snapshot => new EtlImportDatasetProgress(snapshot.Definition.Name)
        {
            SnapshotKey = snapshot.Key,
            SnapshotSourceTimestamp = snapshot.SourceTimestamp,
            RowCount = snapshot.RowCount,
            RowsUpserted = snapshot.RowsUpserted,
            RowsIgnoredDeletes = snapshot.RowsIgnoredDeletes
        },

        _ => null
    };

    // A stage can emit more than one payload for a dataset; later values win, as the pipeline runs
    // them in order.
    private static EtlImportDatasetProgress Coalesce(IGrouping<string, EtlImportDatasetProgress> group)
        => group.Aggregate((first, second) => new EtlImportDatasetProgress(group.Key)
        {
            SourceFiles = second.SourceFiles.Count > 0 ? second.SourceFiles : first.SourceFiles,
            RawKeys = second.RawKeys.Count > 0 ? second.RawKeys : first.RawKeys,
            NormalisedKeys = second.NormalisedKeys.Count > 0 ? second.NormalisedKeys : first.NormalisedKeys,
            SnapshotKey = second.SnapshotKey ?? first.SnapshotKey,
            SnapshotSourceTimestamp = second.SnapshotSourceTimestamp ?? first.SnapshotSourceTimestamp,
            RowCount = second.RowCount ?? first.RowCount,
            RowsUpserted = second.RowsUpserted ?? first.RowsUpserted,
            RowsIgnoredDeletes = second.RowsIgnoredDeletes ?? first.RowsIgnoredDeletes
        });

    private static Guid ImportId(IPipelineContext context)
        => context is EtlPipelineContext etl ? etl.RunId : Guid.Empty;
}
