namespace KeeperData.Core.EtlPipeline.Status;

/// <summary>The parts of import status that do not depend on where the document is kept: how a
/// stage's progress folds into it, and when a run counts as abandoned.
///
/// Shared because there is more than one store. The merge rules decide what a caller sees - which
/// keys, which row counts, which dataset entry a stage belongs to - and two stores disagreeing
/// about them would be a difference nothing points at.</summary>
public static class EtlImportProgress
{
    /// <summary>How long a run is trusted to still be alive after its last sign of progress. Longer
    /// than any single stage is expected to take, since the lease is only extended between stages.</summary>
    public static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(30);

    public static EtlImportDocument Queued(Guid importId, string sourceType, string? dataset, DateTime nowUtc)
        => new()
        {
            ImportId = importId,
            Status = EtlImportStatus.Queued.ToString(),
            SourceType = sourceType,
            Dataset = dataset,
            RequestedAtUtc = nowUtc,
            LeaseExpiresAtUtc = nowUtc.Add(LeaseDuration)
        };

    /// <summary>A purge is recorded in the same history as the runs it reset, so a storage wipe is
    /// visible beside them. It is not a run, so it carries no stages, datasets or output keys.</summary>
    public static EtlImportDocument Purged(EtlPurgeRecord purge, DateTime nowUtc)
        => new()
        {
            ImportId = purge.PurgeId,
            Status = EtlImportStatus.Purged.ToString(),
            SourceType = purge.SourceType,
            Dataset = purge.Dataset,
            RequestedAtUtc = nowUtc,
            StartedAtUtc = nowUtc,
            CompletedAtUtc = nowUtc,
            Purge = new EtlImportPurgeDocument
            {
                Stages = [.. purge.Stages],
                DeletedCount = purge.DeletedCount
            }
        };

    /// <summary>Folds one completed stage into the document and extends the lease.</summary>
    public static void ApplyStage(EtlImportDocument document, EtlImportStageProgress progress, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(document);
        ArgumentNullException.ThrowIfNull(progress);

        document.Stages.Add(new EtlImportStageDocument
        {
            Name = progress.StageName,
            ItemCount = progress.ItemCount,
            ElapsedMs = (long)progress.Elapsed.TotalMilliseconds,
            CompletedAtUtc = nowUtc
        });

        foreach (var dataset in progress.Datasets)
        {
            Merge(DatasetEntry(document, dataset.Dataset), dataset);
        }

        if (progress.DuckDbKey is not null)
        {
            document.DuckDbKey = progress.DuckDbKey;
        }

        if (progress.SqliteKey is not null)
        {
            document.SqliteKey = progress.SqliteKey;
        }

        if (progress.SqliteTables is { Count: > 0 })
        {
            document.SqliteTables =
                [.. progress.SqliteTables.Select(table => new EtlImportViewTableDocument
                {
                    Name = table.Name,
                    RowCount = table.RowCount
                })];
        }

        document.LeaseExpiresAtUtc = nowUtc.Add(LeaseDuration);
    }

    public static void Complete(
        EtlImportDocument document,
        EtlImportStatus status,
        string? error,
        EtlImportErrorDetail? detail,
        DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(document);

        document.Status = status.ToString();
        document.CompletedAtUtc = nowUtc;
        document.CurrentStage = null;
        document.LeaseExpiresAtUtc = null;
        document.Error = error;
        document.ErrorDetail = detail;
    }

    /// <summary>A run whose lease lapsed is not running - the process hosting it died. Reported as
    /// failed so a poller gets an answer instead of waiting forever on "Running".</summary>
    public static EtlImportDocument AsAbandonedIfLapsed(EtlImportDocument document, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(document);

        var running = document.Status is nameof(EtlImportStatus.Running) or nameof(EtlImportStatus.Queued);

        if (!running || document.LeaseExpiresAtUtc is null || document.LeaseExpiresAtUtc > nowUtc)
        {
            return document;
        }

        document.Status = EtlImportStatus.Failed.ToString();
        document.Error = "The run stopped reporting progress and is assumed to have been abandoned.";

        return document;
    }

    public static bool IsInFlight(EtlImportDocument document, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(document);

        var active = document.Status is nameof(EtlImportStatus.Running) or nameof(EtlImportStatus.Queued);

        return active && (document.LeaseExpiresAtUtc is null || document.LeaseExpiresAtUtc > nowUtc);
    }

    private static EtlImportDatasetDocument DatasetEntry(EtlImportDocument document, string dataset)
    {
        var existing = document.Datasets.Find(d => d.Dataset == dataset);

        if (existing is not null) return existing;

        var created = new EtlImportDatasetDocument { Dataset = dataset };
        document.Datasets.Add(created);

        return created;
    }

    private static void Merge(EtlImportDatasetDocument target, EtlImportDatasetProgress source)
    {
        if (source.SourceFiles.Count > 0)
        {
            target.SourceFiles = [.. source.SourceFiles.Select(f => new EtlImportSourceFileDocument { Key = f.Key, Size = f.Size })];
        }

        if (source.RawKeys.Count > 0) target.RawKeys = [.. source.RawKeys];
        if (source.NormalisedKeys.Count > 0) target.NormalisedKeys = [.. source.NormalisedKeys];
        if (source.OptimisedKeys.Count > 0) target.OptimisedKeys = [.. source.OptimisedKeys];

        target.SnapshotKey = source.SnapshotKey ?? target.SnapshotKey;
        target.SnapshotSourceTimestampUtc = source.SnapshotSourceTimestamp?.UtcDateTime ?? target.SnapshotSourceTimestampUtc;
        target.RowCount = source.RowCount ?? target.RowCount;
        target.RowsUpserted = source.RowsUpserted ?? target.RowsUpserted;
        target.RowsDeleted = source.RowsDeleted ?? target.RowsDeleted;
        target.RowsIgnoredDeletes = source.RowsIgnoredDeletes ?? target.RowsIgnoredDeletes;
        target.RowsRejected = source.RowsRejected ?? target.RowsRejected;

        if (source.ColumnsNullified.Count > 0) target.ColumnsNullified = [.. source.ColumnsNullified];
        if (source.ColumnsAdded.Count > 0) target.ColumnsAdded = [.. source.ColumnsAdded];
    }
}
