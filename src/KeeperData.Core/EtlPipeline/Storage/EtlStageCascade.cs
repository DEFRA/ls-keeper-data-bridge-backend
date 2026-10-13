using KeeperData.Core.Storage;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>The pipeline's stages in order, and what clearing one of them implies.
///
/// Every artefact is written once and skipped if it already exists, so clearing a stage without
/// clearing what follows leaves the later stages reading outputs built from files that are no
/// longer there. Shared by every caller that clears, so none of them can disagree about it.</summary>
public static class EtlStageCascade
{
    /// <summary>In pipeline order, which is also cascade order.</summary>
    public static readonly string[] Stages =
    [
        EtlPipelineFolders.Raw,
        EtlPipelineFolders.Normalised,
        EtlPipelineFolders.Optimised,
        EtlPipelineFolders.Snapshots,
        EtlPipelineFolders.Staging,
        EtlPipelineFolders.Views
    ];

    public static string Names => string.Join(", ", Stages);

    /// <summary>The named stage and everything downstream of it. Empty when the name is unknown;
    /// every stage when none is given.</summary>
    public static List<string> From(string? stage)
    {
        if (string.IsNullOrWhiteSpace(stage)) return [.. Stages];

        var index = Array.FindIndex(Stages, candidate => string.Equals(candidate, stage, StringComparison.OrdinalIgnoreCase));

        return index < 0 ? [] : [.. Stages[index..]];
    }

    /// <summary>Deletes the stages' contents, stopping at the first one it cannot.
    ///
    /// An artefact held open by something else - a reader on the exported SQLite is the usual
    /// culprit - would otherwise surface as an unhandled IOException mid-clear. Stopping is not the
    /// same as undoing: the stages before the failure are already empty, which is why the result
    /// reports which ones went rather than only that something went wrong.</summary>
    public static async Task<EtlStageClearResult> ClearAsync(
        IEtlPipelineStorageProvider provider,
        IReadOnlyList<string> stages,
        Action<string, int>? onStageCleared = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(stages);

        var deleted = 0;
        var cleared = new List<string>();

        foreach (var folder in stages)
        {
            try
            {
                var result = await provider.ForFolder(folder).DeleteByPrefixAsync(string.Empty, cancellationToken);

                onStageCleared?.Invoke(folder, result.TotalDeleted);
                deleted += result.TotalDeleted;
                cleared.Add(folder);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                return new EtlStageClearResult(deleted, cleared, $"Could not clear '{folder}': {exception.Message}");
            }
        }

        return new EtlStageClearResult(deleted, cleared, null);
    }
}

/// <param name="ClearedStages">The stages actually emptied, which on failure is a prefix of those asked for.</param>
/// <param name="Error">Null when every stage was cleared.</param>
public sealed record EtlStageClearResult(int Deleted, IReadOnlyList<string> ClearedStages, string? Error)
{
    /// <summary>True when the clear failed after already emptying something.</summary>
    public bool IsPartial => Error is not null && ClearedStages.Count > 0;
}
