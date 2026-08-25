using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.EtlPipeline.Views;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>The SQLite read model in views/, built from the staging database. Final output of the
/// pipeline.</summary>
[ExcludeFromCodeCoverage(Justification = "Pipeline payload record - no logic to test.")]
public sealed record SqliteExportFile
{
    public Guid RunId { get; init; }

    public string Key { get; init; } = string.Empty;

    /// <summary>Carried through from the staging database, so the SQLite file and the DuckDB file it
    /// was built from share one timestamp. Not the time the ETL ran.</summary>
    public DateTimeOffset SourceTimestamp { get; init; }

    public IReadOnlyList<SqliteViewTable> Tables { get; init; } = [];

    /// <summary>False when an up-to-date export for this staging database already existed.</summary>
    public bool Created { get; init; }
}
