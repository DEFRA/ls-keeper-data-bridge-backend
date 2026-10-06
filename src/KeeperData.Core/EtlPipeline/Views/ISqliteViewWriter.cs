using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.EtlPipeline.Views;

/// <summary>A table in the SQLite read model, and how many rows it holds.</summary>
[ExcludeFromCodeCoverage(Justification = "Transformation result record - no logic to test.")]
public sealed record SqliteViewTable(string Name, long RowCount);

/// <summary>One transformation: read the source database, write the target one.</summary>
/// <param name="SourceDatabasePath">The staging DuckDB database. Opened read-only.</param>
/// <param name="TargetDatabasePath">Where to create the SQLite database. Must not already exist.</param>
/// <param name="Parts">The transformation body, in execution order. The writer owns attaching and
/// detaching, so none may carry its own ATTACH, CHECKPOINT or DETACH. The writer runs only the
/// parts the source database can satisfy, so the result need not cover all of them.</param>
/// <param name="QueryDate">The date the transformation evaluates its as-at rules against. The
/// snapshots' source timestamp rather than today: the export is reused on the strength of that
/// timestamp and the script version, so a transformation that read the clock would answer
/// differently on a later day while still being treated as the same output.</param>
[ExcludeFromCodeCoverage(Justification = "Transformation request record - no logic to test.")]
public sealed record SqliteViewWriteRequest(
    string SourceDatabasePath,
    string TargetDatabasePath,
    IReadOnlyList<SqliteViewPart> Parts,
    DateTimeOffset QueryDate);

[ExcludeFromCodeCoverage(Justification = "Transformation result record - no logic to test.")]
public sealed record SqliteViewWriteResult(IReadOnlyList<SqliteViewTable> Tables);

/// <summary>Builds the SQLite read model from the staging database.
///
/// Local paths in, local database out: the implementation owns the database engine and knows nothing
/// about object storage, which keeps the engine out of Core and lets the export stage be tested
/// without it. It also lets the transformation itself be exercised against a real database with no
/// pipeline around it.</summary>
public interface ISqliteViewWriter
{
    Task<SqliteViewWriteResult> WriteAsync(
        SqliteViewWriteRequest request,
        CancellationToken cancellationToken = default);
}
