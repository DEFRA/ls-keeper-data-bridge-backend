using KeeperData.Core.EtlPipeline.Staging;
using Parquet;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;

/// <summary>
/// Stands in for <c>DuckDbStagingDatabaseWriter</c>, recording what the load stage asked it to write
/// and producing a database file of the right shape without a database engine.
///
/// Row counts are read from the Parquet sources rather than invented, so a test asserting on table
/// row counts is still asserting on what the snapshot stage actually produced.
/// </summary>
public sealed class RecordingStagingDatabaseWriter : IStagingDatabaseWriter
{
    private readonly List<IReadOnlyList<StagingTableSource>> _calls = [];

    /// <summary>Every set of sources handed to the writer, in call order.</summary>
    public IReadOnlyList<IReadOnlyList<StagingTableSource>> Calls => _calls;

    /// <summary>The sources from the single call, failing loudly if there was not exactly one.</summary>
    public IReadOnlyList<StagingTableSource> OnlyCall => _calls.Single();

    public async Task<StagingDatabaseWriteResult> WriteAsync(
        IReadOnlyList<StagingTableSource> sources,
        string databasePath,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sources);

        _calls.Add([.. sources]);

        var tables = new List<StagingTable>(sources.Count);

        foreach (var source in sources)
        {
            var rowCount = await RowCountAsync(source.ParquetPath, cancellationToken);
            tables.Add(new StagingTable(source.TableName, source.SnapshotKey, rowCount));
        }

        // The load stage uploads whatever is at this path, so it has to exist.
        await File.WriteAllTextAsync(databasePath, "recording-staging-writer", cancellationToken);

        return new StagingDatabaseWriteResult(tables);
    }

    private static async Task<long> RowCountAsync(string parquetPath, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await using var file = File.OpenRead(parquetPath);
        await using var reader = await ParquetReader.CreateAsync(file);

        var rows = 0L;

        for (var group = 0; group < reader.RowGroupCount; group++)
        {
            using var rowGroup = reader.OpenRowGroupReader(group);
            rows += rowGroup.RowCount;
        }

        return rows;
    }
}
