using DuckDB.NET.Data;
using FluentAssertions;
using FluentAssertions.Execution;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Infrastructure.EtlPipeline.Staging;
using KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd;

/// <summary>The optimise stage end to end: a dataset that declares a column type, a projection and
/// a row filter, run through the real pipeline and the real DuckDB writer. What lands in staging
/// proves the whole chain - the typed column arrives as BIGINT, the projected column never reaches
/// the snapshot, and the filtered row is gone before the merge ever sees it.</summary>
public sealed class OptimiseEndToEndCiTests
{
    private const string Timestamp = "20251113121333";

    private static readonly DataSetDefinition TypedDataset = new(
        "sam_typed",
        "litprd/LITP_SAMTYPED_{0}",
        ["PARTY_ID"],
        ChangeType.HeaderName,
        [],
        IngestionMode: DataSetIngestionMode.Delta)
    {
        ColumnTypes = new Dictionary<string, ColumnDataType> { ["SCORE"] = ColumnDataType.Int64 },
        // REGION is projected out, yet the filter still reads it: the predicate sees every source
        // column, typed or not.
        IncludedColumns = ["PARTY_NAME", "SCORE"],
        RowFilter = record => record["REGION"] as string != "XX"
    };

    private const string Content =
        "PARTY_ID|PARTY_NAME|SCORE|REGION|CHANGE_TYPE\n" +
        "P0000001|Alice Holder|10|GB|I\n" +
        "P0000002|Bob Holder|20|XX|I\n" +
        "P0000003|Carol Holder|30|GB|I\n";

    [Fact]
    public async Task Configured_dataset_reaches_staging_typed_projected_and_filtered()
    {
        using var host = InMemoryEtlPipelineHost.Create(
            EtlFixtures.RunClock,
            [TypedDataset],
            new DuckDbStagingDatabaseWriter(new NullLogger<DuckDbStagingDatabaseWriter>()));

        await host.PutEncryptedSourceFileAsync($"litprd/LITP_SAMTYPED_{Timestamp}.csv", Content);
        await host.RunAsync();

        using var scope = new AssertionScope();

        host.Folders.Folder(EtlPipelineFolders.Optimised).Keys.Should().ContainSingle()
            .Which.Should().Be($"sam_typed/LITP_SAMTYPED_{Timestamp}.parquet",
                "a configured file is rewritten under optimised/ rather than passed through");

        var databaseKey = host.Folders.Folder(EtlPipelineFolders.Staging).Keys.Should().ContainSingle().Subject;
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Staging, databaseKey, ".duckdb");

        try
        {
            using var connection = new DuckDBConnection($"Data Source={path}");
            await connection.OpenAsync();

            (await QueryPairsAsync(connection,
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'sam_typed' ORDER BY ordinal_position"))
                .Should().Equal(
                    [("PARTY_ID", "VARCHAR"), ("PARTY_NAME", "VARCHAR"), ("SCORE", "BIGINT")],
                    "the declared Int64 survives to DuckDB, the projected-out columns are gone");

            (await QueryPairsAsync(connection, "SELECT PARTY_ID, SCORE::VARCHAR FROM sam_typed ORDER BY PARTY_ID"))
                .Should().Equal(
                    [("P0000001", "10"), ("P0000003", "30")],
                    "the row whose projected-out REGION is XX was dropped by the filter");
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<List<(string, string)>> QueryPairsAsync(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        using var reader = await command.ExecuteReaderAsync();

        var rows = new List<(string, string)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1)));
        }

        return rows;
    }
}
