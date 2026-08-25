using DuckDB.NET.Data;
using FluentAssertions;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Infrastructure.EtlPipeline.Staging;
using Microsoft.Extensions.Logging.Abstractions;
using Parquet;
using Parquet.Schema;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Staging;

/// <summary>Exercises the real DuckDB writer against real Parquet files on disk: the point of the
/// stage is that the database it leaves behind can be opened and queried on its own.</summary>
public class DuckDbStagingDatabaseWriterTests : IDisposable
{
    private readonly DuckDbStagingDatabaseWriter _sut = new(NullLogger<DuckDbStagingDatabaseWriter>.Instance);
    private readonly string _tempDir;

    public DuckDbStagingDatabaseWriterTests()
    {
        _tempDir = Directory.CreateTempSubdirectory("duckdb-staging-tests-").FullName;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    private string DatabasePath => Path.Combine(_tempDir, "staging.duckdb");

    [Fact]
    public async Task Loads_a_snapshot_into_a_table_of_the_same_name()
    {
        var source = Parquet("sam_cph_holdings", "CPH|HOLDING_NAME", "01/001/0001|Updated Farm", "01/001/0003|New Farm");

        var result = await _sut.WriteAsync([source], DatabasePath);

        result.Tables.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new StagingTable("sam_cph_holdings", source.SnapshotKey, 2));

        Query("SELECT CPH || '|' || HOLDING_NAME FROM sam_cph_holdings ORDER BY CPH")
            .Should().Equal("01/001/0001|Updated Farm", "01/001/0003|New Farm");
    }

    [Fact]
    public async Task Loads_every_snapshot_into_the_one_database()
    {
        await _sut.WriteAsync(
            [
                Parquet("sam_cph_holdings", "CPH", "01/001/0001"),
                Parquet("cts_keeper", "CPH", "02/002/0002", "03/003/0003")
            ],
            DatabasePath);

        Query("SELECT table_name FROM duckdb_tables() ORDER BY table_name")
            .Should().Equal("cts_keeper", "sam_cph_holdings");
        Query("SELECT count(*)::VARCHAR FROM cts_keeper").Should().Equal("2");
    }

    [Fact]
    public async Task Row_count_matches_the_source_parquet_exactly()
    {
        var rows = Enumerable.Range(0, 500).Select(index => $"01/001/{index:0000}").ToArray();

        var result = await _sut.WriteAsync([Parquet("sam_cph_holdings", "CPH", rows)], DatabasePath);

        result.Tables.Single().RowCount.Should().Be(500);
        Query("SELECT count(*)::VARCHAR FROM sam_cph_holdings").Should().Equal("500");
    }

    [Fact]
    public async Task Loads_a_snapshot_holding_no_rows()
    {
        var result = await _sut.WriteAsync([Parquet("sam_cph_holdings", "CPH")], DatabasePath);

        result.Tables.Single().RowCount.Should().Be(0);
        Query("SELECT count(*)::VARCHAR FROM sam_cph_holdings").Should().Equal("0");
    }

    [Fact]
    public async Task The_database_is_queryable_after_the_writer_has_closed_it()
    {
        await _sut.WriteAsync([Parquet("sam_cph_holdings", "CPH|HOLDING_NAME", "01/001/0002|Keep Farm")], DatabasePath);

        File.Exists(DatabasePath).Should().BeTrue();
        Query("SELECT HOLDING_NAME FROM sam_cph_holdings").Should().Equal("Keep Farm");
    }

    [Fact]
    public async Task Refuses_to_write_over_an_existing_database()
    {
        await File.WriteAllTextAsync(DatabasePath, "already here");

        var act = () => _sut.WriteAsync([Parquet("sam_cph_holdings", "CPH", "01/001/0001")], DatabasePath);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*already exists*");
    }

    [Fact]
    public async Task Fails_when_the_parquet_source_is_missing()
    {
        var missing = new StagingTableSource(
            "sam_cph_holdings", Path.Combine(_tempDir, "absent.parquet"), "snapshots/absent.parquet");

        var act = () => _sut.WriteAsync([missing], DatabasePath);

        await act.Should().ThrowAsync<Exception>();
    }

    private StagingTableSource Parquet(string tableName, string header, params string[] rows)
    {
        var path = Path.Combine(_tempDir, $"{tableName}.parquet");
        File.WriteAllBytes(path, ParquetBytes(header, rows));

        return new StagingTableSource(tableName, path, $"snapshots/{tableName}/{tableName}_20251115121333.parquet");
    }

    private static byte[] ParquetBytes(string header, string[] rows)
        => ParquetBytesAsync(header, rows).GetAwaiter().GetResult();

    private static async Task<byte[]> ParquetBytesAsync(string header, string[] rows)
    {
        var columns = header.Split('|');
        var values = columns.Select(_ => new List<string?>()).ToArray();

        foreach (var cells in rows.Select(row => row.Split('|')))
        {
            for (var column = 0; column < columns.Length; column++)
            {
                values[column].Add(column < cells.Length ? cells[column] : null);
            }
        }

        var fields = columns.Select(column => new DataField<string>(column)).ToArray();
        var buffer = new MemoryStream();

        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), buffer))
        {
            using var rowGroup = writer.CreateRowGroup();

            for (var column = 0; column < fields.Length; column++)
            {
                await rowGroup.WriteAsync(fields[column], (IReadOnlyCollection<string?>)values[column]);
            }
        }

        return buffer.ToArray();
    }

    private List<string> Query(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={DatabasePath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = sql;

        var results = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            results.Add(reader.GetString(0));
        }

        return results;
    }
}
