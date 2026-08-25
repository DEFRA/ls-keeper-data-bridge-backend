using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>The merge on its own: previous snapshot parquet + ordered delta parquets -> next snapshot
/// parquet. No storage, no DuckDB, no Mongo.</summary>
public class ParquetDeltaMergeEngineTests
{
    private const string DeltaHeader = "CHANGE_TYPE|CPH|HOLDING_NAME";
    private const string SnapshotHeader = "CPH|HOLDING_NAME";

    private static readonly DataSetDefinition SamCph =
        new("sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta);

    private readonly ParquetDeltaMergeEngine _engine = new(NullLogger<ParquetDeltaMergeEngine>.Instance);

    private static DeltaMergeSource Source(string key, string header, params string[] rows)
    {
        var content = ParquetFixture.From(header, rows);

        return new DeltaMergeSource(key, _ => Task.FromResult<Stream>(new MemoryStream(content)));
    }

    private async Task<(IReadOnlyList<string> Lines, DeltaMergeResult Result)> MergeAsync(
        DeltaMergeSource? baseSnapshot,
        params DeltaMergeSource[] deltas)
    {
        using var output = new MemoryStream();

        var result = await _engine.MergeAsync(SamCph, baseSnapshot, deltas, output);

        return (ParquetFixture.ToLines(output.ToArray()), result);
    }

    [Fact]
    public async Task Folds_the_tickets_fixture_into_the_expected_snapshot()
    {
        var (lines, result) = await MergeAsync(
            null,
            Source("20251113", DeltaHeader, "I|01/001/0001|Old Farm", "I|01/001/0002|Keep Farm"),
            Source("20251114", DeltaHeader, "U|01/001/0001|Updated Farm", "I|01/001/0003|New Farm"),
            Source("20251115", DeltaHeader, "D|01/001/0002|Should Not Delete"));

        lines.Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|Updated Farm",
            "01/001/0002|Keep Farm",
            "01/001/0003|New Farm");

        result.Should().BeEquivalentTo(new DeltaMergeResult
        {
            DeltasApplied = 3,
            RowsUpserted = 4,
            RowsIgnoredDeletes = 1,
            RowsRejected = 0,
            RowCount = 3
        });
    }

    [Fact]
    public async Task Folds_deltas_onto_an_existing_snapshot()
    {
        var (lines, _) = await MergeAsync(
            Source("snapshot", SnapshotHeader, "01/001/0001|Old Farm"),
            Source("delta", DeltaHeader, "U|01/001/0001|Updated Farm", "I|01/001/0002|New Farm"));

        lines.Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|Updated Farm",
            "01/001/0002|New Farm");
    }

    [Fact]
    public async Task Applies_deltas_in_the_order_given_so_the_last_writer_wins()
    {
        var (lines, _) = await MergeAsync(
            null,
            Source("first", DeltaHeader, "I|01/001/0001|First"),
            Source("second", DeltaHeader, "U|01/001/0001|Second"),
            Source("third", DeltaHeader, "U|01/001/0001|Third"));

        lines.Should().Equal("CPH|HOLDING_NAME", "01/001/0001|Third");
    }

    [Fact]
    public async Task A_later_row_in_the_same_delta_overrides_an_earlier_one()
    {
        var (lines, _) = await MergeAsync(
            null,
            Source("one", DeltaHeader, "I|01/001/0001|First", "U|01/001/0001|Second"));

        lines.Should().Equal("CPH|HOLDING_NAME", "01/001/0001|Second");
    }

    [Fact]
    public async Task Matches_rows_on_the_datasets_composite_primary_key()
    {
        var definition = SamCph with { PrimaryKeyHeaderNames = ["CPH", "HOLDING_NAME"] };

        using var output = new MemoryStream();

        await _engine.MergeAsync(
            definition,
            null,
            [Source("one", DeltaHeader, "I|01/001/0001|Old Farm", "U|01/001/0001|Updated Farm")],
            output);

        // Different HOLDING_NAME, so with a composite key these are two rows rather than an update.
        ParquetFixture.ToLines(output.ToArray()).Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|Old Farm",
            "01/001/0001|Updated Farm");
    }

    [Fact]
    public async Task Counts_but_does_not_apply_delete_rows()
    {
        var (lines, result) = await MergeAsync(
            Source("snapshot", SnapshotHeader, "01/001/0001|Keep Farm"),
            Source("delta", DeltaHeader, "D|01/001/0001|Should Not Delete"));

        lines.Should().Equal("CPH|HOLDING_NAME", "01/001/0001|Keep Farm");
        result.RowsIgnoredDeletes.Should().Be(1);
    }

    [Fact]
    public async Task Counts_but_does_not_apply_rows_with_an_unrecognised_change_type()
    {
        var (lines, result) = await MergeAsync(
            null,
            Source("delta", DeltaHeader, "I|01/001/0001|Keep Farm", "X|01/001/0002|Nonsense"));

        lines.Should().Equal("CPH|HOLDING_NAME", "01/001/0001|Keep Farm");
        result.RowsRejected.Should().Be(1);
    }

    [Fact]
    public async Task Rejects_a_delta_missing_a_primary_key_column()
    {
        var merge = async () => await MergeAsync(null, Source("delta", "CHANGE_TYPE|HOLDING_NAME", "I|Old Farm"));

        await merge.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*primary key column 'CPH'*");
    }

    [Fact]
    public async Task Nullifies_a_column_a_delta_no_longer_carries()
    {
        // The source extract dropped ADDRESS_PK part way through: the column stays, and the rows the
        // delta supplies have nothing for it.
        var (lines, _) = await MergeAsync(
            null,
            Source("before", "CHANGE_TYPE|CPH|HOLDING_NAME|ADDRESS_PK",
                "I|01/001/0001|Old Farm|ADDR001", "I|01/001/0002|Keep Farm|ADDR002"),
            Source("after", DeltaHeader, "U|01/001/0001|Updated Farm"));

        lines.Should().Equal(
            "CPH|HOLDING_NAME|ADDRESS_PK",
            "01/001/0001|Updated Farm|",
            "01/001/0002|Keep Farm|ADDR002");
    }

    [Fact]
    public async Task Nullifies_a_column_the_base_snapshot_carries_and_no_delta_does()
    {
        var (lines, _) = await MergeAsync(
            Source("snapshot", "CPH|HOLDING_NAME|ADDRESS_PK", "01/001/0001|Old Farm|ADDR001"),
            Source("delta", DeltaHeader, "U|01/001/0001|Updated Farm"));

        lines.Should().Equal("CPH|HOLDING_NAME|ADDRESS_PK", "01/001/0001|Updated Farm|");
    }

    [Fact]
    public async Task Keeps_a_column_a_later_delta_introduces()
    {
        var (lines, _) = await MergeAsync(
            null,
            Source("before", DeltaHeader, "I|01/001/0001|Old Farm"),
            Source("after", "CHANGE_TYPE|CPH|HOLDING_NAME|NEW_COLUMN", "I|01/001/0002|New Farm|VALUE"));

        lines.Should().Equal(
            "CPH|HOLDING_NAME|NEW_COLUMN",
            "01/001/0001|Old Farm|",
            "01/001/0002|New Farm|VALUE");
    }

    [Fact]
    public async Task Keeps_a_column_a_delta_introduces_over_the_base_snapshot()
    {
        var (lines, _) = await MergeAsync(
            Source("snapshot", SnapshotHeader, "01/001/0001|Old Farm"),
            Source("delta", "CHANGE_TYPE|CPH|HOLDING_NAME|ADDRESS_PK", "U|01/001/0001|Updated Farm|ADDR001"));

        lines.Should().Equal("CPH|HOLDING_NAME|ADDRESS_PK", "01/001/0001|Updated Farm|ADDR001");
    }

    [Fact]
    public async Task Warns_once_per_file_about_a_column_appearing_and_disappearing()
    {
        var logger = new CapturingLogger<ParquetDeltaMergeEngine>();
        var engine = new ParquetDeltaMergeEngine(logger);

        using var output = new MemoryStream();

        await engine.MergeAsync(
            SamCph,
            Source("snapshot", "CPH|HOLDING_NAME|ADDRESS_PK", "01/001/0001|Old Farm|ADDR001"),
            [
                Source("dropped", DeltaHeader, "U|01/001/0001|Updated Farm"),
                Source("added", "CHANGE_TYPE|CPH|HOLDING_NAME|ADDRESS_PK|NEW_COLUMN", "I|01/001/0002|New Farm|ADDR002|VALUE")
            ],
            output);

        var warnings = logger.Warnings;

        warnings.Should().ContainSingle(w => w.Contains("dropped") && w.Contains("ADDRESS_PK") && w.Contains("does not carry"));
        warnings.Should().ContainSingle(w => w.Contains("added") && w.Contains("NEW_COLUMN") && w.Contains("introduces"));

        // The file that establishes the schema is not drift, so it is not warned about.
        warnings.Should().NotContain(w => w.Contains("snapshot"));
    }

    [Fact]
    public async Task Reports_drifted_columns_once_so_the_run_status_can_show_them()
    {
        var (_, result) = await MergeAsync(
            Source("snapshot", "CPH|HOLDING_NAME|ADDRESS_PK", "01/001/0001|Old Farm|ADDR001"),
            Source("dropped", DeltaHeader, "U|01/001/0001|Updated Farm"),
            Source("dropped-again", DeltaHeader, "U|01/001/0001|Updated Twice"),
            Source("added", "CHANGE_TYPE|CPH|HOLDING_NAME|NEW_COLUMN", "I|01/001/0002|New Farm|VALUE"));

        result.ColumnsNullified.Should().Equal("ADDRESS_PK");
        result.ColumnsAdded.Should().Equal("NEW_COLUMN");
    }

    [Fact]
    public async Task Reports_no_drifted_columns_when_every_file_agrees()
    {
        var (_, result) = await MergeAsync(
            Source("snapshot", SnapshotHeader, "01/001/0001|Old Farm"),
            Source("delta", DeltaHeader, "U|01/001/0001|Updated Farm"));

        result.ColumnsNullified.Should().BeEmpty();
        result.ColumnsAdded.Should().BeEmpty();
    }

    [Fact]
    public async Task Still_rejects_a_delta_missing_a_primary_key_column_rather_than_nullifying_it()
    {
        var merge = async () => await MergeAsync(
            Source("snapshot", SnapshotHeader, "01/001/0001|Old Farm"),
            Source("delta", "CHANGE_TYPE|HOLDING_NAME", "U|Updated Farm"));

        await merge.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*primary key column 'CPH'*");
    }

    [Fact]
    public async Task Rewrites_the_snapshot_unchanged_when_there_are_no_deltas()
    {
        var (lines, result) = await MergeAsync(Source("snapshot", SnapshotHeader, "01/001/0001|Old Farm"));

        lines.Should().Equal("CPH|HOLDING_NAME", "01/001/0001|Old Farm");
        result.DeltasApplied.Should().Be(0);
        result.RowCount.Should().Be(1);
    }
}
