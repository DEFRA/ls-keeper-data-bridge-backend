using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;
using Parquet.Schema;

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
        => Source(key, ParquetFixture.From(header, rows));

    private static DeltaMergeSource Source(string key, byte[] content)
        => new(key, _ => Task.FromResult<Stream>(new MemoryStream(content)));

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
    public async Task Deletes_apply_when_the_dataset_defines_an_audit_order()
    {
        // With audit configured a delete tombstones the row: the slot is held so a later insert can
        // reinstate it, and a delete for a key never seen - or already tombstoned - is a no-op.
        var audited = SamCph with { Audit = new AuditColumns("SEQ", "CHANGED_AT") };

        using var output = new MemoryStream();

        var result = await _engine.MergeAsync(
            audited,
            null,
            [
                Source("seed", DeltaHeader, "I|01/001/0001|Keep Farm", "I|01/001/0002|Doomed Farm"),
                Source("delete", DeltaHeader, "D|01/001/0002|Doomed Farm", "D|01/001/0002|Doomed Farm", "D|01/001/0099|Ghost Farm"),
                Source("adds", "CHANGE_TYPE|CPH|HOLDING_NAME|NEW_COLUMN", "I|01/001/0003|New Farm|X")
            ],
            output);

        result.RowsDeleted.Should().Be(1);

        ParquetFixture.ToLines(output.ToArray()).Should().Equal(
            "CPH|HOLDING_NAME|NEW_COLUMN",
            "01/001/0001|Keep Farm|",
            "01/001/0003|New Farm|X");
    }

    [Fact]
    public async Task Fails_to_write_when_no_file_supplied_a_schema()
    {
        using var output = new MemoryStream();

        var merge = async () => await _engine.MergeAsync(SamCph, null, [], output);

        await merge.Should().ThrowAsync<InvalidOperationException>().WithMessage("*Nothing to write*");
    }

    [Fact]
    public async Task Rewrites_the_snapshot_unchanged_when_there_are_no_deltas()
    {
        var (lines, result) = await MergeAsync(Source("snapshot", SnapshotHeader, "01/001/0001|Old Farm"));

        lines.Should().Equal("CPH|HOLDING_NAME", "01/001/0001|Old Farm");
        result.DeltasApplied.Should().Be(0);
        result.RowCount.Should().Be(1);
    }

    [Fact]
    public async Task Typed_columns_merge_into_a_typed_snapshot()
    {
        // The shape optimise output takes when a column converts: merge-required columns stay
        // strings, the rest carry their native type.
        var delta = ParquetFixture.FromTyped(
            (new DataField<string?>("CHANGE_TYPE"), new string?[] { "I", "I" }),
            (new DataField<string?>("CPH"), new string?[] { "01/001/0001", "01/001/0002" }),
            (new DataField<long?>("EASTING"), new long?[] { 123456, 654321 }));

        using var output = new MemoryStream();

        await _engine.MergeAsync(SamCph, null, [Source("delta", delta)], output);

        var bytes = output.ToArray();

        ParquetFixture.SchemaOf(bytes).Should().Equal(
            ("CPH", typeof(ReadOnlyMemory<char>)),
            ("EASTING", typeof(long)));

        ParquetFixture.ToLines(bytes).Should().Equal(
            "CPH|EASTING",
            "01/001/0001|123456",
            "01/001/0002|654321");
    }

    [Fact]
    public async Task A_column_two_files_disagree_on_the_type_of_widens_to_string()
    {
        // File one detects EASTING as Int64, file two carries it as text: the merged column widens
        // rather than fails, and the already-held value stays legible because it was stored as
        // canonical text.
        var typed = ParquetFixture.FromTyped(
            (new DataField<string?>("CHANGE_TYPE"), new string?[] { "I" }),
            (new DataField<string?>("CPH"), new string?[] { "01/001/0001" }),
            (new DataField<long?>("EASTING"), new long?[] { 123456 }));

        var texty = ParquetFixture.From("CHANGE_TYPE|CPH|EASTING", "I|01/001/0002|654321");

        using var output = new MemoryStream();

        await _engine.MergeAsync(SamCph, null, [Source("first", typed), Source("second", texty)], output);

        var bytes = output.ToArray();

        ParquetFixture.SchemaOf(bytes).Should().Equal(
            ("CPH", typeof(ReadOnlyMemory<char>)),
            ("EASTING", typeof(ReadOnlyMemory<char>)));

        ParquetFixture.ToLines(bytes).Should().Equal(
            "CPH|EASTING",
            "01/001/0001|123456",
            "01/001/0002|654321");
    }

    [Fact]
    public async Task A_non_nullable_column_widens_to_nullable_when_a_later_file_drops_it()
    {
        // The column arrived non-nullable, but the merge can still leave nulls in it - a file that
        // drops it leaves nulls behind - so the output field is widened before the schema is built.
        var typed = ParquetFixture.FromTyped(
            (new DataField<string?>("CHANGE_TYPE"), new string?[] { "I", "I" }),
            (new DataField<string?>("CPH"), new string?[] { "01/001/0001", "01/001/0002" }),
            (new DataField<long>("EASTING"), new long[] { 123456, 654321 }),
            (new DecimalDataField("AREA", 10, 2, isNullable: false), new decimal[] { 1.5m, 2.5m }));

        using var output = new MemoryStream();

        await _engine.MergeAsync(
            SamCph, null,
            [Source("first", typed), Source("second", "CHANGE_TYPE|CPH", "U|01/001/0001")],
            output);

        var bytes = output.ToArray();

        ParquetFixture.SchemaOf(bytes).Should().Equal(
            ("CPH", typeof(ReadOnlyMemory<char>)),
            ("EASTING", typeof(long)),
            ("AREA", typeof(decimal)));

        ParquetFixture.ToLines(bytes).Should().Equal(
            "CPH|EASTING|AREA",
            "01/001/0001||",
            "01/001/0002|654321|2.50");
    }

    [Fact]
    public async Task A_string_column_upgrades_to_the_type_a_later_file_carries()
    {
        // The reverse of widening: a column established as text adopts the incoming type when every
        // held value converts - a snapshot that predates typed input migrates rather than pinning
        // the column to string forever.
        var texty = ParquetFixture.From("CHANGE_TYPE|CPH|EASTING", "I|01/001/0001|123456");

        var typed = ParquetFixture.FromTyped(
            (new DataField<string?>("CHANGE_TYPE"), new string?[] { "I" }),
            (new DataField<string?>("CPH"), new string?[] { "01/001/0002" }),
            (new DataField<long?>("EASTING"), new long?[] { 654321 }));

        using var output = new MemoryStream();

        await _engine.MergeAsync(SamCph, null, [Source("first", texty), Source("second", typed)], output);

        var bytes = output.ToArray();

        ParquetFixture.SchemaOf(bytes).Should().Equal(
            ("CPH", typeof(ReadOnlyMemory<char>)),
            ("EASTING", typeof(long)));

        ParquetFixture.ToLines(bytes).Should().Equal(
            "CPH|EASTING",
            "01/001/0001|123456",
            "01/001/0002|654321");
    }

    [Fact]
    public async Task A_string_column_stays_a_string_when_a_held_value_would_not_survive_conversion()
    {
        // "007" parses as a number but loses its zeros - the column stays text rather than silently
        // rewriting the held values, and the refusal is logged.
        var logger = new CapturingLogger<ParquetDeltaMergeEngine>();
        var engine = new ParquetDeltaMergeEngine(logger);

        var texty = ParquetFixture.From("CHANGE_TYPE|CPH|CODE", "I|01/001/0001|007");

        var typed = ParquetFixture.FromTyped(
            (new DataField<string?>("CHANGE_TYPE"), new string?[] { "I" }),
            (new DataField<string?>("CPH"), new string?[] { "01/001/0002" }),
            (new DataField<long?>("CODE"), new long?[] { 12 }));

        using var output = new MemoryStream();

        await engine.MergeAsync(SamCph, null, [Source("first", texty), Source("second", typed)], output);

        var bytes = output.ToArray();

        ParquetFixture.SchemaOf(bytes).Should().Equal(
            ("CPH", typeof(ReadOnlyMemory<char>)),
            ("CODE", typeof(ReadOnlyMemory<char>)));

        ParquetFixture.ToLines(bytes).Should().Equal(
            "CPH|CODE",
            "01/001/0001|007",
            "01/001/0002|12");

        logger.Warnings.Should().ContainSingle(w => w.Contains("CODE") && w.Contains("stays string"));
    }

    [Fact]
    public async Task An_upgraded_column_widens_back_to_string_when_a_later_file_disagrees()
    {
        // The upgrade is reversible: a string file arriving after the upgrade widens the column
        // back, and every cut of the data survives because it was held as text all along.
        var texty = ParquetFixture.From("CHANGE_TYPE|CPH|CODE", "I|01/001/0001|123");

        var typed = ParquetFixture.FromTyped(
            (new DataField<string?>("CHANGE_TYPE"), new string?[] { "I" }),
            (new DataField<string?>("CPH"), new string?[] { "01/001/0002" }),
            (new DataField<long?>("CODE"), new long?[] { 456 }));

        var backToText = ParquetFixture.From("CHANGE_TYPE|CPH|CODE", "I|01/001/0003|abc");

        using var output = new MemoryStream();

        await _engine.MergeAsync(
            SamCph, null,
            [Source("first", texty), Source("second", typed), Source("third", backToText)],
            output);

        var bytes = output.ToArray();

        ParquetFixture.SchemaOf(bytes).Should().Equal(
            ("CPH", typeof(ReadOnlyMemory<char>)),
            ("CODE", typeof(ReadOnlyMemory<char>)));

        ParquetFixture.ToLines(bytes).Should().Equal(
            "CPH|CODE",
            "01/001/0001|123",
            "01/001/0002|456",
            "01/001/0003|abc");
    }
}
