using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>The merge for a dataset whose deltas describe their own ordering: rows are folded in audit
/// sequence rather than file order, deletes are applied, and the audit columns are kept out of the
/// snapshot so it carries the same shape as the baseline file.</summary>
public class AuditAwareMergeTests
{
    /// <summary>The delta shape: the audit columns, the per-file counters, then the row itself.</summary>
    private const string DeltaHeader =
        "LID_AUD_ID|LID_AUD_TYPE|LID_AUD_DATETIME|RECORD_TYPE|RECORD_COUNT|LID_ID|LID_LOCATION_ID";

    /// <summary>The baseline shape: no audit columns, because it describes state rather than change.</summary>
    private const string BulkHeader = "RECORD_TYPE|RECORD_COUNT|LID_ID|LID_LOCATION_ID";

    private const string SnapshotHeader = "LID_ID|LID_LOCATION_ID";

    private static readonly DataSetDefinition Cts = new(
        "cts_location_identifiers",
        "cads/cts/",
        ["LID_ID"],
        "LID_AUD_TYPE",
        [],
        Format: FileFormat.Hcdt,
        IngestionMode: DataSetIngestionMode.Delta,
        Audit: new AuditColumns("LID_AUD_ID", "LID_AUD_DATETIME"))
    {
        ExcludedColumns = ["LID_AUD_ID", "LID_AUD_TYPE", "LID_AUD_DATETIME", "RECORD_TYPE", "RECORD_COUNT"]
    };

    private readonly ParquetDeltaMergeEngine _engine = new(NullLogger<ParquetDeltaMergeEngine>.Instance);

    private static DeltaMergeSource Source(string key, string header, params string[] rows)
    {
        var content = ParquetFixture.From(header, rows);

        return new DeltaMergeSource(key, _ => Task.FromResult<Stream>(new MemoryStream(content)));
    }

    /// <summary>A delta row: the audit sequence and type, then the location it describes.</summary>
    private static string Row(string sequence, string auditType, string locationId, string address)
        => $"{sequence}|{auditType}|2026-08-23 06:30:10|D|1|{locationId}|{address}";

    private static string BulkRow(string locationId, string address)
        => $"D|1|{locationId}|{address}";

    private async Task<(IReadOnlyList<string> Lines, DeltaMergeResult Result)> MergeAsync(
        DataSetDefinition definition,
        DeltaMergeSource? baseSnapshot,
        params DeltaMergeSource[] deltas)
    {
        using var output = new MemoryStream();

        var result = await _engine.MergeAsync(definition, baseSnapshot, deltas, output);

        return (ParquetFixture.ToLines(output.ToArray()), result);
    }

    /// <summary>The sample updates one location twice inside a single file. File order happens to agree
    /// with the sequence there; nothing in the feed guarantees that it will.</summary>
    [Fact]
    public async Task Applies_the_rows_of_one_delta_in_audit_sequence_rather_than_file_order()
    {
        var (lines, _) = await MergeAsync(
            Cts,
            null,
            Source("delta", DeltaHeader,
                Row("701929", "U", "287594", "Second"),
                Row("701928", "U", "287594", "First")));

        lines.Should().Equal(SnapshotHeader, "287594|Second");
    }

    [Fact]
    public async Task Removes_a_deleted_row_from_the_snapshot()
    {
        var (lines, result) = await MergeAsync(
            Cts,
            Source("bulk", BulkHeader, BulkRow("287594", "Old Address"), BulkRow("287595", "Keep")),
            Source("delta", DeltaHeader, Row("701930", "D", "287594", "Old Address")));

        lines.Should().Equal(SnapshotHeader, "287595|Keep");
        result.RowsDeleted.Should().Be(1);
        result.RowCount.Should().Be(1);
    }

    [Fact]
    public async Task Reinstates_a_key_a_later_insert_brings_back()
    {
        var (lines, _) = await MergeAsync(
            Cts,
            Source("bulk", BulkHeader, BulkRow("287594", "Old Address")),
            Source("delete", DeltaHeader, Row("701930", "D", "287594", "Old Address")),
            Source("insert", DeltaHeader, Row("701931", "I", "287594", "New Address")));

        lines.Should().Equal(SnapshotHeader, "287594|New Address");
    }

    /// <summary>The key-to-row map holds indices, so compacting the rows on a delete would shift every
    /// later row out from under its key and update the wrong one - or read off the end.</summary>
    [Fact]
    public async Task Keeps_the_rows_after_a_deleted_one_addressable_by_their_own_key()
    {
        var (lines, _) = await MergeAsync(
            Cts,
            Source("bulk", BulkHeader,
                BulkRow("287594", "First"),
                BulkRow("287595", "Second"),
                BulkRow("287596", "Third")),
            Source("delta", DeltaHeader,
                Row("701930", "D", "287594", "First"),
                Row("701931", "U", "287596", "Third Updated")));

        lines.Should().Equal(SnapshotHeader, "287595|Second", "287596|Third Updated");
    }

    [Fact]
    public async Task Counts_but_does_not_apply_a_row_whose_audit_type_is_unrecognised()
    {
        var (lines, result) = await MergeAsync(
            Cts,
            null,
            Source("delta", DeltaHeader,
                Row("701930", "I", "287594", "Kept"),
                Row("701931", "X", "287595", "Nonsense")));

        lines.Should().Equal(SnapshotHeader, "287594|Kept");
        result.RowsRejected.Should().Be(1);
    }

    /// <summary>The snapshot carries the baseline file's data shape: the audit columns describe the
    /// change rather than the row, and the counters restart in every file, so neither means anything
    /// once rows from many files are merged.</summary>
    [Fact]
    public async Task Suppresses_the_excluded_columns_so_the_snapshot_matches_the_baseline_shape()
    {
        var (lines, _) = await MergeAsync(
            Cts,
            Source("bulk", BulkHeader, BulkRow("287594", "Old Address")),
            Source("delta", DeltaHeader, Row("701930", "U", "287594", "New Address")));

        lines.Should().Equal(SnapshotHeader, "287594|New Address");
    }

    /// <summary>The sequence is shared with the other tables in the same extract, so it always has
    /// gaps. A gap says nothing about a missing file.</summary>
    [Fact]
    public async Task Reads_only_the_order_of_the_audit_sequence_not_its_continuity()
    {
        var (lines, result) = await MergeAsync(
            Cts,
            null,
            Source("delta", DeltaHeader,
                Row("701928", "I", "287594", "First"),
                Row("702149", "I", "287595", "Second")));

        lines.Should().Equal(SnapshotHeader, "287594|First", "287595|Second");
        result.RowsRejected.Should().Be(0);
    }

    [Fact]
    public async Task Applies_a_header_only_delta_as_nothing_at_all()
    {
        var (lines, result) = await MergeAsync(
            Cts,
            Source("bulk", BulkHeader, BulkRow("287594", "Old Address")),
            Source("delta", DeltaHeader));

        lines.Should().Equal(SnapshotHeader, "287594|Old Address");
        result.DeltasApplied.Should().Be(1);
        result.RowsUpserted.Should().Be(0);
    }

    /// <summary>A dataset with no audit lane is untouched by any of it: file order stands and deletes
    /// are still counted rather than applied.</summary>
    [Fact]
    public async Task Leaves_a_dataset_with_no_audit_lane_in_file_order_with_deletes_ignored()
    {
        var samCph = new DataSetDefinition(
            "sam_cph_holdings",
            "sam_cph_holdings_{0}",
            ["CPH"],
            ChangeType.HeaderName,
            [],
            IngestionMode: DataSetIngestionMode.Delta);

        var (lines, result) = await MergeAsync(
            samCph,
            null,
            Source("delta", "LID_AUD_ID|CHANGE_TYPE|CPH|HOLDING_NAME",
                "701929|I|01/001/0001|Second",
                "701928|U|01/001/0001|Last In The File",
                "701930|D|01/001/0001|Should Not Delete"));

        lines.Should().Equal("LID_AUD_ID|CPH|HOLDING_NAME", "701928|01/001/0001|Last In The File");
        result.RowsIgnoredDeletes.Should().Be(1);
        result.RowsDeleted.Should().Be(0);
    }
}
