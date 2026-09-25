using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Optimise. Input: NormalisedFileSet. Output: OptimisedFileSet.
/// Each file resolves to either the normalised original (nothing to do) or a rewritten artefact in
/// optimised/, with projection, conversion and filtering applied as the definition asks.</summary>
public class OptimiseStageTests
{
    private const string Header = "CHANGE_TYPE|CPH|HOLDING_NAME|EASTING";
    private const string Key = "sam_cph_holdings/sam_cph_holdings_20251113121333.parquet";

    private readonly InMemoryEtlPipelineStorage _storage = new();

    private InMemoryBlobStorage Normalised => _storage.Folder(EtlPipelineFolders.Normalised);
    private InMemoryBlobStorage Optimised => _storage.Folder(EtlPipelineFolders.Optimised);

    /// <summary>Auto-detection is on by default; the unconfigured shape is detection switched off
    /// with nothing else asked for.</summary>
    private static DataSetDefinition Definition() =>
        new("sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            AutoDetectColumnTypes = false
        };

    private Task<List<OptimisedFileSet>> RunAsync(DataSetDefinition definition, params string[] keys) =>
        StageRunner.RunAsync(
            new OptimiseStage(_storage, NullLogger<OptimiseStage>.Instance),
            [new NormalisedFileSet(definition) { Files = keys }]);

    private void PutNormalised(string key, string header, params string[] rows)
        => Normalised.Put(key, ParquetFixture.From(header, rows));

    [Fact]
    public async Task References_the_normalised_file_without_touching_it_when_the_dataset_configures_nothing()
    {
        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456");

        var output = await RunAsync(Definition(), Key);

        output.Single().Files.Should().Equal(new OptimisedFile(EtlPipelineFolders.Normalised, Key));
        Optimised.Keys.Should().BeEmpty("a pass-through file is never copied");
    }

    [Fact]
    public async Task Converts_columns_to_the_types_their_values_look_like()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, []);

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456", "U|01/001/0002|New Farm|654321");

        var output = await RunAsync(definition, Key);

        output.Single().Files.Should().Equal(new OptimisedFile(EtlPipelineFolders.Optimised, Key));

        ParquetFixture.SchemaOf(Optimised.BytesOf(Key)).Should().Equal(
            ("CHANGE_TYPE", typeof(ReadOnlyMemory<char>)),
            ("CPH", typeof(ReadOnlyMemory<char>)),
            ("HOLDING_NAME", typeof(ReadOnlyMemory<char>)),
            ("EASTING", typeof(long)));

        ParquetFixture.ToLines(Optimised.BytesOf(Key)).Should().Equal(
            Header,
            "I|01/001/0001|Old Farm|123456",
            "U|01/001/0002|New Farm|654321");
    }

    [Fact]
    public async Task Converts_columns_to_every_detected_type_not_just_integers()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, []);

        PutNormalised(Key, "CHANGE_TYPE|CPH|SCORE|FLAG|BORN|STAMP",
            "I|01/001/0001|1.5|true|2025-11-13|2025-11-13T10:30:15");

        await RunAsync(definition, Key);

        ParquetFixture.SchemaOf(Optimised.BytesOf(Key)).Should().Equal(
            ("CHANGE_TYPE", typeof(ReadOnlyMemory<char>)),
            ("CPH", typeof(ReadOnlyMemory<char>)),
            ("SCORE", typeof(double)),
            ("FLAG", typeof(bool)),
            // DATE reads back as DateTime through Parquet.Net, so SchemaOf cannot tell Date from
            // Timestamp - both land on the same stored logical type's CLR mapping.
            ("BORN", typeof(DateTime)),
            ("STAMP", typeof(DateTime)));
    }

    [Fact]
    public async Task Keeps_a_leading_zero_identifier_a_string_even_though_it_looks_numeric()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, []);

        PutNormalised(Key, "CHANGE_TYPE|CPH|CODE", "I|01/001/0001|007");

        await RunAsync(definition, Key);

        ParquetFixture.SchemaOf(Optimised.BytesOf(Key)).Should().Contain(("CODE", typeof(ReadOnlyMemory<char>)));
        ParquetFixture.ToLines(Optimised.BytesOf(Key)).Should().Contain("I|01/001/0001|007");
    }

    [Fact]
    public async Task An_explicit_column_type_wins_over_what_detection_would_choose()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            ColumnTypes = new Dictionary<string, ColumnDataType> { ["EASTING"] = ColumnDataType.Decimal },
            DecimalPrecision = 10,
            DecimalScale = 2
        };

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456");

        var output = await RunAsync(definition, Key);

        ParquetFixture.SchemaOf(Optimised.BytesOf(Key)).Should().Contain(("EASTING", typeof(decimal)));
    }

    [Fact]
    public async Task Copies_a_file_verbatim_when_the_plan_changes_nothing()
    {
        // Detection on, but nothing in the file resolves to a non-string type - the artefact still
        // materialises under optimised/, as the exact same bytes.
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, []);

        var content = ParquetFixture.From(Header, "I|01/001/0001|Old Farm|not-a-number");
        Normalised.Put(Key, content);

        var output = await RunAsync(definition, Key);

        output.Single().Files.Should().Equal(new OptimisedFile(EtlPipelineFolders.Optimised, Key));
        Optimised.BytesOf(Key).Should().Equal(content, "an identity rewrite is a byte copy");
    }

    [Fact]
    public async Task Drops_excluded_columns_but_keeps_the_ones_the_merge_needs()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            AutoDetectColumnTypes = false,
            ExcludedColumns = ["EASTING", "CPH", "CHANGE_TYPE"]
        };

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456");

        await RunAsync(definition, Key);

        ParquetFixture.ToLines(Optimised.BytesOf(Key)).Should().Equal(
            "CHANGE_TYPE|CPH|HOLDING_NAME",
            "I|01/001/0001|Old Farm");
    }

    [Fact]
    public async Task Keeps_only_the_included_columns_plus_the_ones_the_merge_needs()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            AutoDetectColumnTypes = false,
            IncludedColumns = ["HOLDING_NAME"]
        };

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456");

        await RunAsync(definition, Key);

        ParquetFixture.ToLines(Optimised.BytesOf(Key)).Should().Equal(
            "CHANGE_TYPE|CPH|HOLDING_NAME",
            "I|01/001/0001|Old Farm");
    }

    [Fact]
    public async Task The_audit_sequence_column_is_merge_required_too()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [],
            Audit: new AuditColumns("SEQ", "CHANGED_AT"))
        {
            AutoDetectColumnTypes = false,
            IncludedColumns = ["HOLDING_NAME"]
        };

        PutNormalised(Key, "CHANGE_TYPE|CPH|SEQ|HOLDING_NAME|EASTING", "I|01/001/0001|42|Old Farm|123456");

        await RunAsync(definition, Key);

        ParquetFixture.ToLines(Optimised.BytesOf(Key)).Should().Equal(
            "CHANGE_TYPE|CPH|SEQ|HOLDING_NAME",
            "I|01/001/0001|42|Old Farm");
    }

    [Fact]
    public async Task Drops_the_rows_the_filter_rejects()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            AutoDetectColumnTypes = false,
            RowFilter = record => record["CHANGE_TYPE"] as string != "D"
        };

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456", "D|01/001/0002|Keep Farm|654321");

        await RunAsync(definition, Key);

        ParquetFixture.ToLines(Optimised.BytesOf(Key)).Should().Equal(
            Header,
            "I|01/001/0001|Old Farm|123456");
    }

    [Fact]
    public async Task Lets_the_filter_read_a_column_the_projection_drops_as_its_raw_string()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            AutoDetectColumnTypes = false,
            ExcludedColumns = ["EASTING"],
            RowFilter = record => record["EASTING"] as string == "123456"
        };

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456", "I|01/001/0002|New Farm|654321");

        await RunAsync(definition, Key);

        ParquetFixture.ToLines(Optimised.BytesOf(Key)).Should().Equal(
            "CHANGE_TYPE|CPH|HOLDING_NAME",
            "I|01/001/0001|Old Farm");
    }

    [Fact]
    public async Task Gives_the_filter_typed_values_for_the_columns_it_keeps()
    {
        var seen = new List<object?>();

        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            RowFilter = record =>
            {
                seen.Add(record["EASTING"]);
                return true;
            }
        };

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456");

        await RunAsync(definition, Key);

        seen.Should().Equal(new object?[] { 123456L }, "detection typed EASTING Int64 before the filter saw it");
    }

    [Fact]
    public async Task Writes_a_schema_only_file_when_the_filter_drops_every_row()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            AutoDetectColumnTypes = false,
            RowFilter = _ => false
        };

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456");

        var output = await RunAsync(definition, Key);

        output.Single().Files.Should().Equal(new OptimisedFile(EtlPipelineFolders.Optimised, Key));
        ParquetFixture.ToLines(Optimised.BytesOf(Key)).Should().Equal(Header);
    }

    [Fact]
    public async Task Reuses_an_optimised_file_that_already_exists()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, []);

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456");
        Optimised.Put(Key, "already optimised");

        var output = await RunAsync(definition, Key);

        output.Single().Files.Should().Equal(new OptimisedFile(EtlPipelineFolders.Optimised, Key));
        Optimised.ContentOf(Key).Should().Be("already optimised");
    }

    [Fact]
    public async Task Fails_with_a_diagnosable_error_when_a_value_cannot_be_converted()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            ColumnTypes = new Dictionary<string, ColumnDataType> { ["EASTING"] = ColumnDataType.Int64 }
        };

        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|not-a-number");

        var run = async () => await RunAsync(definition, Key);

        var failure = (await run.Should().ThrowAsync<SourceFileConversionException>()).Which;

        failure.DatasetName.Should().Be("sam_cph_holdings");
        failure.ObjectKey.Should().Be(Key);
        failure.ColumnName.Should().Be("EASTING");
        failure.ErrorDetail.RecordNumber.Should().Be(1);
        failure.ErrorDetail.Expected.Should().Be("Int64");
        failure.ErrorDetail.Actual.Should().Be("not-a-number");

        Optimised.Keys.Should().BeEmpty("a failed write must not leave an artefact a re-run would skip");
    }

    [Fact]
    public async Task Rejects_a_definition_that_both_includes_and_excludes()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            AutoDetectColumnTypes = false,
            IncludedColumns = ["HOLDING_NAME"],
            ExcludedColumns = ["EASTING"]
        };

        var run = async () => await RunAsync(definition, Key);

        await run.Should().ThrowAsync<InvalidOperationException>().WithMessage("*mutually exclusive*");
    }

    [Fact]
    public async Task Rejects_a_column_type_declared_for_a_merge_required_column()
    {
        var definition = new DataSetDefinition(
            "sam_cph_holdings", "sam_cph_holdings_{0}", ["CPH"], ChangeType.HeaderName, [])
        {
            ColumnTypes = new Dictionary<string, ColumnDataType> { ["CPH"] = ColumnDataType.Int64 }
        };

        var run = async () => await RunAsync(definition, Key);

        await run.Should().ThrowAsync<InvalidOperationException>().WithMessage("*'CPH'*string*");
    }

    [Fact]
    public async Task Lists_the_normalised_folder_when_the_payload_carries_no_files()
    {
        PutNormalised(Key, Header, "I|01/001/0001|Old Farm|123456");

        var output = await StageRunner.RunAsync(
            new OptimiseStage(_storage, NullLogger<OptimiseStage>.Instance),
            [new NormalisedFileSet(Definition())]);

        output.Single().Files.Should().Equal(new OptimisedFile(EtlPipelineFolders.Normalised, Key));
    }
}
