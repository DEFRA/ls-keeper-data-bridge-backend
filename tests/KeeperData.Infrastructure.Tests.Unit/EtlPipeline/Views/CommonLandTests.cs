using DuckDB.NET.Data;
using FluentAssertions;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Infrastructure.EtlPipeline.Views;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>The common land projection, run for real against a DuckDB source and read back out of
/// the SQLite it produces: a CommonLand row per 00-series CPH and a CommonLandHolding row per
/// keeper holding / common link, which is the join the KRDS sites endpoint walks in both
/// directions.
///
/// The extract's real column list is unconfirmed, which is why half of these tests are about what
/// the projection does without it - sentinels, absent columns, absent tables.</summary>
public sealed class CommonLandTests : IDisposable
{
    private readonly string _workingDirectory =
        Directory.CreateTempSubdirectory("common-land-tests-").FullName;

    private readonly string _sourcePath;
    private readonly Lazy<Task<string>> _export;

    private static readonly DateTimeOffset QueryDate = new(2026, 9, 15, 7, 0, 3, TimeSpan.Zero);

    public CommonLandTests()
    {
        _sourcePath = Path.Combine(_workingDirectory, "staging.duckdb");
        SamExtractFixture.Create(_sourcePath);

        // One export serves every assertion: the transformation is deterministic, and running it
        // per test would multiply a second of DuckDB work by the size of the class.
        _export = new Lazy<Task<string>>(ExportAsync);
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_workingDirectory, recursive: true); } catch (IOException) { }
    }

    private async Task<string> ExportAsync()
    {
        var target = Path.Combine(_workingDirectory, "krds-db.sqlite");

        await new DuckDbSqliteViewWriter(
            Options.Create(new DuckDbConfiguration { SqliteExtensionPath = DuckDbSqliteExtension.Path }),
            NullLogger<DuckDbSqliteViewWriter>.Instance)
            .WriteAsync(new SqliteViewWriteRequest(
                _sourcePath, target, SqliteViewDefinition.Parts, QueryDate));

        return target;
    }

    [Fact]
    public async Task Produces_one_row_per_holding_to_common_link()
    {
        var target = await _export.Value;

        Strings(target, "SELECT MainCph || '|' || CommonCph FROM CommonLandHolding ORDER BY MainCph, CommonCph")
            .Should().Equal(
                "17/050/0003|00/000/8267",
                "17/050/0003|00/000/8268",
                "17/050/0004|00/000/8267",
                "17/050/0005|00/000/8268",
                "18/060/0001|00/000/8269",
                "19/070/0001|00/000/8270");
    }

    [Fact]
    public async Task Drops_a_link_whose_main_or_common_cph_is_missing_or_a_sentinel()
    {
        var target = await _export.Value;

        Scalar(target, "SELECT count(*) FROM CommonLandHolding")
            .Should().Be(CommonLandExtractFixture.ExpectedLinkCount);

        Scalar(target, "SELECT count(*) FROM CommonLandHolding WHERE MainCph LIKE '20/%'")
            .Should().Be(0L, "a link whose common is absent or a sentinel goes nowhere");
    }

    [Fact]
    public async Task Produces_a_site_row_for_every_common_including_one_with_no_keeper()
    {
        var target = await _export.Value;

        Strings(target, "SELECT CommonCph FROM CommonLand ORDER BY CommonCph")
            .Should().Equal(
                "00/000/8267",
                "00/000/8268",
                "00/000/8269",
                "00/000/8270",
                "00/000/8271");

        Strings(target, "SELECT AddressLine1 FROM CommonLand WHERE CommonCph = '00/000/8271'")
            .Should().Equal(["LONELY COMMON"],
                "a common with no current keeper is still a site - it is only the link that needs both keys");
    }

    [Fact]
    public async Task Links_point_at_the_site_they_belong_to()
    {
        var target = await _export.Value;

        Scalar(target,
                "SELECT count(*) FROM CommonLandHolding h " +
                "WHERE NOT EXISTS (SELECT 1 FROM CommonLand c WHERE c.Id = h.CommonLandId)")
            .Should().Be(0L, "CommonLandId is derived the same way as the site's Id, so it always resolves");

        Strings(target,
                "SELECT c.AddressLine1 FROM CommonLandHolding h JOIN CommonLand c ON c.Id = h.CommonLandId " +
                "WHERE h.LinkId = 'L2'")
            .Should().Equal(["Black Mountain Common"]);
    }

    [Fact]
    public async Task Resolves_the_commons_attributes_from_the_most_recent_link_row()
        // 8268 is described by two link rows; the 2023 row's 'GRETEL MARSH NORTH' beats the 2022
        // row's 'GRETEL MARSH', and its coordinates come with it.
        => Strings(await _export.Value,
                "SELECT AddressLine1 || '|' || Easting FROM CommonLand WHERE CommonCph = '00/000/8268'")
            .Should().Equal(["GRETEL MARSH NORTH|320001"]);

    [Fact]
    public async Task Carries_the_commons_own_attributes_on_the_site_row()
    {
        var target = await _export.Value;

        // The most recent 8267 row (L2, 2021) wins every attribute it carries; the premise id
        // follows it too, and 'Common Land' is what the feed calls the site's usage.
        Strings(target, "SELECT PremiseId || '|' || BusinessUsage || '|' || AddressLine1 || '|' || " +
                        "LocalAuthorityName || '|' || Country || '|' || Postcode " +
                        "FROM CommonLand WHERE CommonCph = '00/000/8267'")
            .Should().Equal(["546200|Common Land|Black Mountain Common|POWYS COUNTY COUNCIL|Wales|LD1 1AA"]);
    }

    [Fact]
    public async Task Stores_the_coordinates_as_integers()
        // '320000.0' is what a DOUBLE staging column renders as; the cast goes via DOUBLE and lands
        // the integer, exactly as the Holding address numbers do.
        => Strings(await _export.Value,
                "SELECT Easting || '|' || typeof(Easting) FROM CommonLand WHERE CommonCph = '00/000/8268'")
            .Should().Equal(["320001|integer"]);

    [Fact]
    public async Task Trims_the_cph_keys_the_extract_pads()
    {
        var target = await _export.Value;

        Strings(target, "SELECT MainCph || '|' || CommonCph FROM CommonLandHolding WHERE LinkId = 'L5'")
            .Should().Equal(["19/070/0001|00/000/8270"]);

        Strings(target, "SELECT CommonCph FROM CommonLand WHERE CommonCph = '00/000/8270'")
            .Should().Equal(["00/000/8270"], "the site keys off the trimmed value too");
    }

    [Theory]
    // 'Yes' and 'No' are the spellings the feed uses.
    [InlineData("146780", 1)]
    [InlineData("L5", 1)]
    [InlineData("L2", 0)]
    [InlineData("L3", 0)]
    public async Task Stores_the_contiguity_flag_as_one_or_zero(string linkId, int expected)
        => Scalar(await _export.Value,
                $"SELECT IsContiguous FROM CommonLandHolding WHERE LinkId = '{linkId}'")
            .Should().Be(expected);

    [Fact]
    public async Task Holds_an_absent_flag_as_null_rather_than_guessing()
        => Scalar(await _export.Value,
                "SELECT count(*) FROM CommonLandHolding WHERE MainCph = '18/060/0001' AND IsContiguous IS NULL")
            .Should().Be(1L, "'-' means the extract does not say, not that the link is either kind");

    [Fact]
    public async Task Stores_dates_as_epoch_seconds()
    {
        var target = await _export.Value;

        // '40500' is the serial form of 2010-11-18 the review workbook produced; the feed's
        // formatted dates land the same way.
        Scalar(target, "SELECT StartDate FROM CommonLandHolding WHERE LinkId = '146780'")
            .Should().Be(new DateTimeOffset(2010, 11, 18, 0, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds());

        Scalar(target, "SELECT EndDate FROM CommonLandHolding WHERE LinkId = 'L2'")
            .Should().Be(new DateTimeOffset(2024, 12, 31, 0, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds());
    }

    [Fact]
    public async Task Reads_the_2999_end_date_as_no_end_at_all()
        // 31-DEC-2999 is AMLS's permanent-link sentinel - serial 401768 in the workbook. A live
        // link has a NULL end, not a year-3000 timestamp.
        => Scalar(await _export.Value,
                "SELECT count(*) FROM CommonLandHolding WHERE LinkId = '146780' AND EndDate IS NULL")
            .Should().Be(1L);

    [Fact]
    public async Task Stores_missing_value_sentinels_as_nulls()
    {
        var target = await _export.Value;

        Strings(target, "SELECT ifnull(IsContiguous,'<null>') || '|' || ifnull(StartDate,'<null>') || '|' || " +
                        "ifnull(EndDate,'<null>') || '|' || ifnull(LinkId,'<null>') " +
                        "FROM CommonLandHolding WHERE MainCph = '18/060/0001'")
            .Should().Equal(["<null>|<null>|<null>|<null>"],
                "'-', ',' and '' all mean absent; the link itself still stands on its keys");

        Strings(target, "SELECT ifnull(PremiseId,'<null>') || '|' || ifnull(BusinessUsage,'<null>') || '|' || " +
                        "ifnull(PremisesName,'<null>') || '|' || ifnull(LocalAuthorityName,'<null>') || '|' || " +
                        "ifnull(Easting,'<null>') FROM CommonLand WHERE CommonCph = '00/000/8269'")
            .Should().Equal(["<null>|<null>|<null>|<null>|<null>"],
                "the site row survives on its key even where every attribute is absent");
    }

    [Fact]
    public async Task Derives_stable_uuid_shaped_ids_in_both_tables()
    {
        var target = await _export.Value;

        Strings(target, "SELECT Id FROM CommonLandHolding")
            .Should().OnlyHaveUniqueItems()
            .And.AllSatisfy(id => id.Should().MatchRegex(
                "^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$",
                "every id is the name-derived, UUID-shaped guid the rest of the read model uses"));

        Strings(target, "SELECT Id FROM CommonLand")
            .Should().OnlyHaveUniqueItems()
            .And.AllSatisfy(id => id.Should().MatchRegex(
                "^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$"));

        // Site and link are namespaced separately: a site's id is not its first link's id.
        Scalar(target,
                "SELECT count(*) FROM CommonLand c JOIN CommonLandHolding h ON h.CommonLandId = c.Id " +
                "WHERE h.Id = c.Id")
            .Should().Be(0L);
    }

    [Fact]
    public async Task Runs_when_only_the_key_columns_arrived()
    {
        // The feed may carry fewer columns than the projection asks after. Every optional column is
        // supplied NULL, so the tables still produce, just narrower.
        var source = Path.Combine(_workingDirectory, "keys-only.duckdb");
        SamExtractFixture.Create(source);

        await using (var connection = new DuckDBConnection($"Data Source={source}"))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "DROP TABLE amls2_common_land";
            await command.ExecuteNonQueryAsync();
            CommonLandExtractFixture.CreateKeysOnly(connection);
        }

        var target = Path.Combine(_workingDirectory, "keys-only.sqlite");
        var result = await new DuckDbSqliteViewWriter(
            Options.Create(new DuckDbConfiguration { SqliteExtensionPath = DuckDbSqliteExtension.Path }),
            NullLogger<DuckDbSqliteViewWriter>.Instance)
            .WriteAsync(new SqliteViewWriteRequest(
                source, target, SqliteViewDefinition.Parts, QueryDate));

        result.Tables.Should().ContainEquivalentOf(new SqliteViewTable("CommonLandHolding", 2));
        result.Tables.Should().ContainEquivalentOf(new SqliteViewTable("CommonLand", 1));

        Scalar(target, "SELECT count(*) FROM CommonLandHolding WHERE IsContiguous IS NULL")
            .Should().Be(2L, "columns the feed does not carry land NULL rather than failing to bind");

        Scalar(target, "SELECT count(*) FROM CommonLand WHERE CommonCph = '00/000/8267' AND PremisesName IS NULL")
            .Should().Be(1L);
    }

    [Fact]
    public async Task Skips_when_the_staging_database_holds_no_amls2_table()
    {
        var source = Path.Combine(_workingDirectory, "no-amls2.duckdb");
        SamExtractFixture.Create(source, includeCommonLand: false);

        var target = Path.Combine(_workingDirectory, "no-amls2.sqlite");
        var result = await new DuckDbSqliteViewWriter(
            Options.Create(new DuckDbConfiguration { SqliteExtensionPath = DuckDbSqliteExtension.Path }),
            NullLogger<DuckDbSqliteViewWriter>.Instance)
            .WriteAsync(new SqliteViewWriteRequest(
                source, target, SqliteViewDefinition.Parts, QueryDate));

        result.Tables.Select(table => table.Name).Should()
            .NotContain("CommonLand").And.NotContain("CommonLandHolding");

        Scalar(target, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE 'CommonLand%'")
            .Should().Be(0L, "absent tables tell a reader the feed was never loaded; empty ones would not");

        Scalar(target, "SELECT count(*) FROM Holding").Should().Be(4L, "the rest of the read model is unaffected");
    }

    [Fact]
    public async Task Reports_its_row_counts_for_the_export_metadata()
    {
        await _export.Value;

        var result = await new DuckDbSqliteViewWriter(
            Options.Create(new DuckDbConfiguration { SqliteExtensionPath = DuckDbSqliteExtension.Path }),
            NullLogger<DuckDbSqliteViewWriter>.Instance)
            .WriteAsync(new SqliteViewWriteRequest(
                _sourcePath,
                Path.Combine(_workingDirectory, "counted.sqlite"),
                SqliteViewDefinition.Parts,
                QueryDate));

        result.Tables.Should().ContainEquivalentOf(
            new SqliteViewTable("CommonLand", CommonLandExtractFixture.ExpectedCommonCount));
        result.Tables.Should().ContainEquivalentOf(
            new SqliteViewTable("CommonLandHolding", CommonLandExtractFixture.ExpectedLinkCount));
    }

    [Fact]
    public async Task Indexes_the_columns_the_sites_endpoint_joins_by()
        => Strings(await _export.Value,
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'CommonLandHolding' " +
                "AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .Should().Equal(
                "ix_common_land_holding_common_cph",
                "ix_common_land_holding_common_land_id",
                "ix_common_land_holding_main_cph");

    [Fact]
    public async Task Leaves_the_SAM_read_model_alone()
        // The scripts share a database and nothing else. A collision would most likely show up as a
        // SAM table losing rows.
        => Scalar(await _export.Value, "SELECT count(*) FROM Holding").Should().Be(4);

    private static long Scalar(string databasePath, string sql)
    {
        using var connection = Open(databasePath);
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        return Convert.ToInt64(command.ExecuteScalar());
    }

    private static List<string> Strings(string databasePath, string sql)
    {
        using var connection = Open(databasePath);
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        var values = new List<string>();
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static SqliteConnection Open(string databasePath)
    {
        var connection = new SqliteConnection($"Data Source={databasePath};Mode=ReadOnly");
        connection.Open();

        return connection;
    }
}
