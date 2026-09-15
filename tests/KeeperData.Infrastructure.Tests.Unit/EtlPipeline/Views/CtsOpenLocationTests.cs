using FluentAssertions;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Infrastructure.EtlPipeline.Views;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>The Open Locations projection, run for real against a DuckDB source and read back out
/// of the SQLite it produces.
///
/// The rule these assert is specified in open-locations-baseline-spec.md, which was derived from a
/// real run of the Oracle report rather than from the code. Each test names the section it covers,
/// so a deliberate change to the rule can be told apart from a regression.</summary>
public sealed class CtsOpenLocationTests : IDisposable
{
    private readonly string _workingDirectory =
        Directory.CreateTempSubdirectory("cts-open-location-tests-").FullName;

    private readonly string _sourcePath;
    private readonly Lazy<Task<string>> _export;

    /// <summary>The as-at date the fixture is built around. Fixture rows sit either side of it.</summary>
    private static readonly DateTimeOffset QueryDate = new(2026, 9, 15, 7, 0, 3, TimeSpan.Zero);

    public CtsOpenLocationTests()
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
                _sourcePath, target, SqliteViewDefinition.Sql, SqliteViewDefinition.TableNames, QueryDate));

        return target;
    }

    [Fact]
    public async Task Includes_every_open_holding_and_nothing_else()
    {
        var locations = await LocationNumbersAsync();

        locations.Should().Equal(
            "AH-08/001/0004",
            "AH-08/001/0005",
            "AH-10/001/0001",
            "AH-10/001/0002",
            "AH-10/001/0003-01",
            "AH-10/001/0006",
            "AH-10/001/0007",
            "AH-10/001/0008",
            "SH-4001");
    }

    [Theory]
    // Section 4.1: open.
    [InlineData("AH-10/002/0020", "the receive-labels flag is off")]
    [InlineData("AH-10/002/0021", "the location closed before the query date")]
    [InlineData("AH-10/002/0022", "the location is cancelled")]
    [InlineData("AH-10/002/0023", "the location is not yet in effect")]
    // Section 4.3: a valid keeper link.
    [InlineData("AH-10/002/0024", "the only party link is a correspondence one")]
    [InlineData("AH-10/002/0029", "the keeper link ended before the query date")]
    [InlineData("AH-10/002/0030", "the keeper link is cancelled")]
    // Sections 4.4 and 4.5: admissible, and in the country asked for.
    [InlineData("AH-52/002/0025", "the holding is Welsh")]
    [InlineData("AH-66/002/0026", "the holding is Scottish")]
    [InlineData("AH-99/002/0027", "county 99 is BCMS's dummy")]
    [InlineData("AH-08/205/8000", "it is BCMS's own holding")]
    [InlineData("SH-9999", "it is BCMS's other record")]
    [InlineData("SH-4002", "a county-less holding with a Welsh postcode is Welsh")]
    [InlineData("SH-4003", "a county-less holding with a Scottish postcode is Scottish")]
    public async Task Excludes_a_holding_when(string locationNumber, string because)
        => (await LocationNumbersAsync()).Should().NotContain(locationNumber, because);

    [Fact]
    public async Task Keeps_a_superseded_identifier_from_duplicating_its_holding()
    {
        var locations = await LocationNumbersAsync();

        locations.Should().Contain("AH-08/001/0005", "it is the location's current identifier")
            .And.NotContain("AH-08/999/9999", "that identifier has been superseded");

        locations.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task Takes_the_most_recent_of_several_concurrent_keeper_links()
        // Three current KN links: 1996, 2010 and 2020. Section 5.2 takes the latest effective-from,
        // which agrees with the real report on 41,349 of 41,350 rows.
        => (await FieldAsync("AH-10/001/0002", "KeeperSurname")).Should().Be("CARTER");

    [Fact]
    public async Task Reads_a_two_digit_year_the_way_Oracle_does()
        // Effective from 22-JUN-66. Oracle's RR pivot reads 1966; DuckDB's %y reads 2066, which
        // would put the holding in the future and drop it. Section 7.6.
        => (await LocationNumbersAsync()).Should().Contain("AH-08/001/0004");

    [Fact]
    public async Task Derives_Cph_from_the_identifier_without_its_scheme_prefix()
    {
        (await FieldAsync("AH-10/001/0001", "Cph")).Should().Be("10/001/0001");

        (await FieldAsync("AH-10/001/0003-01", "Cph")).Should().Be("10/001/0003",
            "a sub-location joins on its parent's CPH, not its own identifier");

        (await FieldAsync("SH-4001", "Cph")).Should().Be("4001",
            "a separate holding's identifier is a bare number");
    }

    [Fact]
    public async Task Keeps_a_holding_whose_premises_type_is_outside_the_farm_types()
        // spproc112 filters to AH, LK and TH. The real report does not - it carries SG, CA, MA and
        // EX holdings, and rows with no premises type at all. Section 4.6.
        => (await LocationNumbersAsync()).Should().Contain("AH-10/001/0008");

    [Fact]
    public async Task Keeps_a_county_less_holding_with_an_English_postcode()
        // The report's 112 SH- rows have no county row at all. An inner join on CT_COUNTIES loses
        // every one of them. Sections 4.4 and 7.1.
        => (await LocationNumbersAsync()).Should().Contain("SH-4001");

    [Fact]
    public async Task Keeps_a_keeper_who_has_no_current_address()
    {
        // spproc112 and PS1030 both require one; the report does not, and imposing it drops 446
        // holdings the report contains. Section 7.2.
        (await LocationNumbersAsync()).Should().Contain("AH-10/001/0007");

        (await FieldAsync("AH-10/001/0007", "KeeperSurname")).Should().Be("KNIGHT");
        (await FieldAsync("AH-10/001/0007", "KeeperPostCode")).Should().BeNull();
    }

    [Fact]
    public async Task Fills_the_correspondence_and_contact_blocks_when_those_links_exist()
    {
        (await FieldAsync("AH-10/001/0006", "KeeperSurname")).Should().Be("HAYES");
        (await FieldAsync("AH-10/001/0006", "CorrespondenceSurname")).Should().Be("IRVING");
        (await FieldAsync("AH-10/001/0006", "CorrespondencePostCode")).Should().Be("EX5 5EE");
        (await FieldAsync("AH-10/001/0006", "ContactSurname")).Should().Be("JARVIS");
        (await FieldAsync("AH-10/001/0006", "ContactEmailAddress")).Should().Be("jarvis@example.test");
    }

    [Fact]
    public async Task Leaves_the_correspondence_and_contact_blocks_empty_when_there_are_no_such_links()
    {
        (await FieldAsync("AH-10/001/0001", "CorrespondenceSurname")).Should().BeNull();
        (await FieldAsync("AH-10/001/0001", "ContactSurname")).Should().BeNull();
    }

    [Fact]
    public async Task Takes_the_location_s_own_contact_details_from_the_location_not_the_keeper()
    {
        // The keeper's telephone and email come from CT_PARTIES, the location's from CT_LOCATIONS.
        // Different tables, and in production they frequently differ. Section 5.6.
        (await FieldAsync("AH-10/001/0001", "KeeperTelNumber")).Should().Be("01392 111111");
        (await FieldAsync("AH-10/001/0001", "LocationTelNumber")).Should().Be("01392 000001");
        (await FieldAsync("AH-10/001/0001", "LocationEmailAddress")).Should().Be("farm1@example.test");
        (await FieldAsync("AH-10/001/0001", "LocationAddressName")).Should().Be("LOCATION ONE");
    }

    [Fact]
    public async Task Carries_the_county_and_premises_type_alongside_the_report_s_own_columns()
    {
        (await FieldAsync("AH-10/001/0001", "CountyCode")).Should().Be("10");
        (await FieldAsync("AH-10/001/0001", "CountyName")).Should().Be("Devon");
        (await FieldAsync("AH-10/001/0001", "PremisesType")).Should().Be("AH");
        (await FieldAsync("AH-10/001/0001", "LocId")).Should().Be("1");
    }

    [Fact]
    public async Task Holds_an_absent_value_as_null_rather_than_the_empty_string_the_extract_uses()
        // ELLIS carries '' for every contact column, as the extract renders an Oracle NULL.
        => (await FieldAsync("AH-10/001/0003-01", "KeeperEmailAddress")).Should().BeNull();

    [Fact]
    public async Task Answers_as_at_the_query_date_rather_than_today()
    {
        // The keeper link ending 31-DEC-24 and the location starting 30-JUL-30 both sit outside the
        // query date, and would be judged differently on a clock-driven run.
        var target = Path.Combine(_workingDirectory, "as-at-2024.sqlite");

        await new DuckDbSqliteViewWriter(
            Options.Create(new DuckDbConfiguration { SqliteExtensionPath = DuckDbSqliteExtension.Path }),
            NullLogger<DuckDbSqliteViewWriter>.Instance)
            .WriteAsync(new SqliteViewWriteRequest(
                _sourcePath, target, SqliteViewDefinition.Sql, SqliteViewDefinition.TableNames,
                new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero)));

        Strings(target, "SELECT LocationNumber FROM CtsOpenLocation ORDER BY LocationNumber")
            .Should().Contain("AH-10/002/0029",
                "its keeper link was still open on 01-JAN-2024, though it has since ended");
    }

    [Fact]
    public async Task Reports_its_row_count_for_the_export_metadata()
    {
        await _export.Value;

        var result = await new DuckDbSqliteViewWriter(
            Options.Create(new DuckDbConfiguration { SqliteExtensionPath = DuckDbSqliteExtension.Path }),
            NullLogger<DuckDbSqliteViewWriter>.Instance)
            .WriteAsync(new SqliteViewWriteRequest(
                _sourcePath,
                Path.Combine(_workingDirectory, "counted.sqlite"),
                SqliteViewDefinition.Sql,
                SqliteViewDefinition.TableNames,
                QueryDate));

        result.Tables.Should().ContainEquivalentOf(new SqliteViewTable("CtsOpenLocation", 9));
    }

    [Fact]
    public async Task Indexes_the_columns_the_read_model_is_queried_by()
        => Strings(await _export.Value,
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'CtsOpenLocation' " +
                "AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .Should().Equal(
                "ix_cts_open_location_cph",
                "ix_cts_open_location_keeper_email",
                "ix_cts_open_location_keeper_post_code");

    [Fact]
    public async Task Leaves_the_SAM_read_model_alone()
        // The two scripts share a database and nothing else. A collision between them would most
        // likely show up as a SAM table losing rows.
        => Scalar(await _export.Value, "SELECT count(*) FROM Holding").Should().Be(4);

    private async Task<List<string>> LocationNumbersAsync()
        => Strings(await _export.Value, "SELECT LocationNumber FROM CtsOpenLocation ORDER BY LocationNumber");

    private async Task<string?> FieldAsync(string locationNumber, string column)
    {
        using var connection = Open(await _export.Value);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {column} FROM CtsOpenLocation WHERE LocationNumber = $id";
        command.Parameters.AddWithValue("$id", locationNumber);

        var value = command.ExecuteScalar();

        return value is null or DBNull ? null : value.ToString();
    }

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
