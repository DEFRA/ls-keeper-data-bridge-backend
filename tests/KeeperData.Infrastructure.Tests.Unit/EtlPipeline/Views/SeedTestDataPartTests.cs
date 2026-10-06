using FluentAssertions;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Core.EtlPipeline.Views.TestData;
using KeeperData.Infrastructure.EtlPipeline.Views;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>The seeded keepers, run for real on top of the read model they are written into. The
/// fixture's 01/234/5678 is what a persona colliding with the extracts looks like.</summary>
public sealed class SeedTestDataPartTests : IDisposable
{
    private readonly string _workingDirectory =
        Directory.CreateTempSubdirectory("krds-seed-tests-").FullName;

    private readonly string _sourcePath;

    private static readonly DateTimeOffset QueryDate = new(2026, 9, 15, 7, 0, 3, TimeSpan.Zero);

    public SeedTestDataPartTests()
    {
        _sourcePath = Path.Combine(_workingDirectory, "staging.duckdb");
        SamExtractFixture.Create(_sourcePath);
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_workingDirectory, recursive: true); } catch (IOException) { }
    }

    /// <summary>The acceptance criterion: the query a caller runs against the read model.</summary>
    [Fact]
    public async Task Finds_the_seeded_keeper_by_email_and_role()
    {
        var target = await SeedAsync(SeedTestData.Personas);

        Strings(target, """
            SELECT h.Cph
            FROM Holding h
            INNER JOIN PartyRole r ON r.HoldingId = h.Id
            INNER JOIN Party p ON p.Id = r.PartyId
            WHERE p.Email = 'defralivestock+single-holding@gmail.com' AND r.Role = 'holder'
            """)
            .Should().Equal(["03/202/0021"]);
    }

    [Fact]
    public async Task Writes_everything_the_persona_describes()
    {
        var target = await SeedAsync(SeedTestData.Personas);

        Strings(target, """
            SELECT p.PersonTitle || '|' || p.GivenName || '|' || p.FamilyName || '|' || p.Telephone || '|' ||
                   p.Mobile || '|' || p.AddressLine1 || '|' || p.AddressPostcode
            FROM Party p WHERE p.SourcePartyId = 'TEST-SINGLE-HOLDING'
            """)
            .Should().Equal(["MR|Joe|Bloggs|01632 960 890|07700 900123|1 Old Farm|LS1 1AA"]);

        Strings(target, """
            SELECT h.FeatureName || '|' || h.CphType || '|' || h.Town || '|' || h.OsMapReference || '|' || typeof(h.Easting)
            FROM Holding h WHERE h.Cph = '03/202/0021'
            """)
            .Should().Equal(["Old Farm|permanent|Leeds|TA6000060000|integer"]);

        Strings(target, """
            SELECT d.Herdmark || '|' || d.Cphh || '|' || d.AnimalSpeciesCode || '|' || d.AnimalPurposeCode
            FROM Herd d JOIN Holding h ON h.Id = d.HoldingId WHERE h.Cph = '03/202/0021'
            """)
            .Should().Equal(["930021|03/202/0021/01|CTT|CTT-BEEF-SCK"]);

        Strings(target, """
            SELECT a.AnimalSpeciesCode || '|' || a.AnimalProductionUsageCode || '|' || a.DiseaseType
            FROM HoldingAnimalProfile a JOIN Holding h ON h.Id = a.HoldingId WHERE h.Cph = '03/202/0021'
            """)
            .Should().Equal(["CTT|CTT-BEEF|TB"]);
    }

    /// <summary>A herd-level role is keyed on the herd's id, so a role whose id was derived any other
    /// way would still insert but would not be the row the read model would have written.</summary>
    [Fact]
    public async Task Resolves_every_reference_the_seeded_rows_make()
    {
        var target = await SeedAsync(SeedTestData.Personas);

        Strings(target, """
            SELECT r.Role || '|' || ifnull(d.Herdmark, '<holding>')
            FROM PartyRole r
            JOIN Party p ON p.Id = r.PartyId
            JOIN Holding h ON h.Id = r.HoldingId
            LEFT JOIN Herd d ON d.Id = r.HerdId
            WHERE p.SourcePartyId = 'TEST-SINGLE-HOLDING'
            ORDER BY r.Role
            """)
            .Should().Equal("holder|<holding>", "keeper|930021", "owner|930021");

        Scalar(target, """
            SELECT count(*) FROM PartyRole WHERE HerdId IS NOT NULL
              AND HerdId NOT IN (SELECT Id FROM Herd)
            """).Should().Be(0L);
    }

    [Fact]
    public async Task Takes_a_holding_the_extract_also_carries_away_from_it()
    {
        var target = await SeedAsync([Persona("TEST-COLLIDES", "01/234/5678", "AB1234")]);

        Strings(target, "SELECT FeatureName FROM Holding WHERE Cph='01/234/5678'")
            .Should().Equal(["Seeded Farm"], "the persona replaces the holding the extract supplied");

        Scalar(target, "SELECT count(*) FROM Holding WHERE Cph='01/234/5678'")
            .Should().Be(1L, "Cph is unique, so a seeded holding has to land on the row rather than beside it");

        Strings(target, """
            SELECT p.SourcePartyId || '|' || r.Role
            FROM PartyRole r
            JOIN Party p ON p.Id = r.PartyId
            JOIN Holding h ON h.Id = r.HoldingId
            WHERE h.Cph = '01/234/5678'
            ORDER BY 1
            """)
            .Should().Equal(["TEST-COLLIDES|holder"], "the real roles on a claimed holding are cleared");

        Scalar(target, """
            SELECT count(*) FROM Herd d JOIN Holding h ON h.Id = d.HoldingId WHERE h.Cph = '01/234/5678'
            """).Should().Be(0L, "the persona declares no herds, so the extract's must not survive");

        Scalar(target, """
            SELECT count(*) FROM HoldingAnimalProfile a JOIN Holding h ON h.Id = a.HoldingId
            WHERE h.Cph = '01/234/5678'
            """).Should().Be(0L);
    }

    [Fact]
    public async Task Leaves_the_holdings_no_persona_claims_alone()
    {
        var seeded = await SeedAsync([Persona("TEST-COLLIDES", "01/234/5678", "AB1234")]);
        var unseeded = await RunAsync("unseeded.sqlite", SqliteViewDefinition.Parts);

        const string claimed = "(SELECT Id FROM Holding WHERE Cph = '01/234/5678')";

        Strings(seeded, $"SELECT Cph FROM Holding WHERE Id NOT IN {claimed} ORDER BY Cph")
            .Should().Equal(Strings(unseeded, $"SELECT Cph FROM Holding WHERE Id NOT IN {claimed} ORDER BY Cph"));

        foreach (var table in new[] { "Herd", "HoldingAnimalProfile", "PartyRole" })
        {
            var sql = $"SELECT Id FROM {table} WHERE HoldingId NOT IN {claimed} ORDER BY Id";

            Strings(seeded, sql).Should().Equal(Strings(unseeded, sql),
                "{0} rows belonging to an unclaimed holding are left as the extract produced them", table);
        }
    }

    [Fact]
    public async Task Writes_a_value_carrying_a_quote_as_the_value_it_is()
    {
        var persona = Persona("TEST-QUOTED", "05/678/9012", "QU1234") with
        {
            Party = Persona("TEST-QUOTED", "05/678/9012", "QU1234").Party with { FamilyName = "O'Brien" }
        };

        var target = await SeedAsync([persona]);

        Strings(target, "SELECT FamilyName FROM Party WHERE SourcePartyId='TEST-QUOTED'")
            .Should().Equal(["O'Brien"]);
    }

    /// <summary>The counts are the export's reconciliation figures, so a table two parts wrote to has
    /// to be reported once, holding what it finally holds.</summary>
    [Fact]
    public async Task Counts_each_table_once_with_the_rows_it_ends_up_with()
    {
        var target = Path.Combine(_workingDirectory, "counted.sqlite");

        var result = await Sut().WriteAsync(new SqliteViewWriteRequest(
            _sourcePath, target, Parts(SeedTestData.Personas), QueryDate));

        result.Tables.Select(table => table.Name).Should().OnlyHaveUniqueItems();

        foreach (var table in result.Tables)
        {
            Scalar(target, $"SELECT count(*) FROM {table.Name}").Should().Be(
                table.RowCount, "{0}'s reported count must be what the database holds", table.Name);
        }

        result.Tables.Single(table => table.Name == "Party").RowCount
            .Should().Be(7, "the six parties the extract names, plus the seeded keeper");
    }

    [Fact]
    public async Task Identifies_a_seeded_transformation_as_a_different_one()
        => SqliteViewDefinition.VersionOf(Parts(SeedTestData.Personas))
            .Should().NotBe(SqliteViewDefinition.Version,
                "an export already built without the test data has to be rebuilt with it");

    private static TestPersona Persona(string sourcePartyId, string cph, string herdmark) => new()
    {
        Name = sourcePartyId,
        Party = new TestParty
        {
            SourcePartyId = sourcePartyId,
            Email = $"{sourcePartyId.ToLowerInvariant()}@example.test",
            GivenName = "Test",
            FamilyName = "Keeper"
        },
        Holdings =
        [
            new TestHolding
            {
                Cph = cph,
                FeatureName = "Seeded Farm",
                Roles = [new TestPartyRole("holder")]
            }
        ]
    };

    private static IReadOnlyList<SqliteViewPart> Parts(IReadOnlyList<TestPersona> personas)
        => [.. SqliteViewDefinition.Parts, SeedTestDataPart.Create(personas)];

    private Task<string> SeedAsync(IReadOnlyList<TestPersona> personas)
        => RunAsync($"seeded-{Guid.NewGuid():N}.sqlite", Parts(personas));

    private async Task<string> RunAsync(string name, IReadOnlyList<SqliteViewPart> parts)
    {
        var target = Path.Combine(_workingDirectory, name);

        await Sut().WriteAsync(new SqliteViewWriteRequest(_sourcePath, target, parts, QueryDate));

        return target;
    }

    private static DuckDbSqliteViewWriter Sut() => new(
        Options.Create(new DuckDbConfiguration { SqliteExtensionPath = DuckDbSqliteExtension.Path }),
        NullLogger<DuckDbSqliteViewWriter>.Instance);

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
