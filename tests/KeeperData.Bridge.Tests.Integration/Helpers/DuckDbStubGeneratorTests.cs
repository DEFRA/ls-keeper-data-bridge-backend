using DuckDB.NET.Data;
using FluentAssertions;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

public class DuckDbStubGeneratorTests : IDisposable
{
    private static readonly string[] ExpectedColumns =
    [
        "BATCH_ID", "CHANGE_TYPE", "CPH", "FEATURE_NAME", "CPH_TYPE",
        "ADDRESS_PK", "SAON_START_NUMBER", "SAON_START_NUMBER_SUFFIX",
        "SAON_END_NUMBER", "SAON_END_NUMBER_SUFFIX", "PAON_START_NUMBER",
        "PAON_START_NUMBER_SUFFIX", "PAON_END_NUMBER", "PAON_END_NUMBER_SUFFIX",
        "STREET", "TOWN", "LOCALITY", "UK_INTERNAL_CODE", "POSTCODE",
        "COUNTRY_CODE", "UDPRN", "EASTING", "NORTHING", "OS_MAP_REFERENCE",
        "DISEASE_TYPE", "INTERVAL", "INTERVAL_UNIT_OF_TIME",
        "FEATURE_ADDRESS_FROM_DATE", "FEATURE_ADDRESS_TO_DATE",
        "CPH_RELATIONSHIP_TYPE", "SECONDARY_CPH", "FACILITY_BUSINSS_ACTVTY_CODE",
        "FACILITY_TYPE_CODE", "FCLTY_SUB_BSNSS_ACTVTY_CODE",
        "FEATURE_STATUS_CODE", "MOVEMENT_RSTRCTN_RSN_CODE",
        "ANIMAL_SPECIES_CODE", "ANIMAL_PRODUCTION_USAGE_CODE"
    ];

    private readonly string _tempDir;

    public DuckDbStubGeneratorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"duckdb_stub_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);

        GC.SuppressFinalize(this);
    }

    [Fact]
    public void Generate_CreatesQueryableDuckDbFile()
    {
        var path = Path.Combine(_tempDir, "test.duckdb");

        DuckDbStubGenerator.Generate(path, rowCount: 750);

        File.Exists(path).Should().BeTrue();

        using var conn = new DuckDBConnection($"Data Source={path}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sam_cph_holdings";
        var count = (long)cmd.ExecuteScalar()!;
        count.Should().Be(750);
    }

    [Fact]
    public void Generate_ProducesDistinctCphs()
    {
        var path = Path.Combine(_tempDir, "test.duckdb");

        DuckDbStubGenerator.Generate(path, rowCount: 750);

        using var conn = new DuckDBConnection($"Data Source={path}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(DISTINCT CPH) FROM sam_cph_holdings WHERE CPH IS NOT NULL AND CPH <> ''";
        var distinctCount = (long)cmd.ExecuteScalar()!;

        distinctCount.Should().BeGreaterThan(100, "should have many distinct CPH values");
    }

    [Fact]
    public void Generate_IncludesNullAndEmptyCphEdgeCases()
    {
        var path = Path.Combine(_tempDir, "test.duckdb");

        DuckDbStubGenerator.Generate(path, rowCount: 750);

        using var conn = new DuckDBConnection($"Data Source={path}");
        conn.Open();

        using var nullCmd = conn.CreateCommand();
        nullCmd.CommandText = "SELECT COUNT(*) FROM sam_cph_holdings WHERE CPH IS NULL";
        var nullCount = (long)nullCmd.ExecuteScalar()!;
        nullCount.Should().BeGreaterThan(0, "should include null CPH edge cases");

        using var emptyCmd = conn.CreateCommand();
        emptyCmd.CommandText = "SELECT COUNT(*) FROM sam_cph_holdings WHERE CPH = ''";
        var emptyCount = (long)emptyCmd.ExecuteScalar()!;
        emptyCount.Should().BeGreaterThan(0, "should include empty CPH edge cases");
    }

    [Fact]
    public void Generate_IncludesDuplicateCphs()
    {
        var path = Path.Combine(_tempDir, "test.duckdb");

        DuckDbStubGenerator.Generate(path, rowCount: 750);

        using var conn = new DuckDBConnection($"Data Source={path}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM (
                SELECT CPH FROM sam_cph_holdings
                WHERE CPH IS NOT NULL AND CPH <> ''
                GROUP BY CPH HAVING COUNT(*) > 1
            )
            """;
        var duplicateGroups = (long)cmd.ExecuteScalar()!;
        duplicateGroups.Should().BeGreaterThan(0, "should include duplicate CPH values");
    }

    [Fact]
    public void Generate_HasVariedAnimalSpeciesCodes()
    {
        var path = Path.Combine(_tempDir, "test.duckdb");

        DuckDbStubGenerator.Generate(path, rowCount: 750);

        using var conn = new DuckDBConnection($"Data Source={path}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(DISTINCT ANIMAL_SPECIES_CODE) FROM sam_cph_holdings WHERE ANIMAL_SPECIES_CODE IS NOT NULL";
        var speciesCount = (long)cmd.ExecuteScalar()!;
        speciesCount.Should().BeGreaterThanOrEqualTo(5, "should have varied species codes");
    }

    [Fact]
    public void Generate_HasCorrectSchema()
    {
        var path = Path.Combine(_tempDir, "test.duckdb");

        DuckDbStubGenerator.Generate(path, rowCount: 100);

        using var conn = new DuckDBConnection($"Data Source={path}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'sam_cph_holdings' ORDER BY ordinal_position";
        using var reader = cmd.ExecuteReader();

        var columns = new List<(string Name, string Type)>();
        while (reader.Read())
            columns.Add((reader.GetString(0), reader.GetString(1)));

        columns.Should().HaveCount(38);
        columns.Select(c => c.Name).Should().BeEquivalentTo(ExpectedColumns, options => options.WithStrictOrdering());
        columns.Should().OnlyContain(c => c.Type == "VARCHAR");
    }

    [Fact]
    public void Generate_IsReproducibleWithSameSeed()
    {
        var path1 = Path.Combine(_tempDir, "test1.duckdb");
        var path2 = Path.Combine(_tempDir, "test2.duckdb");

        DuckDbStubGenerator.Generate(path1, rowCount: 100, seed: 42);
        DuckDbStubGenerator.Generate(path2, rowCount: 100, seed: 42);

        var rows1 = ReadAllRows(path1);
        var rows2 = ReadAllRows(path2);

        rows1.Should().BeEquivalentTo(rows2, options => options.WithStrictOrdering());
    }

    [Fact]
    public void BuildStagingPath_FollowsPhaseINamingConvention()
    {
        var ts = new DateTime(2026, 6, 23, 12, 0, 0, DateTimeKind.Utc);
        var path = DuckDbStubGenerator.BuildStagingPath("/data", ts);

        path.Should().Be("/data/staging/keeper_data_bridge_20260623T120000Z.duckdb");
    }

    [Fact]
    public void Generate_CphFormatMatchesPattern()
    {
        var path = Path.Combine(_tempDir, "test.duckdb");

        DuckDbStubGenerator.Generate(path, rowCount: 200);

        using var conn = new DuckDBConnection($"Data Source={path}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sam_cph_holdings
            WHERE CPH IS NOT NULL AND CPH <> ''
              AND CPH NOT SIMILAR TO '[0-9]{2}/[0-9]{3}/[0-9]{4}'
            """;
        var invalidCount = (long)cmd.ExecuteScalar()!;
        invalidCount.Should().Be(0, "all non-null/non-empty CPH values should match NN/NNN/NNNN format");
    }

    private static List<string> ReadAllRows(string dbPath)
    {
        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM sam_cph_holdings";
        using var reader = cmd.ExecuteReader();

        var rows = new List<string>();
        while (reader.Read())
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
                values[i] = reader.IsDBNull(i) ? "NULL" : reader.GetString(i);
            rows.Add(string.Join("|", values));
        }

        return rows;
    }
}
