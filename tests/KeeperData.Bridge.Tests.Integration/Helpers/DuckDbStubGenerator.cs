using Bogus;
using DuckDB.NET.Data;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Generates a pre-seeded DuckDB file matching the Phase I staging output schema.
/// Schema contract: table sam_cph_holdings(CPH, FEATURE_NAME, SECONDARY_CPH, ANIMAL_SPECIES_CODE).
/// </summary>
public static class DuckDbStubGenerator
{
    private static readonly string[] AnimalSpeciesCodes =
        ["BO", "OV", "PO", "CP", "DE", "GO", "EQ", "BU", "PI"];

    private static readonly string[] FeatureNames =
    [
        "Main Holding", "Grazing Land", "Arable Field", "Livestock Unit",
        "Dairy Parlour", "Sheep Pen", "Poultry House", "Stable Block",
        "Common Land", "Temporary Holding", "Market Premises", "Slaughterhouse"
    ];

    /// <summary>
    /// Creates a DuckDB file at the given path with a seeded sam_cph_holdings table.
    /// </summary>
    /// <param name="outputPath">Absolute file path for the .duckdb file.</param>
    /// <param name="rowCount">Number of rows to generate (default 750).</param>
    /// <param name="seed">Bogus seed for reproducible output.</param>
    public static void Generate(string outputPath, int rowCount = 750, int seed = 42)
    {
        var dir = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        if (File.Exists(outputPath))
            File.Delete(outputPath);

        using var connection = new DuckDBConnection($"Data Source={outputPath}");
        connection.Open();

        CreateTable(connection);
        InsertRows(connection, rowCount, seed);
    }

    /// <summary>
    /// Builds a Phase I naming-convention path:
    /// {baseDir}/staging/keeper_data_bridge_{timestamp}.duckdb
    /// </summary>
    public static string BuildStagingPath(string baseDir, DateTime? timestamp = null)
    {
        var ts = timestamp ?? DateTime.UtcNow;
        var fileName = $"keeper_data_bridge_{ts:yyyyMMdd'T'HHmmss'Z'}.duckdb";
        return $"{baseDir.TrimEnd('/')}/staging/{fileName}";
    }

    private static void CreateTable(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE sam_cph_holdings (
                CPH                 VARCHAR,
                FEATURE_NAME        VARCHAR,
                SECONDARY_CPH       VARCHAR,
                ANIMAL_SPECIES_CODE VARCHAR
            )
            """;
        cmd.ExecuteNonQuery();
    }

    private static void InsertRows(DuckDBConnection connection, int rowCount, int seed)
    {
        var faker = new Faker { Random = new Randomizer(seed) };

        using var appender = connection.CreateAppender("sam_cph_holdings");

        var distinctCphCount = (int)(rowCount * 0.6);
        var cphPool = Enumerable.Range(0, distinctCphCount)
            .Select(_ => GenerateCph(faker))
            .ToList();

        for (var i = 0; i < rowCount; i++)
        {
            var row = BuildRow(faker, cphPool, i, rowCount);
            appender.CreateRow()
                .AppendValue(row.Cph)
                .AppendValue(row.FeatureName)
                .AppendValue(row.SecondaryCph)
                .AppendValue(row.AnimalSpeciesCode)
                .EndRow();
        }
    }

    private static (string? Cph, string? FeatureName, string? SecondaryCph, string? AnimalSpeciesCode)
        BuildRow(Faker faker, List<string> cphPool, int index, int totalRows)
    {
        // Reserve last ~2% of rows for null/empty edge cases
        var edgeCaseThreshold = totalRows - (int)(totalRows * 0.02);

        if (index >= edgeCaseThreshold)
        {
            return index % 2 == 0
                ? (null, faker.PickRandom(FeatureNames), null, faker.PickRandom(AnimalSpeciesCodes))
                : ("", faker.PickRandom(FeatureNames), "", faker.PickRandom(AnimalSpeciesCodes));
        }

        var cph = faker.PickRandom(cphPool);
        var featureName = faker.PickRandom(FeatureNames);
        var secondaryCph = faker.Random.Bool(0.3f) ? GenerateCph(faker) : null;
        var speciesCode = faker.PickRandom(AnimalSpeciesCodes);

        return (cph, featureName, secondaryCph, speciesCode);
    }

    /// <summary>
    /// Generates a realistic CPH in NN/NNN/NNNN format.
    /// </summary>
    private static string GenerateCph(Faker faker)
    {
        var county = faker.Random.Int(1, 99);
        var parish = faker.Random.Int(1, 999);
        var holding = faker.Random.Int(1, 9999);
        return $"{county:D2}/{parish:D3}/{holding:D4}";
    }
}
