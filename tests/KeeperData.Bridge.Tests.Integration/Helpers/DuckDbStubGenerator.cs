using Bogus;
using DuckDB.NET.Data;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Generates a pre-seeded DuckDB file matching the Phase I staging output schema.
/// Schema contract: table sam_cph_holdings with 38 VARCHAR columns matching the source PSV.
/// </summary>
public static class DuckDbStubGenerator
{
    private static readonly string[] AnimalSpeciesCodes =
        ["CTT", "CTT1", "SH", "PG", "GT", "DE", "EQ", "BO", "OV"];

    private static readonly string[] AnimalProductionUsageCodes =
        ["MEAT", "DAIRY", "BREEDING", "WOOL", "EGGS", ""];

    private static readonly string[] ChangeTypes = ["I", "U", "D"];

    private static readonly string[] CphTypes = ["MAIN", "TEMPORARY", "MARKET"];

    private static readonly string[] CphRelationshipTypes = ["MAIN", "SECONDARY", "ASSOCIATED"];

    private static readonly string[] FacilityBusinessActivityCodes =
        ["AG-MARP", "AG-SHO", "AG-PIG", "AG-POUL", ""];

    private static readonly string[] FacilityTypeCodes = ["AG", "SL", "MK", ""];

    private static readonly string[] FacilitySubBusinessActivityCodes =
        ["AG-MARP-SLSH", "AG-MARP-SLSL", "AG-SHO-NK", "AG-SHO-SHP", "AG-SHO-SHSL", "AG-SHO-SHU", ""];

    private static readonly string[] FeatureStatusCodes = ["ACTIVE", "INACTIVE", "SUSPENDED"];

    private static readonly string[] UkInternalCodes = ["ENGLAND", "WALES", "SCOTLAND"];

    private static readonly string[] IntervalUnits = ["Months", "Years", "Days"];

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
                BATCH_ID                       VARCHAR,
                CHANGE_TYPE                    VARCHAR,
                CPH                            VARCHAR,
                FEATURE_NAME                   VARCHAR,
                CPH_TYPE                       VARCHAR,
                ADDRESS_PK                     VARCHAR,
                SAON_START_NUMBER              VARCHAR,
                SAON_START_NUMBER_SUFFIX        VARCHAR,
                SAON_END_NUMBER                VARCHAR,
                SAON_END_NUMBER_SUFFIX          VARCHAR,
                PAON_START_NUMBER              VARCHAR,
                PAON_START_NUMBER_SUFFIX        VARCHAR,
                PAON_END_NUMBER                VARCHAR,
                PAON_END_NUMBER_SUFFIX          VARCHAR,
                STREET                         VARCHAR,
                TOWN                           VARCHAR,
                LOCALITY                       VARCHAR,
                UK_INTERNAL_CODE               VARCHAR,
                POSTCODE                       VARCHAR,
                COUNTRY_CODE                   VARCHAR,
                UDPRN                          VARCHAR,
                EASTING                        VARCHAR,
                NORTHING                       VARCHAR,
                OS_MAP_REFERENCE               VARCHAR,
                DISEASE_TYPE                   VARCHAR,
                INTERVAL                       VARCHAR,
                INTERVAL_UNIT_OF_TIME          VARCHAR,
                FEATURE_ADDRESS_FROM_DATE      VARCHAR,
                FEATURE_ADDRESS_TO_DATE        VARCHAR,
                CPH_RELATIONSHIP_TYPE          VARCHAR,
                SECONDARY_CPH                  VARCHAR,
                FACILITY_BUSINSS_ACTVTY_CODE   VARCHAR,
                FACILITY_TYPE_CODE             VARCHAR,
                FCLTY_SUB_BSNSS_ACTVTY_CODE    VARCHAR,
                FEATURE_STATUS_CODE            VARCHAR,
                MOVEMENT_RSTRCTN_RSN_CODE      VARCHAR,
                ANIMAL_SPECIES_CODE            VARCHAR,
                ANIMAL_PRODUCTION_USAGE_CODE   VARCHAR
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
            var isEdgeCase = i >= rowCount - (int)(rowCount * 0.02);
            var row = appender.CreateRow();

            if (isEdgeCase && i % 2 == 0)
            {
                AppendNullCphRow(row, faker);
            }
            else if (isEdgeCase)
            {
                AppendEmptyCphRow(row, faker);
            }
            else
            {
                AppendNormalRow(row, faker, cphPool, i);
            }

            row.EndRow();
        }
    }

    private static void AppendNormalRow(DuckDBAppenderRow row, Faker faker, List<string> cphPool, int index)
    {
        var cph = faker.PickRandom(cphPool);
        var addressPk = (25000000 + index + 1).ToString();

        row.AppendValue("1");                                                // BATCH_ID
        row.AppendValue(faker.PickRandom(ChangeTypes));                      // CHANGE_TYPE
        row.AppendValue(cph);                                                // CPH
        row.AppendValue($"Feature {index + 1}");                             // FEATURE_NAME
        row.AppendValue(faker.PickRandom(CphTypes));                         // CPH_TYPE
        row.AppendValue(addressPk);                                          // ADDRESS_PK
        row.AppendValue(faker.Random.Int(1, 99).ToString());                 // SAON_START_NUMBER
        row.AppendValue(faker.Random.Bool(0.3f) ? faker.Random.String2(1, "ABCDEF") : ""); // SAON_START_NUMBER_SUFFIX
        row.AppendValue(faker.Random.Int(1, 99).ToString());                 // SAON_END_NUMBER
        row.AppendValue(faker.Random.Bool(0.3f) ? faker.Random.String2(1, "ABCDEF") : ""); // SAON_END_NUMBER_SUFFIX
        row.AppendValue(faker.Random.Int(1, 99).ToString());                 // PAON_START_NUMBER
        row.AppendValue(faker.Random.Bool(0.3f) ? faker.Random.String2(1, "ABCDEF") : ""); // PAON_START_NUMBER_SUFFIX
        row.AppendValue(faker.Random.Int(1, 99).ToString());                 // PAON_END_NUMBER
        row.AppendValue(faker.Random.Bool(0.3f) ? faker.Random.String2(1, "ABCDEF") : ""); // PAON_END_NUMBER_SUFFIX
        row.AppendValue($"Holding Street {index + 1}");                      // STREET
        row.AppendValue($"Town{index + 1}");                                 // TOWN
        row.AppendValue($"Locality{index + 1}");                             // LOCALITY
        row.AppendValue(faker.PickRandom(UkInternalCodes));                  // UK_INTERNAL_CODE
        row.AppendValue($"CPH{index + 1:D2} {100 + index + 1}");            // POSTCODE
        row.AppendValue("GB");                                               // COUNTRY_CODE
        row.AppendValue(addressPk);                                          // UDPRN
        row.AppendValue((400000 + index + 1).ToString());                    // EASTING
        row.AppendValue((500000 + index + 1).ToString());                    // NORTHING
        row.AppendNullValue();                                               // OS_MAP_REFERENCE
        row.AppendNullValue();                                               // DISEASE_TYPE
        row.AppendValue(faker.Random.Int(1, 24).ToString());                 // INTERVAL
        row.AppendValue(faker.PickRandom(IntervalUnits));                    // INTERVAL_UNIT_OF_TIME
        row.AppendValue("2025-01-01");                                       // FEATURE_ADDRESS_FROM_DATE
        row.AppendNullValue();                                               // FEATURE_ADDRESS_TO_DATE
        row.AppendValue(faker.PickRandom(CphRelationshipTypes));             // CPH_RELATIONSHIP_TYPE
        row.AppendValue(faker.Random.Bool(0.3f) ? GenerateCph(faker) : "");  // SECONDARY_CPH
        row.AppendValue(faker.PickRandom(FacilityBusinessActivityCodes));    // FACILITY_BUSINSS_ACTVTY_CODE
        row.AppendValue(faker.PickRandom(FacilityTypeCodes));                // FACILITY_TYPE_CODE
        row.AppendValue(faker.PickRandom(FacilitySubBusinessActivityCodes)); // FCLTY_SUB_BSNSS_ACTVTY_CODE
        row.AppendValue(faker.PickRandom(FeatureStatusCodes));               // FEATURE_STATUS_CODE
        row.AppendNullValue();                                               // MOVEMENT_RSTRCTN_RSN_CODE
        row.AppendValue(faker.PickRandom(AnimalSpeciesCodes));               // ANIMAL_SPECIES_CODE
        row.AppendValue(faker.PickRandom(AnimalProductionUsageCodes));        // ANIMAL_PRODUCTION_USAGE_CODE
    }

    private static void AppendNullCphRow(DuckDBAppenderRow row, Faker faker)
    {
        row.AppendValue("1");                                                // BATCH_ID
        row.AppendValue("I");                                                // CHANGE_TYPE
        row.AppendNullValue();                                               // CPH (null edge case)
        row.AppendValue("Feature Null");                                     // FEATURE_NAME
        row.AppendValue("MAIN");                                             // CPH_TYPE
        row.AppendNullValue();                                               // ADDRESS_PK
        row.AppendNullValue();                                               // SAON_START_NUMBER
        row.AppendNullValue();                                               // SAON_START_NUMBER_SUFFIX
        row.AppendNullValue();                                               // SAON_END_NUMBER
        row.AppendNullValue();                                               // SAON_END_NUMBER_SUFFIX
        row.AppendNullValue();                                               // PAON_START_NUMBER
        row.AppendNullValue();                                               // PAON_START_NUMBER_SUFFIX
        row.AppendNullValue();                                               // PAON_END_NUMBER
        row.AppendNullValue();                                               // PAON_END_NUMBER_SUFFIX
        row.AppendNullValue();                                               // STREET
        row.AppendNullValue();                                               // TOWN
        row.AppendNullValue();                                               // LOCALITY
        row.AppendValue("ENGLAND");                                          // UK_INTERNAL_CODE
        row.AppendNullValue();                                               // POSTCODE
        row.AppendValue("GB");                                               // COUNTRY_CODE
        row.AppendNullValue();                                               // UDPRN
        row.AppendNullValue();                                               // EASTING
        row.AppendNullValue();                                               // NORTHING
        row.AppendNullValue();                                               // OS_MAP_REFERENCE
        row.AppendNullValue();                                               // DISEASE_TYPE
        row.AppendNullValue();                                               // INTERVAL
        row.AppendNullValue();                                               // INTERVAL_UNIT_OF_TIME
        row.AppendNullValue();                                               // FEATURE_ADDRESS_FROM_DATE
        row.AppendNullValue();                                               // FEATURE_ADDRESS_TO_DATE
        row.AppendNullValue();                                               // CPH_RELATIONSHIP_TYPE
        row.AppendNullValue();                                               // SECONDARY_CPH
        row.AppendNullValue();                                               // FACILITY_BUSINSS_ACTVTY_CODE
        row.AppendNullValue();                                               // FACILITY_TYPE_CODE
        row.AppendNullValue();                                               // FCLTY_SUB_BSNSS_ACTVTY_CODE
        row.AppendValue("ACTIVE");                                           // FEATURE_STATUS_CODE
        row.AppendNullValue();                                               // MOVEMENT_RSTRCTN_RSN_CODE
        row.AppendValue(faker.PickRandom(AnimalSpeciesCodes));               // ANIMAL_SPECIES_CODE
        row.AppendValue(faker.PickRandom(AnimalProductionUsageCodes));        // ANIMAL_PRODUCTION_USAGE_CODE
    }

    private static void AppendEmptyCphRow(DuckDBAppenderRow row, Faker faker)
    {
        row.AppendValue("1");                                                // BATCH_ID
        row.AppendValue("I");                                                // CHANGE_TYPE
        row.AppendValue("");                                                 // CPH (empty edge case)
        row.AppendValue("Feature Empty");                                    // FEATURE_NAME
        row.AppendValue("MAIN");                                             // CPH_TYPE
        row.AppendValue("");                                                 // ADDRESS_PK
        row.AppendValue("");                                                 // SAON_START_NUMBER
        row.AppendValue("");                                                 // SAON_START_NUMBER_SUFFIX
        row.AppendValue("");                                                 // SAON_END_NUMBER
        row.AppendValue("");                                                 // SAON_END_NUMBER_SUFFIX
        row.AppendValue("");                                                 // PAON_START_NUMBER
        row.AppendValue("");                                                 // PAON_START_NUMBER_SUFFIX
        row.AppendValue("");                                                 // PAON_END_NUMBER
        row.AppendValue("");                                                 // PAON_END_NUMBER_SUFFIX
        row.AppendValue("");                                                 // STREET
        row.AppendValue("");                                                 // TOWN
        row.AppendValue("");                                                 // LOCALITY
        row.AppendValue("ENGLAND");                                          // UK_INTERNAL_CODE
        row.AppendValue("");                                                 // POSTCODE
        row.AppendValue("GB");                                               // COUNTRY_CODE
        row.AppendValue("");                                                 // UDPRN
        row.AppendValue("");                                                 // EASTING
        row.AppendValue("");                                                 // NORTHING
        row.AppendValue("");                                                 // OS_MAP_REFERENCE
        row.AppendValue("");                                                 // DISEASE_TYPE
        row.AppendValue("");                                                 // INTERVAL
        row.AppendValue("");                                                 // INTERVAL_UNIT_OF_TIME
        row.AppendValue("");                                                 // FEATURE_ADDRESS_FROM_DATE
        row.AppendValue("");                                                 // FEATURE_ADDRESS_TO_DATE
        row.AppendValue("");                                                 // CPH_RELATIONSHIP_TYPE
        row.AppendValue("");                                                 // SECONDARY_CPH
        row.AppendValue("");                                                 // FACILITY_BUSINSS_ACTVTY_CODE
        row.AppendValue("");                                                 // FACILITY_TYPE_CODE
        row.AppendValue("");                                                 // FCLTY_SUB_BSNSS_ACTVTY_CODE
        row.AppendValue("ACTIVE");                                           // FEATURE_STATUS_CODE
        row.AppendValue("");                                                 // MOVEMENT_RSTRCTN_RSN_CODE
        row.AppendValue(faker.PickRandom(AnimalSpeciesCodes));               // ANIMAL_SPECIES_CODE
        row.AppendValue(faker.PickRandom(AnimalProductionUsageCodes));        // ANIMAL_PRODUCTION_USAGE_CODE
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
