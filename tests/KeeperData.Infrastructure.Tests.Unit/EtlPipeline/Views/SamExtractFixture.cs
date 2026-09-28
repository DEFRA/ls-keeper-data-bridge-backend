using DuckDB.NET.Data;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>A small SAM extract shaped to exercise the transformation's rules: missing-value
/// sentinels, comma-delimited relationship tokens, duplicate source rows, an invalid CPHH, and
/// relationships pointing at holdings and herds that are not in the canonical population.
///
/// The CTS tables are created alongside it, by <see cref="CtsExtractFixture"/>. The transformation
/// is one script over one staging database, so a staging database missing either half does not bind
/// - every caller needs both, and none of them needs to say so.</summary>
public static class SamExtractFixture
{
    /// <summary>Every column the read model reads from sam_cph_holdings, in the order the fixture
    /// has always declared them. A column the extract never carried can be left out of the staging
    /// table entirely, so tests may withhold any of them.</summary>
    private static readonly string[] HoldingColumns =
    [
        "CPH", "FEATURE_NAME", "CPH_TYPE", "ADDRESS_PK",
        "SAON_START_NUMBER", "SAON_START_NUMBER_SUFFIX", "SAON_END_NUMBER",
        "SAON_END_NUMBER_SUFFIX", "SAON_DESCRIPTION", "PAON_START_NUMBER",
        "PAON_START_NUMBER_SUFFIX", "PAON_END_NUMBER", "PAON_END_NUMBER_SUFFIX",
        "PAON_DESCRIPTION", "STREET", "TOWN", "LOCALITY",
        "UK_INTERNAL_CODE", "POSTCODE", "COUNTRY_CODE", "UDPRN",
        "EASTING", "NORTHING", "OS_MAP_REFERENCE", "DISEASE_TYPE",
        "INTERVAL", "INTERVAL_UNIT_OF_TIME", "ANIMAL_SPECIES_CODE",
        "ANIMAL_PRODUCTION_USAGE_CODE", "FEATURE_ADDRESS_FROM_DATE",
        "FEATURE_ADDRESS_TO_DATE"
    ];

    public static void Create(string databasePath) => Create(databasePath, omittedHoldingColumns: Array.Empty<string>(), holdingColumnTypes: null, herdColumnTypes: null);

    public static void Create(string databasePath, IReadOnlyList<string> omittedHoldingColumns, IReadOnlyDictionary<string,string>? holdingColumnTypes = null, IReadOnlyDictionary<string,string>? herdColumnTypes = null)
    {
        var holdingColumns = string.Join(", ",
            HoldingColumns.Where(column => !omittedHoldingColumns.Contains(column))
                          .Select(column =>
                          {
                              var type = holdingColumnTypes != null && holdingColumnTypes.TryGetValue(column, out var t) ? t : "VARCHAR";
                              return $"{column} {type}";
                          }));

        var herdColumnDefs = string.Join(", ", new[]
        {
            "HERDMARK VARCHAR",
            "CPHH VARCHAR",
            "KEEPER_PARTY_IDS VARCHAR",
            "OWNER_PARTY_IDS VARCHAR",
            "ANIMAL_SPECIES_CODE VARCHAR",
            "ANIMAL_PURPOSE_CODE VARCHAR",
            $"ANIMAL_GROUP_ID_MCH_FRM_DAT {(herdColumnTypes != null && herdColumnTypes.TryGetValue("ANIMAL_GROUP_ID_MCH_FRM_DAT", out var h1) ? h1 : "VARCHAR")}",
            $"ANIMAL_GROUP_ID_MCH_TO_DAT {(herdColumnTypes != null && herdColumnTypes.TryGetValue("ANIMAL_GROUP_ID_MCH_TO_DAT", out var h2) ? h2 : "VARCHAR")}" 
        });

        using var connection = new DuckDBConnection($"Data Source={databasePath}");
        connection.Open();

        Execute(connection, $"""
            CREATE TABLE sam_cph_holdings ({holdingColumns});

            CREATE TABLE sam_party (
                PARTY_ID VARCHAR, PERSON_TITLE VARCHAR, PERSON_GIVEN_NAME VARCHAR, PERSON_GIVEN_NAME2 VARCHAR,
                PERSON_INITIALS VARCHAR, PERSON_FAMILY_NAME VARCHAR, ORGANISATION_NAME VARCHAR,
                TELEPHONE_NUMBER VARCHAR, MOBILE_NUMBER VARCHAR, INTERNET_EMAIL_ADDRESS VARCHAR, ROLES VARCHAR);

            CREATE TABLE sam_cph_holder (
                PARTY_ID VARCHAR, PERSON_TITLE VARCHAR, PERSON_GIVEN_NAME VARCHAR, PERSON_GIVEN_NAME2 VARCHAR,
                PERSON_INITIALS VARCHAR, PERSON_FAMILY_NAME VARCHAR, ORGANISATION_NAME VARCHAR,
                TELEPHONE_NUMBER VARCHAR, MOBILE_NUMBER VARCHAR, INTERNET_EMAIL_ADDRESS VARCHAR, CPHS VARCHAR);

            CREATE TABLE sam_herd (
                HERDMARK VARCHAR, CPHH VARCHAR, KEEPER_PARTY_IDS VARCHAR, OWNER_PARTY_IDS VARCHAR,
                ANIMAL_SPECIES_CODE VARCHAR, ANIMAL_PURPOSE_CODE VARCHAR, DISEASE_TYPE VARCHAR,
                INTERVALS VARCHAR, INTERVAL_UNIT_OF_TIME VARCHAR, MOVEMENT_RSTRCTN_RSN_CODE VARCHAR,
                ANIMAL_GROUP_ID_MCH_FRM_DAT VARCHAR, ANIMAL_GROUP_ID_MCH_TO_DAT VARCHAR);
            """);

        Execute(connection, """
            INSERT INTO sam_cph_holdings (CPH, FEATURE_NAME, CPH_TYPE, STREET, TOWN, POSTCODE,
                UK_INTERNAL_CODE, ANIMAL_SPECIES_CODE, ANIMAL_PRODUCTION_USAGE_CODE, DISEASE_TYPE,
                INTERVAL, INTERVAL_UNIT_OF_TIME, FEATURE_ADDRESS_FROM_DATE)
            VALUES
                -- Two concurrent records for one CPH. The later one wins, and the whole address
                -- follows it rather than being mixed with the earlier record.
                ('01/234/5678', 'Superseded Farm', 'PERMANENT', 'Old Street', 'Exeter', 'EX1 1AA',
                 'ENGLAND', '01', '-', '', ',', 'M', '2024-01-01 00:00:00'),
                -- Different missing-value sentinels represent the same profile and must collapse.
                ('01/234/5678', 'Main Farm', 'PERMANENT', 'New Street', 'Exeter', 'EX1 1AA',
                 'ENGLAND', '01', NULL, '-', NULL, ' M ', '2025-06-01 00:00:00'),
                -- The later record names no location, so the earlier real name must survive.
                ('02/345/6789', 'Known Farm', 'TEMPORARY', NULL, 'Truro', '',
                 'SCOTLAND', NULL, NULL, NULL, NULL, NULL, '2024-01-01 00:00:00'),
                ('02/345/6789', 'Notknown', 'TEMPORARY', NULL, 'Truro', '',
                 'SCOTLAND', NULL, NULL, NULL, NULL, NULL, '2025-06-01 00:00:00'),
                ('  03/456/7890  ', 'Spaced Farm', 'EMERGENCY', NULL, 'Bodmin', 'PL31 1AA',
                 'NORTHERN IRELAND', '02', 'BEEF', NULL, NULL, NULL, '2025-01-01 00:00:00'),
                -- Two records sharing a date, so the date alone cannot decide between them.
                ('04/567/8901', 'Tied Alpha', 'PERMANENT', 'Alpha Street', 'Newport', 'NP1 1AA',
                 'WALES', NULL, NULL, NULL, NULL, NULL, '2025-03-01 00:00:00'),
                ('04/567/8901', 'Tied Beta', 'PERMANENT', 'Beta Street', 'Newport', 'NP1 1AA',
                 'WALES', NULL, NULL, NULL, NULL, NULL, '2025-03-01 00:00:00'),
                ('-', 'Sentinel Only', 'PERMANENT', NULL, 'Nowhere', NULL,
                 NULL, NULL, NULL, NULL, NULL, NULL, '2025-01-01 00:00:00');

            INSERT INTO sam_party (PARTY_ID, PERSON_TITLE, PERSON_GIVEN_NAME, PERSON_FAMILY_NAME,
                ORGANISATION_NAME, TELEPHONE_NUMBER, INTERNET_EMAIL_ADDRESS, ROLES)
            VALUES
                ('P1', 'Mr', 'Alan', 'Archer', NULL, '01392 000001', 'Alan.Archer@Example.TEST', 'keeper,owner'),
                -- P2 is also a holder, so its sentinels must give way to the holder's real names.
                ('P2', '-', '-', '-', NULL, ',', '', ''),
                -- P3 carries the organisation placeholder, which must not make it an organisation.
                ('P3', 'Ms', 'Carol', 'Cooper', 'No Organisation Name', NULL, NULL, NULL),
                -- P6 exists nowhere else, so nothing can fill in what its sentinels stand for.
                ('P6', '-', '', ',', NULL, '-', '   ', '-');

            INSERT INTO sam_cph_holder (PARTY_ID, PERSON_TITLE, PERSON_GIVEN_NAME, PERSON_FAMILY_NAME,
                TELEPHONE_NUMBER, MOBILE_NUMBER, INTERNET_EMAIL_ADDRESS, CPHS)
            VALUES
                -- Every one of P2's contact columns is a sentinel in sam_party, so the holder's real
                -- values must come through. The email is mixed case to prove it is still folded.
                ('P2', 'Mrs', 'Brenda', 'Baker', '01392 000002', '07700 900002',
                 'Brenda.Baker@Example.TEST', '01/234/5678, 02/345/6789'),
                -- P3 has a real title in sam_party, which a different one here must not displace.
                ('P3', 'Dr', 'Carol', 'Cooper', NULL, NULL, NULL, '99/999/9999'),
                -- P4 is in no other extract, so the holder is the only thing that can name it at all.
                ('P4', 'Mr', 'Derek', 'Dunn', NULL, '07700 900004', 'derek.dunn@example.test', NULL);

            INSERT INTO sam_herd (HERDMARK, CPHH, KEEPER_PARTY_IDS, OWNER_PARTY_IDS, ANIMAL_SPECIES_CODE,
                ANIMAL_PURPOSE_CODE, ANIMAL_GROUP_ID_MCH_FRM_DAT, ANIMAL_GROUP_ID_MCH_TO_DAT)
            VALUES
                ('AB1234', '01/234/5678/01', 'P1, P5', 'P1', '01', 'DAIRY', '2008-07-16 00:00:00', NULL),
                ('CD5678', 'NOT-A-CPHH', 'P1', 'P1', '01', 'BEEF', '2010-01-01 00:00:00', NULL),
                ('EF9012', '77/777/7777/01', 'P1', 'P1', '01', 'BEEF', '2011-01-01 00:00:00', NULL);
            """);

        // The address numbers go in by UPDATE rather than the INSERT above: a test may withhold the
        // columns entirely (the extract-never-carried-them case), and an UPDATE of a missing column
        // would not compile. Only columns present are set, per row.
        var addressNumbers = new (string Feature, string? Udprn, string? Easting, string? Northing)[]
        {
            ("Superseded Farm", "80000001", "300001", "400001"),
            ("Main Farm", "80000002", "300002", "400002"),
            ("Known Farm", "80000003", "300003", "400003"),
            ("Notknown", null, null, null),
            ("Spaced Farm", "80000005", "300005", "400005"),
            ("Tied Alpha", "80000006", "300006", "400006"),
            ("Tied Beta", "80000007", "300007", "400007"),
            ("Sentinel Only", null, null, null)
        };

        foreach (var (feature, udprn, easting, northing) in addressNumbers)
        {
            var assignments = new List<string>();

            if (!omittedHoldingColumns.Contains("UDPRN")) assignments.Add($"UDPRN = {Literal(udprn)}");
            if (!omittedHoldingColumns.Contains("EASTING")) assignments.Add($"EASTING = {Literal(easting)}");
            if (!omittedHoldingColumns.Contains("NORTHING")) assignments.Add($"NORTHING = {Literal(northing)}");

            if (assignments.Count > 0)
            {
                Execute(connection,
                    $"UPDATE sam_cph_holdings SET {string.Join(", ", assignments)} WHERE FEATURE_NAME = '{feature}'");
            }
        }

        CtsExtractFixture.Create(connection);
    }

    private static string Literal(string? value) => value is null ? "NULL" : $"'{value}'";

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
