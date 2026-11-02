using DuckDB.NET.Data;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>A small amls2_common_land extract shaped on a real sample file to exercise the
/// projection's rules: links in both directions, the flag's spellings, missing-value sentinels,
/// serial-number dates, the 2999 no-end sentinel, a common with no keeper, conflicting site
/// descriptions, and rows with no usable key that must not become links.
///
/// Column names match the observed extract. A separate minimal variant creates the table with
/// only its keys, for the run where the optional columns never arrived.
///
/// Link ids are readable rather than realistic, so a failure names the case rather than a
/// number.</summary>
public static class CommonLandExtractFixture
{
    /// <summary>The number of rows each table is expected to hold for this fixture. Links need
    /// both keys; commons need only their own - the keeper-less common is a site but no link.</summary>
    public const int ExpectedLinkCount = 6;

    public const int ExpectedCommonCount = 5;

    /// <summary>A staging database holding the amls2 table and nothing else - the mirror of the
    /// environment whose feed loads nothing but common land.</summary>
    public static void Create(string databasePath)
    {
        using var connection = new DuckDBConnection($"Data Source={databasePath}");
        connection.Open();

        Create(connection);
    }

    public static void Create(DuckDBConnection connection)
    {
        Execute(connection, """
            CREATE TABLE amls2_common_land (
                BATCH_ID VARCHAR, CHANGE_TYPE VARCHAR, COMMON_LAND_PREMISE_ID VARCHAR,
                MAIN_CPH VARCHAR, COMMON_CPH VARCHAR, BUSINESS_USAGE VARCHAR,
                PREMISES_NAME VARCHAR, ADDRESS_LINE_1 VARCHAR, ADDRESS_LINE_2 VARCHAR,
                ADDRESS_LINE_3 VARCHAR, LOCAL_AUTH_NAME VARCHAR, COUNTRY VARCHAR,
                POSTCODE VARCHAR, EASTING VARCHAR, NORTHING VARCHAR,
                LINK_ID VARCHAR, CONTIGUOUS_COMMON VARCHAR,
                START_DATE VARCHAR, END_DATE VARCHAR);
            """);

        Execute(connection, """
            INSERT INTO amls2_common_land (
                BATCH_ID, CHANGE_TYPE, COMMON_LAND_PREMISE_ID, MAIN_CPH, COMMON_CPH,
                BUSINESS_USAGE, PREMISES_NAME, ADDRESS_LINE_1, ADDRESS_LINE_2, ADDRESS_LINE_3,
                LOCAL_AUTH_NAME, COUNTRY, POSTCODE, EASTING, NORTHING,
                LINK_ID, CONTIGUOUS_COMMON, START_DATE, END_DATE) VALUES
                -- A live contiguous link. Serial dates as the review workbook wrote them:
                -- 40500 is 2010-11-18, 401768 is the 31-DEC-2999 no-end sentinel.
                ('21', 'I', '554454', '17/050/0003', '00/000/8267', 'Common Land',
                 '-', 'Black Mountain Common', NULL, NULL, 'POWYS COUNTY COUNCIL',
                 'Wales', NULL, '330400', NULL, '146780', 'Yes', '40500', '401768'),
                -- A second keeper on the same common, transported, with a real end date.
                ('21', 'I', '546200', '17/050/0004', '00/000/8267', 'Common Land',
                 '-', 'Black Mountain Common', NULL, NULL, 'POWYS COUNTY COUNCIL',
                 'Wales', 'LD1 1AA', '300000', '250000', 'L2', 'No', '2021-06-01', '2024-12-31'),
                -- One keeper registered on two commons.
                ('21', 'I', '546204', '17/050/0003', '00/000/8268', 'Common Land',
                 '-', 'GRETEL MARSH', 'Marsh Road', 'Estuary Side', 'CUMBRIA COUNCIL',
                 'England', 'CA7 4DD', '320000.0', '560000', 'L3', 'No', '2022-03-01', NULL),
                -- A second keeper on 8268 whose row describes the common differently. The later
                -- start makes 'GRETEL MARSH NORTH' the site's name, deterministically.
                ('21', 'I', '546204', '17/050/0005', '00/000/8268', 'Common Land',
                 '-', 'GRETEL MARSH NORTH', 'Marsh Road', 'Estuary Side', 'CUMBRIA COUNCIL',
                 'England', 'CA7 4DD', '320001', '560001', 'L10', 'No', '2023-01-01', NULL),
                -- Sentinels everywhere a link can carry them, including the '-1' no-link id:
                -- they normalise away, the link stays.
                ('21', 'I', '-', '18/060/0001', '00/000/8269', '-',
                 '-', '', NULL, ',', '-', '-', '-', '-', '-', '-1', '-', '-', ','),
                -- Untrimmed keys must still land trimmed.
                ('21', 'I', '546213', '  19/070/0001  ', ' 00/000/8270 ', 'Common Land',
                 '-', 'UPPER DOWN', NULL, NULL, 'DEVON COUNTY COUNCIL',
                 'England', 'EX1 9ZZ', NULL, NULL, 'L5', 'Yes', NULL, NULL),
                -- A common with no current keeper, exactly as the feed writes most rows: a site,
                -- not a link.
                ('21', 'I', '546300', '-', '00/000/8271', 'Common Land',
                 '-', 'LONELY COMMON', 'Moor Gate', NULL, 'NORTHUMBERLAND COUNCIL',
                 'England', 'NE1 2BB', '410000', '610000', '-1', 'No', NULL, NULL),
                -- No usable key at all: neither a link nor a site.
                ('21', 'I', '546400', '20/080/0001', NULL, 'Common Land',
                 '-', 'NO COMMON', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 'L8', 'Yes', NULL, NULL),
                ('21', 'I', '546500', '20/080/0002', '-', 'Common Land',
                 '-', 'SENTINEL COMMON', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 'L9', 'Yes', NULL, NULL);
            """);
    }

    /// <summary>The table as it stands where the feed carries only its key columns - every optional
    /// column absent rather than blank, which is the harder case for the projection.</summary>
    public static void CreateKeysOnly(DuckDBConnection connection)
    {
        Execute(connection, """
            CREATE TABLE amls2_common_land (MAIN_CPH VARCHAR, COMMON_CPH VARCHAR);
            """);

        Execute(connection, """
            INSERT INTO amls2_common_land VALUES
                ('17/050/0003', '00/000/8267'),
                ('17/050/0004', '00/000/8267');
            """);
    }

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
