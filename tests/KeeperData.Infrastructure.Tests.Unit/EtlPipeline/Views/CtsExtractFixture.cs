using DuckDB.NET.Data;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>A small CTS extract shaped to exercise every rule of the Open Locations projection:
/// competing keeper links, a holding with no county, Welsh and Scottish holdings, a separate
/// holding with no county at all, a sub-location, a superseded identifier, and each of the
/// exclusions - closed, label flag off, cancelled, BCMS's own records.
///
/// The VARCHAR2 columns store Oracle NULLs as empty strings and reach staging as text. The keys,
/// foreign keys and effective dates are declared in the dataset definition, so they reach it as
/// BIGINT and DATE with real nulls, and Oracle's RR pivot already applied by ColumnValueParser. The
/// fixture reproduces that split faithfully, because the projection's null and date handling is a
/// large part of what these tests are for.
///
/// Ids are readable rather than realistic - LOC 1..n, matching LID and PAR ids where it helps -
/// so a failure names the case rather than a number.</summary>
public static class CtsExtractFixture
{
    /// <summary>The query date the transformation is expected to run for. Fixture dates are chosen
    /// either side of it.</summary>
    public const string QueryDate = "2026-09-15";

    public static void Create(DuckDBConnection connection)
    {
        Execute(connection, """
            CREATE TABLE cts_locations (
                LOC_ID BIGINT, LOC_CTY_ID BIGINT, LOC_PREMISES_TYPE VARCHAR,
                LOC_RECEIVE_LABELS_FLAG VARCHAR, LOC_EFFECTIVE_FROM DATE, LOC_EFFECTIVE_TO DATE,
                LOC_CESSATION_REASON VARCHAR, LOC_CURRENT_STATUS VARCHAR,
                LOC_TEL_NUMBER VARCHAR, LOC_MOBILE_NUMBER VARCHAR, LOC_FAX_NUMBER VARCHAR,
                LOC_EMAIL_ADDRESS VARCHAR);

            CREATE TABLE cts_location_identifiers (
                LID_ID BIGINT, LID_LOC_ID BIGINT, LID_IDENTIFIER VARCHAR,
                LID_SUB_IDENTIFIER VARCHAR, LID_FULL_IDENTIFIER VARCHAR, LID_CURRENT_STATUS VARCHAR);

            CREATE TABLE cts_location_party_rels (
                LPR_ID BIGINT, LPR_LOC_ID BIGINT, LPR_PAR_ID BIGINT, LPR_LPT_ID BIGINT,
                LPR_EFFECTIVE_FROM_DATE DATE, LPR_EFFECTIVE_TO_DATE DATE,
                LPR_CESSATION_REASON VARCHAR, LPR_CURRENT_STATUS VARCHAR);

            CREATE TABLE cts_parties (
                PAR_ID BIGINT, PAR_TITLE VARCHAR, PAR_INITIALS VARCHAR, PAR_SURNAME VARCHAR,
                PAR_TEL_NUMBER VARCHAR, PAR_MOBILE_NUMBER VARCHAR, PAR_FAX_NUMBER VARCHAR,
                PAR_EMAIL_ADDRESS VARCHAR, PAR_WELSH_INDICATOR VARCHAR, PAR_CURRENT_STATUS VARCHAR);

            CREATE TABLE cts_addresses (
                ADR_ID BIGINT, ADR_LOC_ID BIGINT, ADR_PAR_ID BIGINT, ADR_NAME VARCHAR,
                ADR_ADDRESS_2 VARCHAR, ADR_ADDRESS_3 VARCHAR, ADR_ADDRESS_4 VARCHAR,
                ADR_ADDRESS_5 VARCHAR, ADR_POST_CODE VARCHAR, ADR_CURRENT_STATUS VARCHAR);

            CREATE TABLE cts_counties (
                CTY_ID BIGINT, CTY_CODE VARCHAR, CTY_NAME VARCHAR, CTY_UK_AREA VARCHAR);
            """);

        // CTY_UK_AREA is the country classification: '' (England), 'W', 'S'. 99 is BCMS's dummy.
        // CTY_CODE keeps its leading zero - '08' is a county, not eight - while CTY_ID is a number.
        Execute(connection, """
            INSERT INTO cts_counties (CTY_ID, CTY_CODE, CTY_NAME, CTY_UK_AREA) VALUES
                (10, '10', 'Devon',   ''),
                (8,  '08', 'Cumbria', ''),
                (52, '52', 'Powys',   'W'),
                (66, '66', 'Aberdeen','S'),
                (99, '99', 'BCMS',    '');
            """);

        Execute(connection, """
            INSERT INTO cts_locations (LOC_ID, LOC_CTY_ID, LOC_PREMISES_TYPE, LOC_RECEIVE_LABELS_FLAG,
                LOC_EFFECTIVE_FROM, LOC_EFFECTIVE_TO, LOC_CESSATION_REASON, LOC_CURRENT_STATUS,
                LOC_TEL_NUMBER, LOC_MOBILE_NUMBER, LOC_FAX_NUMBER, LOC_EMAIL_ADDRESS)
            VALUES
                -- 1  Plain open holding. Location contact details differ from the keeper's, which is
                --    what the report shows and a common place to wire the wrong column.
                (1,  10, 'AH', 'Y', '1996-07-01', NULL, '', '1',
                 '01392 000001', '07700 900001', '01392 900001', 'farm1@example.test'),
                -- 2  Three competing current keeper links: latest effective-from must win.
                (2,  10, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),
                -- 3  Sub-location. Included, and renders with its -01 suffix.
                (3,  10, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),
                -- 4  Stored as '22-JUN-66'. ColumnValueParser's RR pivot reads 1966, not 2066, so a
                --    holding this old still reaches the projection as a past effective-from.
                (4,  8,  'AH', 'Y', '1966-06-22', NULL, '', '1', '', '', '', ''),
                -- 5  Superseded identifier: one current CPH, one cancelled. Must not fan out.
                (5,  8,  'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),
                -- 6  Correspondence (CA) and contact (CO) links alongside the keeper.
                (6,  10, 'LK', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),
                -- 7  Keeper has no address row at all: kept, with empty address columns.
                (7,  10, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),
                -- 8  Premises type outside spproc112's AH/LK/TH: still in the report.
                (8,  10, 'SG', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),

                -- Excluded, one reason each.
                (20, 10, 'AH', 'N', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- label flag off
                (21, 10, 'AH', 'Y', '1996-07-01', '2024-12-31', 'LC', '1', '', '', '', ''),  -- closed
                (22, 10, 'AH', 'Y', '1996-07-01', NULL, '', '2', '', '', '', ''),  -- cancelled
                (23, 10, 'AH', 'Y', '2030-07-30', NULL, '', '1', '', '', '', ''),  -- not yet effective
                (24, 10, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- no keeper link
                (25, 52, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- Wales
                (26, 66, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- Scotland
                (27, 99, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- county 99
                (28, 8,  'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- AH-08/205/8000
                (29, 10, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- keeper link ended
                (30, 10, 'AH', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- keeper link cancelled

                -- County-less separate holdings, classified by the postcode of their own address.
                (40, NULL, 'SR', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- EX: England, kept
                (41, NULL, 'SR', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- LL: Wales, dropped
                (42, NULL, 'SR', 'Y', '1996-07-01', NULL, '', '1', '', '', '', ''),  -- PA: Scotland, dropped
                (43, NULL, 'SR', 'Y', '1996-07-01', NULL, '', '1', '', '', '', '');  -- SH-9999, BCMS
            """);

        Execute(connection, """
            INSERT INTO cts_location_identifiers (LID_ID, LID_LOC_ID, LID_IDENTIFIER,
                LID_SUB_IDENTIFIER, LID_FULL_IDENTIFIER, LID_CURRENT_STATUS)
            VALUES
                (1,  1,  '10/001/0001', '',   'AH-10/001/0001',    '1'),
                (2,  2,  '10/001/0002', '',   'AH-10/001/0002',    '1'),
                (3,  3,  '10/001/0003', '01', 'AH-10/001/0003-01', '1'),
                (4,  4,  '08/001/0004', '',   'AH-08/001/0004',    '1'),
                -- Location 5 has held two identifiers. Only the current one may appear.
                (5,  5,  '08/001/0005', '',   'AH-08/001/0005',    '1'),
                (6,  5,  '08/999/9999', '',   'AH-08/999/9999',    '2'),
                (7,  6,  '10/001/0006', '',   'AH-10/001/0006',    '1'),
                (8,  7,  '10/001/0007', '',   'AH-10/001/0007',    '1'),
                (9,  8,  '10/001/0008', '',   'AH-10/001/0008',    '1'),

                (20, 20, '10/002/0020', '', 'AH-10/002/0020', '1'),
                (21, 21, '10/002/0021', '', 'AH-10/002/0021', '1'),
                (22, 22, '10/002/0022', '', 'AH-10/002/0022', '1'),
                (23, 23, '10/002/0023', '', 'AH-10/002/0023', '1'),
                (24, 24, '10/002/0024', '', 'AH-10/002/0024', '1'),
                (25, 25, '52/002/0025', '', 'AH-52/002/0025', '1'),
                (26, 26, '66/002/0026', '', 'AH-66/002/0026', '1'),
                (27, 27, '99/002/0027', '', 'AH-99/002/0027', '1'),
                (28, 28, '08/205/8000', '', 'AH-08/205/8000', '1'),
                (29, 29, '10/002/0029', '', 'AH-10/002/0029', '1'),
                (30, 30, '10/002/0030', '', 'AH-10/002/0030', '1'),

                (40, 40, '4001', '', 'SH-4001', '1'),
                (41, 41, '4002', '', 'SH-4002', '1'),
                (42, 42, '4003', '', 'SH-4003', '1'),
                (43, 43, '9999', '', 'SH-9999', '1');
            """);

        // LPT 4 = KN keeper name, 5 = CA correspondence, 8 = CO other contact.
        Execute(connection, """
            INSERT INTO cts_location_party_rels (LPR_ID, LPR_LOC_ID, LPR_PAR_ID, LPR_LPT_ID,
                LPR_EFFECTIVE_FROM_DATE, LPR_EFFECTIVE_TO_DATE, LPR_CESSATION_REASON, LPR_CURRENT_STATUS)
            VALUES
                (1,  1, 1, 4, '1996-07-01', NULL, '', '1'),

                -- Location 2: the 2020 link is the latest and must be the one reported.
                (2,  2, 2, 4, '1996-07-01', NULL, '', '1'),
                (3,  2, 3, 4, '2020-01-01', NULL, '', '1'),
                (4,  2, 4, 4, '2010-01-01', NULL, '', '1'),

                (5,  3, 5, 4, '1996-07-01', NULL, '', '1'),
                -- Location 4: stored as '22-JUN-66', pivoted to 1966 before it reaches staging.
                (6,  4, 6, 4, '1966-06-22', NULL, '', '1'),
                (7,  5, 7, 4, '1996-07-01', NULL, '', '1'),

                -- Location 6 carries all three link types.
                (8,  6, 8,  4, '1996-07-01', NULL, '', '1'),
                (9,  6, 9,  5, '1996-07-01', NULL, '', '1'),
                (10, 6, 10, 8, '1996-07-01', NULL, '', '1'),

                (11, 7, 11, 4, '1996-07-01', NULL, '', '1'),
                (12, 8, 12, 4, '1996-07-01', NULL, '', '1'),

                (20, 20, 1, 4, '1996-07-01', NULL, '', '1'),
                (21, 21, 1, 4, '1996-07-01', NULL, '', '1'),
                (22, 22, 1, 4, '1996-07-01', NULL, '', '1'),
                (23, 23, 1, 4, '1996-07-01', NULL, '', '1'),
                -- 24 deliberately has no KN link, only a correspondence one.
                (24, 24, 1, 5, '1996-07-01', NULL, '', '1'),
                (25, 25, 1, 4, '1996-07-01', NULL, '', '1'),
                (26, 26, 1, 4, '1996-07-01', NULL, '', '1'),
                (27, 27, 1, 4, '1996-07-01', NULL, '', '1'),
                (28, 28, 1, 4, '1996-07-01', NULL, '', '1'),
                -- 29's only keeper link ended before the query date; 30's is cancelled.
                (29, 29, 1, 4, '1996-07-01', '2024-12-31', '', '1'),
                (30, 30, 1, 4, '1996-07-01', NULL, '', '2'),

                (40, 40, 40, 4, '1996-07-01', NULL, '', '1'),
                (41, 41, 41, 4, '1996-07-01', NULL, '', '1'),
                (42, 42, 42, 4, '1996-07-01', NULL, '', '1'),
                (43, 43, 43, 4, '1996-07-01', NULL, '', '1');
            """);

        Execute(connection, """
            INSERT INTO cts_parties (PAR_ID, PAR_TITLE, PAR_INITIALS, PAR_SURNAME, PAR_TEL_NUMBER,
                PAR_MOBILE_NUMBER, PAR_FAX_NUMBER, PAR_EMAIL_ADDRESS, PAR_WELSH_INDICATOR, PAR_CURRENT_STATUS)
            VALUES
                (1,  'MR',     'A',  'ARCHER',  '01392 111111', '07700 900111', '01392 911111', 'archer@example.test', 'N', '1'),
                (2,  'MRS',    'B',  'BAKER',   '', '', '', '', 'N', '1'),
                (3,  'MISS',   'C',  'CARTER',  '', '', '', 'carter@example.test', 'N', '1'),
                (4,  'DR',     'D',  'DYER',    '', '', '', '', 'N', '1'),
                (5,  'MESSRS', 'E',  'ELLIS',   '', '', '', '', 'N', '1'),
                (6,  'MR',     'F',  'FLETCHER','', '', '', '', 'N', '1'),
                (7,  'MR',     'G',  'GARDNER', '', '', '', '', 'N', '1'),
                (8,  'MR',     'H',  'HAYES',   '', '', '', '', 'N', '1'),
                (9,  'MS',     'I',  'IRVING',  '01392 999999', '', '', 'irving@example.test', 'Y', '1'),
                (10, 'MR',     'J',  'JARVIS',  '', '', '', 'jarvis@example.test', 'N', '1'),
                (11, 'MR',     'K',  'KNIGHT',  '', '', '', '', 'N', '1'),
                (12, 'MR',     'L',  'LOWE',    '', '', '', '', 'N', '1'),
                (40, 'MR',     'M',  'MASON',   '', '', '', '', 'N', '1'),
                (41, 'MR',     'N',  'NORRIS',  '', '', '', '', 'Y', '1'),
                (42, 'MR',     'O',  'OGILVY',  '', '', '', '', 'N', '1'),
                (43, '',       '',   'BCMS',    '', '', '', '', 'N', '1');
            """);

        // A keeper address hangs off ADR_PAR_ID; a location's own address off ADR_LOC_ID.
        // P7 deliberately has none, and location 7 deliberately has none.
        Execute(connection, """
            INSERT INTO cts_addresses (ADR_ID, ADR_LOC_ID, ADR_PAR_ID, ADR_NAME, ADR_ADDRESS_2,
                ADR_ADDRESS_3, ADR_ADDRESS_4, ADR_ADDRESS_5, ADR_POST_CODE, ADR_CURRENT_STATUS)
            VALUES
                (1,  NULL, 1,  'ARCHER FARM',  'LINE 2', 'LINE 3', 'LINE 4', 'LINE 5', 'EX1 1AA', '1'),
                (2,  NULL, 3,  'CARTER FARM',  '', '', '', '', 'EX2 2BB', '1'),
                (3,  NULL, 5,  'ELLIS FARM',   '', '', '', '', 'EX3 3CC', '1'),
                (4,  NULL, 6,  'FLETCHER FARM','', '', '', '', 'CA1 1AA', '1'),
                (5,  NULL, 7,  'GARDNER FARM', '', '', '', '', 'CA2 2BB', '1'),
                (6,  NULL, 8,  'HAYES FARM',   '', '', '', '', 'EX4 4DD', '1'),
                (7,  NULL, 9,  'IRVING HOUSE', '', '', '', '', 'EX5 5EE', '1'),
                (8,  NULL, 10, 'JARVIS HOUSE', '', '', '', '', 'EX6 6FF', '1'),
                -- Party 11 has only a cancelled address, so location 7 reports empty address columns.
                (9,  NULL, 11, 'OLD KNIGHT FARM', '', '', '', '', 'EX7 7GG', '2'),
                (10, NULL, 12, 'LOWE FARM',    '', '', '', '', 'EX8 8HH', '1'),
                (11, NULL, 2,  'BAKER FARM',   '', '', '', '', 'EX9 9II', '1'),
                (12, NULL, 4,  'DYER FARM',    '', '', '', '', 'EX9 9JJ', '1'),
                (13, NULL, 40, 'MASON FARM',   '', '', '', '', 'EX10 1AA', '1'),
                (14, NULL, 41, 'NORRIS FARM',  '', '', '', '', 'LL11 1AA', '1'),
                (15, NULL, 42, 'OGILVY FARM',  '', '', '', '', 'PA12 1AA', '1'),
                (16, NULL, 43, 'BCMS',         '', '', '', '', 'CA14 2DD', '1'),

                (30, 1,  NULL, 'LOCATION ONE', 'LOC 2', 'LOC 3', '', '', 'EX1 1AA', '1'),
                -- The country of a county-less holding is read from its own address postcode.
                (40, 40, NULL, 'MASON SHOW',  '', '', '', '', 'EX10 1AA', '1'),
                (41, 41, NULL, 'NORRIS SHOW', '', '', '', '', 'LL11 1AA', '1'),
                (42, 42, NULL, 'OGILVY SHOW', '', '', '', '', 'PA12 1AA', '1'),
                (43, 43, NULL, 'BCMS',        '', '', '', '', 'CA14 2DD', '1');
            """);
    }

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
