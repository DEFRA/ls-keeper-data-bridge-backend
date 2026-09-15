using DuckDB.NET.Data;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>A small CTS extract shaped to exercise every rule of the Open Locations projection:
/// competing keeper links, a holding with no county, Welsh and Scottish holdings, a separate
/// holding with no county at all, a sub-location, a superseded identifier, and each of the
/// exclusions - closed, label flag off, cancelled, BCMS's own records.
///
/// The extract stores Oracle NULLs as empty strings and every column as text, dates as 'DD-MON-YY'.
/// The fixture reproduces that faithfully, because the projection's null and date handling is a
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
                LOC_ID VARCHAR, LOC_CTY_ID VARCHAR, LOC_PREMISES_TYPE VARCHAR,
                LOC_RECEIVE_LABELS_FLAG VARCHAR, LOC_EFFECTIVE_FROM VARCHAR, LOC_EFFECTIVE_TO VARCHAR,
                LOC_CESSATION_REASON VARCHAR, LOC_CURRENT_STATUS VARCHAR,
                LOC_TEL_NUMBER VARCHAR, LOC_MOBILE_NUMBER VARCHAR, LOC_FAX_NUMBER VARCHAR,
                LOC_EMAIL_ADDRESS VARCHAR);

            CREATE TABLE cts_location_identifiers (
                LID_ID VARCHAR, LID_LOC_ID VARCHAR, LID_IDENTIFIER VARCHAR,
                LID_SUB_IDENTIFIER VARCHAR, LID_FULL_IDENTIFIER VARCHAR, LID_CURRENT_STATUS VARCHAR);

            CREATE TABLE cts_location_party_rels (
                LPR_ID VARCHAR, LPR_LOC_ID VARCHAR, LPR_PAR_ID VARCHAR, LPR_LPT_ID VARCHAR,
                LPR_EFFECTIVE_FROM_DATE VARCHAR, LPR_EFFECTIVE_TO_DATE VARCHAR,
                LPR_CESSATION_REASON VARCHAR, LPR_CURRENT_STATUS VARCHAR);

            CREATE TABLE cts_parties (
                PAR_ID VARCHAR, PAR_TITLE VARCHAR, PAR_INITIALS VARCHAR, PAR_SURNAME VARCHAR,
                PAR_TEL_NUMBER VARCHAR, PAR_MOBILE_NUMBER VARCHAR, PAR_FAX_NUMBER VARCHAR,
                PAR_EMAIL_ADDRESS VARCHAR, PAR_WELSH_INDICATOR VARCHAR, PAR_CURRENT_STATUS VARCHAR);

            CREATE TABLE cts_addresses (
                ADR_ID VARCHAR, ADR_LOC_ID VARCHAR, ADR_PAR_ID VARCHAR, ADR_NAME VARCHAR,
                ADR_ADDRESS_2 VARCHAR, ADR_ADDRESS_3 VARCHAR, ADR_ADDRESS_4 VARCHAR,
                ADR_ADDRESS_5 VARCHAR, ADR_POST_CODE VARCHAR, ADR_CURRENT_STATUS VARCHAR);

            CREATE TABLE cts_counties (
                CTY_ID VARCHAR, CTY_CODE VARCHAR, CTY_NAME VARCHAR, CTY_UK_AREA VARCHAR);
            """);

        // CTY_UK_AREA is the country classification: '' (England), 'W', 'S'. 99 is BCMS's dummy.
        Execute(connection, """
            INSERT INTO cts_counties (CTY_ID, CTY_CODE, CTY_NAME, CTY_UK_AREA) VALUES
                ('10', '10', 'Devon',   ''),
                ('08', '08', 'Cumbria', ''),
                ('52', '52', 'Powys',   'W'),
                ('66', '66', 'Aberdeen','S'),
                ('99', '99', 'BCMS',    '');
            """);

        Execute(connection, """
            INSERT INTO cts_locations (LOC_ID, LOC_CTY_ID, LOC_PREMISES_TYPE, LOC_RECEIVE_LABELS_FLAG,
                LOC_EFFECTIVE_FROM, LOC_EFFECTIVE_TO, LOC_CESSATION_REASON, LOC_CURRENT_STATUS,
                LOC_TEL_NUMBER, LOC_MOBILE_NUMBER, LOC_FAX_NUMBER, LOC_EMAIL_ADDRESS)
            VALUES
                -- 1  Plain open holding. Location contact details differ from the keeper's, which is
                --    what the report shows and a common place to wire the wrong column.
                ('1',  '10', 'AH', 'Y', '01-JUL-96', '', '', '1',
                 '01392 000001', '07700 900001', '01392 900001', 'farm1@example.test'),
                -- 2  Three competing current keeper links: latest effective-from must win.
                ('2',  '10', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),
                -- 3  Sub-location. Included, and renders with its -01 suffix.
                ('3',  '10', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),
                -- 4  Two-digit year 66: Oracle RR reads 1966, a naive %y pivot reads 2066 and drops it.
                ('4',  '08', 'AH', 'Y', '22-JUN-66', '', '', '1', '', '', '', ''),
                -- 5  Superseded identifier: one current CPH, one cancelled. Must not fan out.
                ('5',  '08', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),
                -- 6  Correspondence (CA) and contact (CO) links alongside the keeper.
                ('6',  '10', 'LK', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),
                -- 7  Keeper has no address row at all: kept, with empty address columns.
                ('7',  '10', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),
                -- 8  Premises type outside spproc112's AH/LK/TH: still in the report.
                ('8',  '10', 'SG', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),

                -- Excluded, one reason each.
                ('20', '10', 'AH', 'N', '01-JUL-96', '', '', '1', '', '', '', ''),  -- label flag off
                ('21', '10', 'AH', 'Y', '01-JUL-96', '31-DEC-24', 'LC', '1', '', '', '', ''),  -- closed
                ('22', '10', 'AH', 'Y', '01-JUL-96', '', '', '2', '', '', '', ''),  -- cancelled
                ('23', '10', 'AH', 'Y', '30-JUL-30', '', '', '1', '', '', '', ''),  -- not yet effective
                ('24', '10', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- no keeper link
                ('25', '52', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- Wales
                ('26', '66', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- Scotland
                ('27', '99', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- county 99
                ('28', '08', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- AH-08/205/8000
                ('29', '10', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- keeper link ended
                ('30', '10', 'AH', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- keeper link cancelled

                -- County-less separate holdings, classified by the postcode of their own address.
                ('40', '', 'SR', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- EX: England, kept
                ('41', '', 'SR', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- LL: Wales, dropped
                ('42', '', 'SR', 'Y', '01-JUL-96', '', '', '1', '', '', '', ''),  -- PA: Scotland, dropped
                ('43', '', 'SR', 'Y', '01-JUL-96', '', '', '1', '', '', '', '');  -- SH-9999, BCMS
            """);

        Execute(connection, """
            INSERT INTO cts_location_identifiers (LID_ID, LID_LOC_ID, LID_IDENTIFIER,
                LID_SUB_IDENTIFIER, LID_FULL_IDENTIFIER, LID_CURRENT_STATUS)
            VALUES
                ('1',  '1',  '10/001/0001', '',   'AH-10/001/0001',    '1'),
                ('2',  '2',  '10/001/0002', '',   'AH-10/001/0002',    '1'),
                ('3',  '3',  '10/001/0003', '01', 'AH-10/001/0003-01', '1'),
                ('4',  '4',  '08/001/0004', '',   'AH-08/001/0004',    '1'),
                -- Location 5 has held two identifiers. Only the current one may appear.
                ('5',  '5',  '08/001/0005', '',   'AH-08/001/0005',    '1'),
                ('6',  '5',  '08/999/9999', '',   'AH-08/999/9999',    '2'),
                ('7',  '6',  '10/001/0006', '',   'AH-10/001/0006',    '1'),
                ('8',  '7',  '10/001/0007', '',   'AH-10/001/0007',    '1'),
                ('9',  '8',  '10/001/0008', '',   'AH-10/001/0008',    '1'),

                ('20', '20', '10/002/0020', '', 'AH-10/002/0020', '1'),
                ('21', '21', '10/002/0021', '', 'AH-10/002/0021', '1'),
                ('22', '22', '10/002/0022', '', 'AH-10/002/0022', '1'),
                ('23', '23', '10/002/0023', '', 'AH-10/002/0023', '1'),
                ('24', '24', '10/002/0024', '', 'AH-10/002/0024', '1'),
                ('25', '25', '52/002/0025', '', 'AH-52/002/0025', '1'),
                ('26', '26', '66/002/0026', '', 'AH-66/002/0026', '1'),
                ('27', '27', '99/002/0027', '', 'AH-99/002/0027', '1'),
                ('28', '28', '08/205/8000', '', 'AH-08/205/8000', '1'),
                ('29', '29', '10/002/0029', '', 'AH-10/002/0029', '1'),
                ('30', '30', '10/002/0030', '', 'AH-10/002/0030', '1'),

                ('40', '40', '4001', '', 'SH-4001', '1'),
                ('41', '41', '4002', '', 'SH-4002', '1'),
                ('42', '42', '4003', '', 'SH-4003', '1'),
                ('43', '43', '9999', '', 'SH-9999', '1');
            """);

        // LPT 4 = KN keeper name, 5 = CA correspondence, 8 = CO other contact.
        Execute(connection, """
            INSERT INTO cts_location_party_rels (LPR_ID, LPR_LOC_ID, LPR_PAR_ID, LPR_LPT_ID,
                LPR_EFFECTIVE_FROM_DATE, LPR_EFFECTIVE_TO_DATE, LPR_CESSATION_REASON, LPR_CURRENT_STATUS)
            VALUES
                ('1',  '1', 'P1', '4', '01-JUL-96', '', '', '1'),

                -- Location 2: the 2020 link is the latest and must be the one reported.
                ('2',  '2', 'P2', '4', '01-JUL-96', '', '', '1'),
                ('3',  '2', 'P3', '4', '01-JAN-20', '', '', '1'),
                ('4',  '2', 'P4', '4', '01-JAN-10', '', '', '1'),

                ('5',  '3', 'P5', '4', '01-JUL-96', '', '', '1'),
                -- Location 4: 66 must read as 1966, not 2066.
                ('6',  '4', 'P6', '4', '22-JUN-66', '', '', '1'),
                ('7',  '5', 'P7', '4', '01-JUL-96', '', '', '1'),

                -- Location 6 carries all three link types.
                ('8',  '6', 'P8',  '4', '01-JUL-96', '', '', '1'),
                ('9',  '6', 'P9',  '5', '01-JUL-96', '', '', '1'),
                ('10', '6', 'P10', '8', '01-JUL-96', '', '', '1'),

                ('11', '7', 'P11', '4', '01-JUL-96', '', '', '1'),
                ('12', '8', 'P12', '4', '01-JUL-96', '', '', '1'),

                ('20', '20', 'P1', '4', '01-JUL-96', '', '', '1'),
                ('21', '21', 'P1', '4', '01-JUL-96', '', '', '1'),
                ('22', '22', 'P1', '4', '01-JUL-96', '', '', '1'),
                ('23', '23', 'P1', '4', '01-JUL-96', '', '', '1'),
                -- 24 deliberately has no KN link, only a correspondence one.
                ('24', '24', 'P1', '5', '01-JUL-96', '', '', '1'),
                ('25', '25', 'P1', '4', '01-JUL-96', '', '', '1'),
                ('26', '26', 'P1', '4', '01-JUL-96', '', '', '1'),
                ('27', '27', 'P1', '4', '01-JUL-96', '', '', '1'),
                ('28', '28', 'P1', '4', '01-JUL-96', '', '', '1'),
                -- 29's only keeper link ended before the query date; 30's is cancelled.
                ('29', '29', 'P1', '4', '01-JUL-96', '31-DEC-24', '', '1'),
                ('30', '30', 'P1', '4', '01-JUL-96', '', '', '2'),

                ('40', '40', 'P40', '4', '01-JUL-96', '', '', '1'),
                ('41', '41', 'P41', '4', '01-JUL-96', '', '', '1'),
                ('42', '42', 'P42', '4', '01-JUL-96', '', '', '1'),
                ('43', '43', 'P43', '4', '01-JUL-96', '', '', '1');
            """);

        Execute(connection, """
            INSERT INTO cts_parties (PAR_ID, PAR_TITLE, PAR_INITIALS, PAR_SURNAME, PAR_TEL_NUMBER,
                PAR_MOBILE_NUMBER, PAR_FAX_NUMBER, PAR_EMAIL_ADDRESS, PAR_WELSH_INDICATOR, PAR_CURRENT_STATUS)
            VALUES
                ('P1',  'MR',     'A',  'ARCHER',  '01392 111111', '07700 900111', '01392 911111', 'archer@example.test', 'N', '1'),
                ('P2',  'MRS',    'B',  'BAKER',   '', '', '', '', 'N', '1'),
                ('P3',  'MISS',   'C',  'CARTER',  '', '', '', 'carter@example.test', 'N', '1'),
                ('P4',  'DR',     'D',  'DYER',    '', '', '', '', 'N', '1'),
                ('P5',  'MESSRS', 'E',  'ELLIS',   '', '', '', '', 'N', '1'),
                ('P6',  'MR',     'F',  'FLETCHER','', '', '', '', 'N', '1'),
                ('P7',  'MR',     'G',  'GARDNER', '', '', '', '', 'N', '1'),
                ('P8',  'MR',     'H',  'HAYES',   '', '', '', '', 'N', '1'),
                ('P9',  'MS',     'I',  'IRVING',  '01392 999999', '', '', 'irving@example.test', 'Y', '1'),
                ('P10', 'MR',     'J',  'JARVIS',  '', '', '', 'jarvis@example.test', 'N', '1'),
                ('P11', 'MR',     'K',  'KNIGHT',  '', '', '', '', 'N', '1'),
                ('P12', 'MR',     'L',  'LOWE',    '', '', '', '', 'N', '1'),
                ('P40', 'MR',     'M',  'MASON',   '', '', '', '', 'N', '1'),
                ('P41', 'MR',     'N',  'NORRIS',  '', '', '', '', 'Y', '1'),
                ('P42', 'MR',     'O',  'OGILVY',  '', '', '', '', 'N', '1'),
                ('P43', '',       '',   'BCMS',    '', '', '', '', 'N', '1');
            """);

        // A keeper address hangs off ADR_PAR_ID; a location's own address off ADR_LOC_ID.
        // P7 deliberately has none, and location 7 deliberately has none.
        Execute(connection, """
            INSERT INTO cts_addresses (ADR_ID, ADR_LOC_ID, ADR_PAR_ID, ADR_NAME, ADR_ADDRESS_2,
                ADR_ADDRESS_3, ADR_ADDRESS_4, ADR_ADDRESS_5, ADR_POST_CODE, ADR_CURRENT_STATUS)
            VALUES
                ('1',  '', 'P1',  'ARCHER FARM',  'LINE 2', 'LINE 3', 'LINE 4', 'LINE 5', 'EX1 1AA', '1'),
                ('2',  '', 'P3',  'CARTER FARM',  '', '', '', '', 'EX2 2BB', '1'),
                ('3',  '', 'P5',  'ELLIS FARM',   '', '', '', '', 'EX3 3CC', '1'),
                ('4',  '', 'P6',  'FLETCHER FARM','', '', '', '', 'CA1 1AA', '1'),
                ('5',  '', 'P7',  'GARDNER FARM', '', '', '', '', 'CA2 2BB', '1'),
                ('6',  '', 'P8',  'HAYES FARM',   '', '', '', '', 'EX4 4DD', '1'),
                ('7',  '', 'P9',  'IRVING HOUSE', '', '', '', '', 'EX5 5EE', '1'),
                ('8',  '', 'P10', 'JARVIS HOUSE', '', '', '', '', 'EX6 6FF', '1'),
                -- P11 has only a cancelled address, so location 7 reports empty address columns.
                ('9',  '', 'P11', 'OLD KNIGHT FARM', '', '', '', '', 'EX7 7GG', '2'),
                ('10', '', 'P12', 'LOWE FARM',    '', '', '', '', 'EX8 8HH', '1'),
                ('11', '', 'P2',  'BAKER FARM',   '', '', '', '', 'EX9 9II', '1'),
                ('12', '', 'P4',  'DYER FARM',    '', '', '', '', 'EX9 9JJ', '1'),
                ('13', '', 'P40', 'MASON FARM',   '', '', '', '', 'EX10 1AA', '1'),
                ('14', '', 'P41', 'NORRIS FARM',  '', '', '', '', 'LL11 1AA', '1'),
                ('15', '', 'P42', 'OGILVY FARM',  '', '', '', '', 'PA12 1AA', '1'),
                ('16', '', 'P43', 'BCMS',         '', '', '', '', 'CA14 2DD', '1'),

                ('30', '1', '', 'LOCATION ONE', 'LOC 2', 'LOC 3', '', '', 'EX1 1AA', '1'),
                -- The country of a county-less holding is read from its own address postcode.
                ('40', '40', '', 'MASON SHOW',  '', '', '', '', 'EX10 1AA', '1'),
                ('41', '41', '', 'NORRIS SHOW', '', '', '', '', 'LL11 1AA', '1'),
                ('42', '42', '', 'OGILVY SHOW', '', '', '', '', 'PA12 1AA', '1'),
                ('43', '43', '', 'BCMS',        '', '', '', '', 'CA14 2DD', '1');
            """);
    }

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
