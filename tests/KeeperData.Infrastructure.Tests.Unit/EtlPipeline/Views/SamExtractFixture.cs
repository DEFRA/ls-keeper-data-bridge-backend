using DuckDB.NET.Data;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>A small SAM extract shaped to exercise the transformation's rules: missing-value
/// sentinels, comma-delimited relationship tokens, duplicate source rows, an invalid CPHH, and
/// relationships pointing at holdings and herds that are not in the canonical population.</summary>
public static class SamExtractFixture
{
    public static void Create(string databasePath)
    {
        using var connection = new DuckDBConnection($"Data Source={databasePath}");
        connection.Open();

        Execute(connection, """
            CREATE TABLE sam_cph_holdings (
                CPH VARCHAR, FEATURE_NAME VARCHAR, CPH_TYPE VARCHAR, ADDRESS_PK VARCHAR,
                SAON_START_NUMBER VARCHAR, SAON_START_NUMBER_SUFFIX VARCHAR, SAON_END_NUMBER VARCHAR,
                SAON_END_NUMBER_SUFFIX VARCHAR, SAON_DESCRIPTION VARCHAR, PAON_START_NUMBER VARCHAR,
                PAON_START_NUMBER_SUFFIX VARCHAR, PAON_END_NUMBER VARCHAR, PAON_END_NUMBER_SUFFIX VARCHAR,
                PAON_DESCRIPTION VARCHAR, STREET VARCHAR, TOWN VARCHAR, LOCALITY VARCHAR,
                UK_INTERNAL_CODE VARCHAR, POSTCODE VARCHAR, COUNTRY_CODE VARCHAR, UDPRN VARCHAR,
                EASTING VARCHAR, NORTHING VARCHAR, OS_MAP_REFERENCE VARCHAR, DISEASE_TYPE VARCHAR,
                INTERVAL VARCHAR, INTERVAL_UNIT_OF_TIME VARCHAR, ANIMAL_SPECIES_CODE VARCHAR,
                ANIMAL_PRODUCTION_USAGE_CODE VARCHAR);

            CREATE TABLE sam_party (
                PARTY_ID VARCHAR, PERSON_TITLE VARCHAR, PERSON_GIVEN_NAME VARCHAR, PERSON_GIVEN_NAME2 VARCHAR,
                PERSON_INITIALS VARCHAR, PERSON_FAMILY_NAME VARCHAR, ORGANISATION_NAME VARCHAR,
                TELEPHONE_NUMBER VARCHAR, MOBILE_NUMBER VARCHAR, INTERNET_EMAIL_ADDRESS VARCHAR, ROLES VARCHAR);

            CREATE TABLE sam_cph_holder (
                PARTY_ID VARCHAR, PERSON_GIVEN_NAME VARCHAR, PERSON_GIVEN_NAME2 VARCHAR, PERSON_INITIALS VARCHAR,
                PERSON_FAMILY_NAME VARCHAR, ORGANISATION_NAME VARCHAR, CPHS VARCHAR);

            CREATE TABLE sam_herd (
                HERDMARK VARCHAR, CPHH VARCHAR, KEEPER_PARTY_IDS VARCHAR, OWNER_PARTY_IDS VARCHAR,
                ANIMAL_SPECIES_CODE VARCHAR, ANIMAL_PURPOSE_CODE VARCHAR, DISEASE_TYPE VARCHAR,
                INTERVALS VARCHAR, INTERVAL_UNIT_OF_TIME VARCHAR, MOVEMENT_RSTRCTN_RSN_CODE VARCHAR,
                ANIMAL_GROUP_ID_MCH_FRM_DAT VARCHAR, ANIMAL_GROUP_ID_MCH_TO_DAT VARCHAR);
            """);

        Execute(connection, """
            INSERT INTO sam_cph_holdings (CPH, FEATURE_NAME, CPH_TYPE, TOWN, POSTCODE,
                ANIMAL_SPECIES_CODE, ANIMAL_PRODUCTION_USAGE_CODE, DISEASE_TYPE, INTERVAL, INTERVAL_UNIT_OF_TIME)
            VALUES
                -- Different missing-value sentinels represent the same profile and must collapse.
                ('01/234/5678', 'Main Farm', 'MAIN', 'Exeter', 'EX1 1AA', '01', '-', '', ',', 'M'),
                ('01/234/5678', 'Main Farm', 'MAIN', 'Exeter', 'EX1 1AA', '01', NULL, '-', NULL, ' M '),
                ('02/345/6789', '-', 'MAIN', 'Truro', '', NULL, NULL, NULL, NULL, NULL),
                ('  03/456/7890  ', 'Spaced Farm', 'MAIN', 'Bodmin', 'PL31 1AA', '02', 'BEEF', NULL, NULL, NULL),
                ('-', 'Sentinel Only', 'MAIN', 'Nowhere', NULL, NULL, NULL, NULL, NULL, NULL);

            INSERT INTO sam_party (PARTY_ID, PERSON_TITLE, PERSON_GIVEN_NAME, PERSON_FAMILY_NAME,
                TELEPHONE_NUMBER, INTERNET_EMAIL_ADDRESS, ROLES)
            VALUES
                ('P1', 'Mr', 'Alan', 'Archer', '01392 000001', 'alan@example.test', 'keeper,owner'),
                -- P2 is also a holder, so its sentinels must give way to the holder's real names.
                ('P2', '-', '-', '-', ',', '', ''),
                ('P3', 'Ms', 'Carol', 'Cooper', NULL, NULL, NULL),
                -- P6 exists nowhere else, so nothing can fill in what its sentinels stand for.
                ('P6', '-', '', ',', '-', '   ', '-');

            INSERT INTO sam_cph_holder (PARTY_ID, PERSON_GIVEN_NAME, PERSON_FAMILY_NAME, CPHS)
            VALUES
                ('P2', 'Brenda', 'Baker', '01/234/5678, 02/345/6789'),
                ('P3', 'Carol', 'Cooper', '99/999/9999'),
                ('P4', 'Derek', 'Dunn', NULL);

            INSERT INTO sam_herd (HERDMARK, CPHH, KEEPER_PARTY_IDS, OWNER_PARTY_IDS, ANIMAL_SPECIES_CODE, ANIMAL_PURPOSE_CODE)
            VALUES
                ('AB1234', '01/234/5678/01', 'P1, P5', 'P1', '01', 'DAIRY'),
                ('CD5678', 'NOT-A-CPHH', 'P1', 'P1', '01', 'BEEF'),
                ('EF9012', '77/777/7777/01', 'P1', 'P1', '01', 'BEEF');
            """);
    }

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
