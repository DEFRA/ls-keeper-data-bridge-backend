-- CTS extracts -> the Open Locations read model table.
--
-- Reproduces the Oracle BI Discoverer report "MI0048 - Open Locations Report" for
-- Country : 'England', from the six CTS tables in the staging database.
--
-- The rule is specified, with its provenance and every contested decision, in
-- open-locations-baseline-spec.md. Measured against a same-day real run of MI0048, this projection
-- reproduces 41,324 of its 41,361 rows exactly across all 53 report columns; of the 37 differences,
-- 35 are staleness in the source extract and 2 are the known limitation in section 4.2 below.
-- Change the rule there first, then here.
--
-- Runs in the same DuckDB command as krds-read-model.sql, so that script's temp macros are in
-- scope. This one defines its own, prefixed cts_, and relies on none of them: the two scripts are
-- independent of each other and of the order they are concatenated in.
--
-- The caller owns the connection, attaches source and target, and supplies the query date. To run
-- this by hand in the duckdb CLI:
--
--   LOAD '<path>/sqlite_scanner.duckdb_extension';
--   ATTACH '<path>/staging.duckdb' AS source (READ_ONLY);
--   ATTACH '<path>/krds-db.sqlite' AS target (TYPE sqlite);
--   USE source;
--   SET VARIABLE cts_query_date = DATE '2026-09-15';

DROP TABLE IF EXISTS target.CtsOpenLocation;

CREATE TABLE target.CtsOpenLocation (
    -- LID_FULL_IDENTIFIER, exactly as the report prints it: AH-01/001/0003, SH-2436,
    -- AH-71/254/7004-01. Unique because it carries the sub-location suffix.
    LocationNumber TEXT PRIMARY KEY,
    -- LID_IDENTIFIER, for joining to Holding.Cph. Deliberately NOT unique: sub-locations of one
    -- holding share their parent's identifier, so two open siblings would collide. Indexed below.
    Cph TEXT,
    LocId TEXT,
    PremisesType TEXT,
    CountyCode TEXT,
    CountyName TEXT,

    KeeperTitle TEXT,
    KeeperInitials TEXT,
    KeeperSurname TEXT,
    KeeperAddressName TEXT,
    KeeperAddress2 TEXT,
    KeeperAddress3 TEXT,
    KeeperAddress4 TEXT,
    KeeperAddress5 TEXT,
    KeeperPostCode TEXT,
    KeeperTelNumber TEXT,
    KeeperMobileNumber TEXT,
    KeeperFaxNumber TEXT,
    KeeperEmailAddress TEXT,
    KeeperWelshIndicator TEXT,

    LocationAddressName TEXT,
    LocationAddress2 TEXT,
    LocationAddress3 TEXT,
    LocationAddress4 TEXT,
    LocationAddress5 TEXT,
    LocationPostCode TEXT,
    LocationTelNumber TEXT,
    LocationMobileNumber TEXT,
    LocationFaxNumber TEXT,
    LocationEmailAddress TEXT,

    CorrespondenceTitle TEXT,
    CorrespondenceInitials TEXT,
    CorrespondenceSurname TEXT,
    CorrespondenceAddressName TEXT,
    CorrespondenceAddress2 TEXT,
    CorrespondenceAddress3 TEXT,
    CorrespondenceAddress4 TEXT,
    CorrespondenceAddress5 TEXT,
    CorrespondencePostCode TEXT,
    CorrespondenceTelNumber TEXT,
    CorrespondenceMobileNumber TEXT,
    CorrespondenceFaxNumber TEXT,
    CorrespondenceEmailAddress TEXT,
    CorrespondenceWelshIndicator TEXT,

    ContactTitle TEXT,
    ContactInitials TEXT,
    ContactSurname TEXT,
    ContactAddressName TEXT,
    ContactAddress2 TEXT,
    ContactAddress3 TEXT,
    ContactAddress4 TEXT,
    ContactAddress5 TEXT,
    ContactPostCode TEXT,
    ContactTelNumber TEXT,
    ContactMobileNumber TEXT,
    ContactFaxNumber TEXT,
    ContactEmailAddress TEXT,
    ContactWelshIndicator TEXT
);

-- The extract carries every column as text and holds Oracle NULLs as empty strings, so restoring
-- null semantics is the caller's job. This form also trims, which is right for a predicate or a
-- join key and wrong for an output column - see cts_value.
CREATE OR REPLACE TEMP MACRO cts_nz(value) AS nullif(trim(value), '');

-- The output form. The report prints an absent value as empty but is otherwise verbatim: a stored
-- surname of ' MUIR' appears with its leading space, and trimming it here would differ from the
-- report on 14 rows. Verified across 41,350 rows x 52 columns that whitespace is the only thing
-- separating a faithful reproduction from a trimmed one.
CREATE OR REPLACE TEMP MACRO cts_value(value) AS nullif(value, '');

-- The as-at date, bound by the caller. Named once so that the transformation and the writer cannot
-- disagree about it silently.
CREATE OR REPLACE TEMP MACRO cts_as_at() AS getvariable('cts_query_date');

-- Dates are text, 'DD-MON-YY'.
CREATE OR REPLACE TEMP MACRO cts_date(value) AS strptime(cts_nz(value), '%d-%b-%y');

-- Oracle's RR pivot reads 50-99 as 19xx; DuckDB's %y reads 00-68 as 20xx, so a stored '66' becomes
-- 2066 and the row falls outside the query date. Nine locations and six keeper links are affected,
-- and because the keeper effective-from is also the ordering key below, getting this wrong picks
-- the wrong keeper as well as dropping rows.
CREATE OR REPLACE TEMP MACRO cts_rr(value) AS
    CASE
        WHEN cts_date(value) > TIMESTAMP '2049-12-31'
        THEN cts_date(value) - INTERVAL 100 YEAR
        ELSE cts_date(value)
    END;

-- The outward code's area letters: 'AB54 8FG' -> 'AB'. Section 4.2.
CREATE OR REPLACE TEMP MACRO cts_pc_area(value) AS
    regexp_extract(upper(coalesce(trim(value), '')), '^([A-Z]{1,2})', 1);

CREATE OR REPLACE TEMP VIEW cts_location AS
SELECT
    LOC_ID,
    cts_nz(LOC_CTY_ID) AS CTY_ID,
    cts_nz(LOC_PREMISES_TYPE) AS PREMISES_TYPE,
    cts_nz(LOC_RECEIVE_LABELS_FLAG) AS RECEIVE_LABELS_FLAG,
    -- Both forms are needed: the raw value distinguishes "no end date" from one that fails to
    -- parse, the parsed value answers whether it has passed.
    cts_nz(LOC_EFFECTIVE_TO) AS EFFECTIVE_TO_RAW,
    cts_rr(LOC_EFFECTIVE_TO) AS EFFECTIVE_TO,
    cts_rr(LOC_EFFECTIVE_FROM) AS EFFECTIVE_FROM,
    cts_nz(LOC_CURRENT_STATUS) AS CURRENT_STATUS,
    LOC_TEL_NUMBER, LOC_MOBILE_NUMBER, LOC_FAX_NUMBER, LOC_EMAIL_ADDRESS
FROM cts_locations;

CREATE OR REPLACE TEMP VIEW cts_identifier AS
SELECT
    LID_LOC_ID,
    cts_nz(LID_IDENTIFIER) AS IDENTIFIER,
    cts_nz(LID_FULL_IDENTIFIER) AS FULL_IDENTIFIER,
    cts_nz(LID_CURRENT_STATUS) AS CURRENT_STATUS
FROM cts_location_identifiers;

CREATE OR REPLACE TEMP VIEW cts_party AS
SELECT
    PAR_ID,
    cts_nz(PAR_CURRENT_STATUS) AS CURRENT_STATUS,
    PAR_TITLE, PAR_INITIALS, PAR_SURNAME,
    PAR_TEL_NUMBER, PAR_MOBILE_NUMBER, PAR_FAX_NUMBER,
    PAR_EMAIL_ADDRESS, PAR_WELSH_INDICATOR
FROM cts_parties;

CREATE OR REPLACE TEMP VIEW cts_county AS
SELECT CTY_ID, cts_nz(CTY_CODE) AS CODE, cts_nz(CTY_NAME) AS NAME, cts_nz(CTY_UK_AREA) AS UK_AREA
FROM cts_counties;

-- One current address per party and per location. Verified to hold on production data but enforced
-- by no constraint, so both are collapsed deterministically rather than trusted: an unnoticed
-- second row would otherwise duplicate a holding and break the table's grain.
CREATE OR REPLACE TEMP VIEW cts_party_address AS
SELECT * EXCLUDE (pick) FROM (
    SELECT
        ADR_PAR_ID, ADR_NAME, ADR_ADDRESS_2, ADR_ADDRESS_3, ADR_ADDRESS_4, ADR_ADDRESS_5,
        ADR_POST_CODE,
        row_number() OVER (PARTITION BY ADR_PAR_ID ORDER BY ADR_ID) AS pick
    FROM cts_addresses
    WHERE cts_nz(ADR_CURRENT_STATUS) = '1'
      AND cts_nz(ADR_PAR_ID) IS NOT NULL
) WHERE pick = 1;

CREATE OR REPLACE TEMP VIEW cts_location_address AS
SELECT * EXCLUDE (pick) FROM (
    SELECT
        ADR_LOC_ID, ADR_NAME, ADR_ADDRESS_2, ADR_ADDRESS_3, ADR_ADDRESS_4, ADR_ADDRESS_5,
        ADR_POST_CODE,
        row_number() OVER (PARTITION BY ADR_LOC_ID ORDER BY ADR_ID) AS pick
    FROM cts_addresses
    WHERE cts_nz(ADR_CURRENT_STATUS) = '1'
      AND cts_nz(ADR_LOC_ID) IS NOT NULL
) WHERE pick = 1;

-- One CTE serves all three link types. LPT_ID 4 (KN, keeper name) decides whether a holding is in
-- the report at all; 5 (CA, correspondence) and 8 (CO, other contact) only decorate it.
--
-- About a third of qualifying holdings carry two to eight concurrent KN links, some with different
-- email addresses, so a rule is needed rather than an arbitrary pick. Latest effective-from agrees
-- with the real report's keeper on 41,349 of 41,350 rows; earliest manages 84%. The LPR_ID
-- tie-break is deterministic but has no business meaning - a data-quality point for BCMS, not one
-- this projection can settle.
CREATE OR REPLACE TEMP VIEW cts_party_link AS
SELECT * EXCLUDE (pick) FROM (
    SELECT
        r.LPR_LOC_ID,
        r.LPR_PAR_ID,
        cts_nz(r.LPR_LPT_ID) AS LPT_ID,
        row_number() OVER (
            PARTITION BY r.LPR_LOC_ID, cts_nz(r.LPR_LPT_ID)
            ORDER BY cts_rr(r.LPR_EFFECTIVE_FROM_DATE) DESC, r.LPR_ID DESC) AS pick
    FROM cts_location_party_rels r
    JOIN cts_party p
      ON p.PAR_ID = r.LPR_PAR_ID
     AND p.CURRENT_STATUS = '1'
    WHERE cts_nz(r.LPR_LPT_ID) IN ('4', '5', '8')
      AND cts_nz(r.LPR_CURRENT_STATUS) = '1'
      AND cts_rr(r.LPR_EFFECTIVE_FROM_DATE) <= cts_as_at()
      AND (cts_nz(r.LPR_EFFECTIVE_TO_DATE) IS NULL
           OR cts_rr(r.LPR_EFFECTIVE_TO_DATE) > cts_as_at())
) WHERE pick = 1;

-- The keeper, correspondence and contact blocks are the same fourteen columns drawn the same way,
-- so they are resolved once here and joined three times rather than written out three times.
CREATE OR REPLACE TEMP VIEW cts_party_block AS
SELECT
    k.LPR_LOC_ID,
    k.LPT_ID,
    p.PAR_TITLE, p.PAR_INITIALS, p.PAR_SURNAME,
    a.ADR_NAME, a.ADR_ADDRESS_2, a.ADR_ADDRESS_3, a.ADR_ADDRESS_4, a.ADR_ADDRESS_5,
    a.ADR_POST_CODE,
    p.PAR_TEL_NUMBER, p.PAR_MOBILE_NUMBER, p.PAR_FAX_NUMBER, p.PAR_EMAIL_ADDRESS,
    p.PAR_WELSH_INDICATOR
FROM cts_party_link k
LEFT JOIN cts_party p
  ON p.PAR_ID = k.LPR_PAR_ID
LEFT JOIN cts_party_address a
  ON a.ADR_PAR_ID = k.LPR_PAR_ID;

-- Which country a holding is in, and its own postal address.
--
-- CTY_UK_AREA answers the question for a holding that has a county: 'S' Scotland, 'W' Wales, null
-- England. 145 holdings - the SH- separate holdings - have no county at all, and the column the
-- live report classifies those by is not in the OLTP. The postcode area of the holding's own
-- address stands in, which is right for 143 of the 145. SY is deliberately absent from the Welsh
-- list: it spans the border, and the data holds both Shropshire and Ceredigion holdings under it.
-- Sections 4.5 and 8.4.
CREATE OR REPLACE TEMP VIEW cts_location_country AS
SELECT
    l.LOC_ID,
    c.CODE AS COUNTY_CODE,
    c.NAME AS COUNTY_NAME,
    -- County 99 is BCMS's dummy. A holding with no county row at all is admissible: the report's
    -- 112 SH- rows have none, and an inner join loses every one of them. Sections 4.4 and 7.1.
    (c.CODE IS NULL OR c.CODE <> '99') AS COUNTY_ADMISSIBLE,
    CASE
        WHEN l.CTY_ID IS NOT NULL THEN c.UK_AREA IS NULL
        ELSE cts_pc_area(a.ADR_POST_CODE) NOT IN (
            'AB', 'DD', 'DG', 'EH', 'FK', 'G', 'HS', 'IV', 'KA', 'KW', 'KY',
            'ML', 'PA', 'PH', 'TD', 'ZE',
            'CF', 'LD', 'LL', 'NP', 'SA')
    END AS IN_ENGLAND,
    a.ADR_NAME, a.ADR_ADDRESS_2, a.ADR_ADDRESS_3, a.ADR_ADDRESS_4, a.ADR_ADDRESS_5,
    a.ADR_POST_CODE
FROM cts_location l
LEFT JOIN cts_county c
  ON c.CTY_ID = l.CTY_ID
LEFT JOIN cts_location_address a
  ON a.ADR_LOC_ID = l.LOC_ID;

-- A holding is open when every one of these holds. Section numbers are the specification's.
--
-- 4.1 open           effective-to null or future, effective-from past, receive-labels flag Y,
--                    current status 1. The label flag is the defining filter, not a detail: it
--                    controls whether BCMS posts ear-tag labels, which is why the report is a
--                    mailing list rather than a register.
-- 4.2 identifiable   exactly one current CPH. Without the current-status test a renumbered holding
--                    returns one row per historical identifier.
-- 4.3 kept           a valid current KN link to a current party. No address is required - that
--                    requirement belonged to two other processes, and imposing it drops 446
--                    holdings the report contains.
-- 4.4 admissible     and 4.5 English: both resolved by cts_location_country above.
--
-- No premises-type filter: the report contains SG, CA, MA and EX holdings and rows with no type.
-- Sub-locations are included. Both of BCMS's own records are excluded by identifier.
CREATE OR REPLACE TEMP VIEW cts_open_location AS
SELECT
    l.LOC_ID,
    i.FULL_IDENTIFIER AS LocationNumber,
    i.IDENTIFIER AS Cph,
    l.PREMISES_TYPE,
    g.COUNTY_CODE,
    g.COUNTY_NAME,
    l.LOC_TEL_NUMBER, l.LOC_MOBILE_NUMBER, l.LOC_FAX_NUMBER, l.LOC_EMAIL_ADDRESS,
    g.ADR_NAME, g.ADR_ADDRESS_2, g.ADR_ADDRESS_3, g.ADR_ADDRESS_4, g.ADR_ADDRESS_5,
    g.ADR_POST_CODE
FROM cts_location l
JOIN cts_identifier i
  ON i.LID_LOC_ID = l.LOC_ID
 AND i.CURRENT_STATUS = '1'
JOIN cts_party_link k
  ON k.LPR_LOC_ID = l.LOC_ID
 AND k.LPT_ID = '4'
JOIN cts_location_country g
  ON g.LOC_ID = l.LOC_ID
WHERE (l.EFFECTIVE_TO_RAW IS NULL OR l.EFFECTIVE_TO > cts_as_at())
  AND l.EFFECTIVE_FROM <= cts_as_at()
  AND l.RECEIVE_LABELS_FLAG = 'Y'
  AND l.CURRENT_STATUS = '1'
  AND g.COUNTY_ADMISSIBLE
  AND g.IN_ENGLAND
  AND i.FULL_IDENTIFIER NOT IN ('AH-08/205/8000', 'SH-9999');

-- Note the asymmetry in the location block: the keeper's telephone and email come from CT_PARTIES,
-- but the location's own come from CT_LOCATIONS. Different tables, and they frequently differ.
INSERT INTO target.CtsOpenLocation
SELECT
    o.LocationNumber,
    o.Cph,
    o.LOC_ID,
    o.PREMISES_TYPE,
    o.COUNTY_CODE,
    o.COUNTY_NAME,

    cts_value(keeper.PAR_TITLE),
    cts_value(keeper.PAR_INITIALS),
    cts_value(keeper.PAR_SURNAME),
    cts_value(keeper.ADR_NAME),
    cts_value(keeper.ADR_ADDRESS_2),
    cts_value(keeper.ADR_ADDRESS_3),
    cts_value(keeper.ADR_ADDRESS_4),
    cts_value(keeper.ADR_ADDRESS_5),
    cts_value(keeper.ADR_POST_CODE),
    cts_value(keeper.PAR_TEL_NUMBER),
    cts_value(keeper.PAR_MOBILE_NUMBER),
    cts_value(keeper.PAR_FAX_NUMBER),
    cts_value(keeper.PAR_EMAIL_ADDRESS),
    cts_value(keeper.PAR_WELSH_INDICATOR),

    cts_value(o.ADR_NAME),
    cts_value(o.ADR_ADDRESS_2),
    cts_value(o.ADR_ADDRESS_3),
    cts_value(o.ADR_ADDRESS_4),
    cts_value(o.ADR_ADDRESS_5),
    cts_value(o.ADR_POST_CODE),
    cts_value(o.LOC_TEL_NUMBER),
    cts_value(o.LOC_MOBILE_NUMBER),
    cts_value(o.LOC_FAX_NUMBER),
    cts_value(o.LOC_EMAIL_ADDRESS),

    cts_value(corres.PAR_TITLE),
    cts_value(corres.PAR_INITIALS),
    cts_value(corres.PAR_SURNAME),
    cts_value(corres.ADR_NAME),
    cts_value(corres.ADR_ADDRESS_2),
    cts_value(corres.ADR_ADDRESS_3),
    cts_value(corres.ADR_ADDRESS_4),
    cts_value(corres.ADR_ADDRESS_5),
    cts_value(corres.ADR_POST_CODE),
    cts_value(corres.PAR_TEL_NUMBER),
    cts_value(corres.PAR_MOBILE_NUMBER),
    cts_value(corres.PAR_FAX_NUMBER),
    cts_value(corres.PAR_EMAIL_ADDRESS),
    cts_value(corres.PAR_WELSH_INDICATOR),

    cts_value(contact.PAR_TITLE),
    cts_value(contact.PAR_INITIALS),
    cts_value(contact.PAR_SURNAME),
    cts_value(contact.ADR_NAME),
    cts_value(contact.ADR_ADDRESS_2),
    cts_value(contact.ADR_ADDRESS_3),
    cts_value(contact.ADR_ADDRESS_4),
    cts_value(contact.ADR_ADDRESS_5),
    cts_value(contact.ADR_POST_CODE),
    cts_value(contact.PAR_TEL_NUMBER),
    cts_value(contact.PAR_MOBILE_NUMBER),
    cts_value(contact.PAR_FAX_NUMBER),
    cts_value(contact.PAR_EMAIL_ADDRESS),
    cts_value(contact.PAR_WELSH_INDICATOR)
FROM cts_open_location o
LEFT JOIN cts_party_block keeper
  ON keeper.LPR_LOC_ID = o.LOC_ID AND keeper.LPT_ID = '4'
LEFT JOIN cts_party_block corres
  ON corres.LPR_LOC_ID = o.LOC_ID AND corres.LPT_ID = '5'
LEFT JOIN cts_party_block contact
  ON contact.LPR_LOC_ID = o.LOC_ID AND contact.LPT_ID = '8';

CREATE INDEX ix_cts_open_location_cph ON target.main.CtsOpenLocation (Cph);
CREATE INDEX ix_cts_open_location_keeper_post_code ON target.main.CtsOpenLocation (KeeperPostCode);
CREATE INDEX ix_cts_open_location_keeper_email ON target.main.CtsOpenLocation (KeeperEmailAddress);
