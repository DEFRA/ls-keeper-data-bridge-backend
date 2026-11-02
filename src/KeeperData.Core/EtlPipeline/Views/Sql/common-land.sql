-- AMLS2 common land extract -> the common land read model tables.
--
-- Two grains, because the extract carries two kinds of fact:
--
--   CommonLand         one row per COMMON_CPH - the common itself: its name,
--                      address, local authority and grid reference. Properties
--                      of the land, not of anyone's use of it.
--   CommonLandHolding  one row per MAIN_CPH / COMMON_CPH link - a keeper's
--                      registration to use a common: the contiguity flag and
--                      the link's dates. This is the join the KRDS sites
--                      endpoint walks in both directions - a common's CPH to
--                      its associatedMainHoldings, a holding's CPH to its
--                      associatedCommonLands (LKPR-18).
--
-- A link row whose MAIN_CPH is unusable still contributes its COMMON_CPH to
-- CommonLand: a common with no current keeper is still a site. A row with no
-- usable COMMON_CPH contributes nothing at all.
--
-- The extract's full column list is not yet confirmed against a sample file, so
-- every column beyond the two keys is read tolerantly: the UNION ALL BY NAME
-- below supplies NULL for each candidate name the feed does not carry, and each
-- output field coalesces across the names it might arrive under. A candidate
-- the real feed does not use lands NULL rather than failing to bind. MAIN_CPH
-- and COMMON_CPH are the declared primary key - they are referenced bare, so a
-- feed that stops carrying either fails the part rather than emitting links to
-- nowhere.
--
-- Runs on the same DuckDB connection as the other parts, defines its own
-- cl_-prefixed macros and relies on none of theirs.
--
-- Skipped entirely where the staging database holds no amls2_common_land table:
-- an environment the feed has not reached exports a read model without these
-- tables rather than failing. Declared in SqliteViewDefinition - a new table
-- read here has to be added there too.
--
-- The caller owns the connection: the SQLite extension is loaded, the source
-- database is attached read-only as "source", and the target is attached as
-- "target". To run this by hand in the duckdb CLI:
--
--   LOAD '<path>/sqlite_scanner.duckdb_extension';
--   ATTACH '<path>/staging.duckdb' AS source (READ_ONLY);
--   ATTACH '<path>/krds-db.sqlite' AS target (TYPE sqlite);
--   USE source;

DROP TABLE IF EXISTS target.CommonLandHolding;
DROP TABLE IF EXISTS target.CommonLand;

CREATE TABLE target.CommonLand (
    Id TEXT PRIMARY KEY,
    -- The common's 00-series CPH. UNIQUE: the join from CommonLandHolding and
    -- the sites lookup both land here.
    CommonCph TEXT NOT NULL UNIQUE,
    -- AMLS's own identifier for the registered common, distinct from the CPH
    -- the RPA allocates for movement reporting.
    PremiseId INTEGER,
    -- What the land is registered as: 'Common Land' on every row so far, but
    -- it is the column that would carry a quasi-common usage were one loaded.
    BusinessUsage TEXT,
    PremisesName TEXT,
    AddressLine1 TEXT,
    AddressLine2 TEXT,
    AddressLine3 TEXT,
    LocalAuthorityName TEXT,
    Country TEXT,
    Postcode TEXT,
    Easting INTEGER,
    Northing INTEGER
);

CREATE TABLE target.CommonLandHolding (
    Id TEXT PRIMARY KEY,
    -- The CommonLand row this link belongs to. Derived identically to that
    -- table's Id, so it always resolves - a link can only exist where its
    -- common does.
    CommonLandId TEXT NOT NULL,
    -- The keeper's holding CPH, verbatim: '17/050/0003'. A join key, not a
    -- foreign key - the holding may not be in the SAM extract (the convention
    -- CtsOpenLocation.Cph follows for the same reason).
    MainCph TEXT NOT NULL,
    CommonCph TEXT NOT NULL,
    -- 1 contiguous (livestock walk directly onto the common), 0 non-contiguous
    -- (livestock are transported), NULL where the extract does not say.
    IsContiguous INTEGER,
    -- The registration's start and end, as epoch seconds like the other tables'
    -- dates. NULL where the extract does not carry them.
    StartDate INTEGER,
    EndDate INTEGER,
    -- The extract's own link identifier, where it carries one.
    LinkId TEXT
);

-- See krds-read-model.sql for why the _txt layer exists: optimise writes typed
-- parquet, so a staging column may arrive as BIGINT or DATE rather than VARCHAR,
-- and the semantics below see the same text whatever it carried.
CREATE OR REPLACE TEMP VIEW amls2_common_land_txt AS
SELECT COLUMNS(*)::VARCHAR FROM amls2_common_land;

-- The extract may omit a column entirely rather than carrying it as blanks.
-- UNION ALL BY NAME against an empty row supplies every candidate column the
-- projection asks for, NULL where the extract does not. The keys are
-- deliberately not supplied: an extract with no key is not common land data,
-- and the part should fail on that rather than emit an empty population.
CREATE OR REPLACE TEMP VIEW amls2_common_land_attributed AS
SELECT *
FROM amls2_common_land_txt
UNION ALL BY NAME
SELECT
    NULL AS CONTIGUOUS_FLAG,
    NULL AS IS_CONTIGUOUS,
    NULL AS CONTIGOUS_FLAG,
    NULL AS CONTIGUOUS,
    NULL AS START_DATE,
    NULL AS END_DATE,
    NULL AS EFFECTIVE_FROM_DATE,
    NULL AS EFFECTIVE_TO_DATE,
    NULL AS LINK_ID,
    NULL AS CONTIGUOUS_COMMON,
    NULL AS COMMON_LAND_PREMISE_ID,
    NULL AS BUSINESS_USAGE,
    NULL AS PREMISES_NAME,
    NULL AS COMMON_NAME,
    NULL AS ADDRESS_LINE_1,
    NULL AS ADDRESS_LINE_2,
    NULL AS ADDRESS_LINE_3,
    NULL AS ADDRESS_LINE1,
    NULL AS ADDRESS_LINE2,
    NULL AS ADDRESS_LINE3,
    NULL AS LOCAL_AUTH_NAME,
    NULL AS LOCAL_AUTHORITY_NAME,
    NULL AS COUNTRY,
    NULL AS POSTCODE,
    NULL AS POST_CODE,
    NULL AS EASTING,
    NULL AS NORTHING
WHERE FALSE;

-- The litprd extracts write a hyphen and empty strings as missing-value
-- sentinels. A sentinel means absent, so it is normalised before the
-- candidate-column fallback rather than after - a blank in CONTIGUOUS_FLAG must
-- not shadow a real value in IS_CONTIGUOUS.
CREATE OR REPLACE TEMP MACRO cl_nz(value) AS (
    NULLIF(NULLIF(NULLIF(trim(value), ''), '-'), ',')
);

-- The flag's written form is unconfirmed, so every plausible truthy and falsy
-- spelling maps to 1 and 0; anything else, including a sentinel, lands NULL.
CREATE OR REPLACE TEMP MACRO cl_flag(value) AS (
    CASE lower(trim(value))
        WHEN 'y' THEN 1 WHEN 'yes' THEN 1 WHEN 'true' THEN 1 WHEN 't' THEN 1 WHEN '1' THEN 1
        WHEN 'n' THEN 0 WHEN 'no' THEN 0 WHEN 'false' THEN 0 WHEN 'f' THEN 0 WHEN '0' THEN 0
        ELSE NULL
    END
);

-- Dates reach the snapshot typed where optimise detected them, or as text. The
-- text form is unconfirmed: the observed extract carries ISO-parseable values,
-- but the review xlsx converts them to serial numbers, so a bare 5-6 digit
-- value in the plausible registration window (20000..60000 days, 1954-2064) is
-- read as a serial too. Anything else lands NULL rather than failing the run.
CREATE OR REPLACE TEMP MACRO cl_day(value) AS (
    COALESCE(
        TRY_CAST(cl_nz(value) AS DATE),
        CASE WHEN cl_nz(value) IS NOT NULL
               AND regexp_matches(cl_nz(value), '^\d+$')
               AND CAST(cl_nz(value) AS INTEGER) BETWEEN 20000 AND 60000
             THEN DATE '1899-12-30' + CAST(cl_nz(value) AS INTEGER)
             ELSE NULL END
    )
);

-- Dates come back as epoch seconds, like the other tables' dates.
CREATE OR REPLACE TEMP MACRO cl_epoch(value) AS (
    epoch(cl_day(value))::BIGINT
);

-- AMLS writes 31-DEC-2999 for a link with no end. It is a sentinel, not a date:
-- a consumer asking "is this link live" should see NULL, not a year-3000
-- timestamp that a date comparison would mishandle anyway.
CREATE OR REPLACE TEMP MACRO cl_end(value) AS (
    CASE WHEN cl_day(value) IS NULL OR cl_day(value) >= DATE '2999-01-01'
         THEN NULL
         ELSE epoch(cl_day(value))::BIGINT
    END
);

-- The coordinate cast goes via DOUBLE because a DOUBLE staging column renders
-- '300000.0', which BIGINT's parser rejects.
CREATE OR REPLACE TEMP MACRO cl_number(value) AS (
    TRY_CAST(TRY_CAST(cl_nz(value) AS DOUBLE) AS BIGINT)
);

-- Deterministic ids, SHA-1 name-derived and UUID-shaped as in krds-read-model,
-- under this part's own namespace so its ids can never collide with a SAM one.
-- Site and link are namespaced separately so a site guid is stable however many
-- links point at it.
CREATE OR REPLACE TEMP MACRO cl_digest(source_key) AS (
    sha1('amls2-common-land-v1|' || source_key)
);

CREATE OR REPLACE TEMP MACRO cl_uuid(source_key) AS (
    concat(
        substr(cl_digest(source_key), 1, 8), '-',
        substr(cl_digest(source_key), 9, 4), '-',
        '5', substr(cl_digest(source_key), 14, 3), '-',
        '8', substr(cl_digest(source_key), 18, 3), '-',
        substr(cl_digest(source_key), 21, 12)
    )
);

CREATE OR REPLACE TEMP MACRO cl_site_guid(common_cph) AS (
    cl_uuid('site|' || common_cph)
);

CREATE OR REPLACE TEMP MACRO cl_link_guid(main_cph, common_cph) AS (
    cl_uuid('link|' || main_cph || '|' || common_cph)
);

-- Every candidate column resolved once: the link fields the relationship table
-- wants and the site fields the common table wants, normalised together so the
-- two populations below never read the extract differently.
CREATE OR REPLACE TEMP VIEW cl_row AS
SELECT
    trim(MAIN_CPH) AS MainCph,
    trim(COMMON_CPH) AS CommonCph,
    cl_flag(COALESCE(
        cl_nz(CONTIGUOUS_COMMON), cl_nz(CONTIGUOUS_FLAG), cl_nz(IS_CONTIGUOUS),
        cl_nz(CONTIGOUS_FLAG), cl_nz(CONTIGUOUS))) AS IsContiguous,
    cl_epoch(COALESCE(cl_nz(START_DATE), cl_nz(EFFECTIVE_FROM_DATE))) AS StartDate,
    cl_end(COALESCE(cl_nz(END_DATE), cl_nz(EFFECTIVE_TO_DATE))) AS EndDate,
    -- '-1' is the feed's no-link sentinel, a '-' with an id's shape.
    NULLIF(cl_nz(LINK_ID), '-1') AS LinkId,
    cl_number(COMMON_LAND_PREMISE_ID) AS PremiseId,
    cl_nz(BUSINESS_USAGE) AS BusinessUsage,
    cl_nz(COALESCE(cl_nz(PREMISES_NAME), cl_nz(COMMON_NAME))) AS PremisesName,
    cl_nz(COALESCE(cl_nz(ADDRESS_LINE_1), cl_nz(ADDRESS_LINE1))) AS AddressLine1,
    cl_nz(COALESCE(cl_nz(ADDRESS_LINE_2), cl_nz(ADDRESS_LINE2))) AS AddressLine2,
    cl_nz(COALESCE(cl_nz(ADDRESS_LINE_3), cl_nz(ADDRESS_LINE3))) AS AddressLine3,
    cl_nz(COALESCE(cl_nz(LOCAL_AUTH_NAME), cl_nz(LOCAL_AUTHORITY_NAME))) AS LocalAuthorityName,
    cl_nz(COUNTRY) AS Country,
    cl_nz(COALESCE(cl_nz(POSTCODE), cl_nz(POST_CODE))) AS Postcode,
    cl_number(EASTING) AS Easting,
    cl_number(NORTHING) AS Northing
FROM amls2_common_land_attributed
WHERE cl_nz(COMMON_CPH) IS NOT NULL;

-- The common's attributes repeat on every link row and need not agree. Latest
-- start wins; where starts tie or are absent the row fingerprint decides, and
-- being row-level it settles every column on the same row - the same scheme
-- holding_attributes uses. arg_max skips a null argument, so a column takes
-- the most recent value it actually has.
CREATE OR REPLACE TEMP VIEW cl_common_source AS
SELECT
    r.*,
    concat(
        CASE WHEN r.StartDate IS NULL THEN '0' ELSE '1' END,
        COALESCE(CAST(r.StartDate AS VARCHAR), ''), '|',
        md5(to_json(r)::VARCHAR)
    ) AS record_order
FROM cl_row r;

CREATE OR REPLACE TEMP VIEW cl_common AS
SELECT
    cl_site_guid(CommonCph) AS Id,
    CommonCph,
    arg_max(PremiseId, record_order) AS PremiseId,
    arg_max(BusinessUsage, record_order) AS BusinessUsage,
    arg_max(PremisesName, record_order) AS PremisesName,
    arg_max(AddressLine1, record_order) AS AddressLine1,
    arg_max(AddressLine2, record_order) AS AddressLine2,
    arg_max(AddressLine3, record_order) AS AddressLine3,
    arg_max(LocalAuthorityName, record_order) AS LocalAuthorityName,
    arg_max(Country, record_order) AS Country,
    arg_max(Postcode, record_order) AS Postcode,
    arg_max(Easting, record_order) AS Easting,
    arg_max(Northing, record_order) AS Northing
FROM cl_common_source
GROUP BY CommonCph;

INSERT INTO target.CommonLand
SELECT Id, CommonCph, PremiseId, BusinessUsage, PremisesName, AddressLine1,
       AddressLine2, AddressLine3, LocalAuthorityName, Country, Postcode,
       Easting, Northing
FROM cl_common;

-- One row per link. The composite key should already be unique - the delta
-- merge keys on it - so the ordering only settles pathological duplicates: the
-- latest start wins, and LinkId breaks a tie deterministically rather than by
-- physical order.
CREATE OR REPLACE TEMP VIEW cl_link AS
SELECT * EXCLUDE (pick) FROM (
    SELECT
        cl_link_guid(MainCph, CommonCph) AS Id,
        cl_site_guid(CommonCph) AS CommonLandId,
        MainCph, CommonCph, IsContiguous, StartDate, EndDate, LinkId,
        row_number() OVER (
            PARTITION BY MainCph, CommonCph
            ORDER BY StartDate DESC NULLS LAST,
                     LinkId DESC NULLS LAST
        ) AS pick
    FROM cl_row
    WHERE cl_nz(MainCph) IS NOT NULL
) WHERE pick = 1;

INSERT INTO target.CommonLandHolding
SELECT Id, CommonLandId, MainCph, CommonCph, IsContiguous, StartDate, EndDate, LinkId
FROM cl_link;

CREATE INDEX ix_common_land_holding_main_cph ON target.main.CommonLandHolding (MainCph);
CREATE INDEX ix_common_land_holding_common_cph ON target.main.CommonLandHolding (CommonCph);
CREATE INDEX ix_common_land_holding_common_land_id ON target.main.CommonLandHolding (CommonLandId);
