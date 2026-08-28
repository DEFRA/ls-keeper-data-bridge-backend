-- SAM extracts -> normalised SQLite read model.
--
-- The caller owns the connection: the SQLite extension is loaded from a vendored path, the source
-- database is attached read-only as "source", and the target is attached as "target". To run this by
-- hand in the duckdb CLI, execute these first:
--
--   LOAD '<path>/sqlite_scanner.duckdb_extension';
--   ATTACH '<path>/staging.duckdb' AS source (READ_ONLY);
--   ATTACH '<path>/krds-db.sqlite' AS target (TYPE sqlite);
--   USE source;
--
-- and afterwards:
--
--   CHECKPOINT target;
--   DETACH target;

DROP TABLE IF EXISTS target.PartyRole;
DROP TABLE IF EXISTS target.HoldingAnimalProfile;
DROP TABLE IF EXISTS target.Herd;
DROP TABLE IF EXISTS target.Holding;
DROP TABLE IF EXISTS target.Party;

CREATE TABLE target.Party (
    Id TEXT PRIMARY KEY,
    SourcePartyId TEXT NOT NULL UNIQUE,
    PersonTitle TEXT,
    GivenName TEXT,
    GivenName2 TEXT,
    Initials TEXT,
    FamilyName TEXT,
    OrganisationName TEXT,
    Telephone TEXT,
    Mobile TEXT,
    Email TEXT,
    Roles TEXT
);

CREATE TABLE target.Holding (
    Id TEXT PRIMARY KEY,
    Cph TEXT NOT NULL UNIQUE,
    FeatureName TEXT,
    CphType TEXT,
    StartDate INTEGER,
    EndDate INTEGER,
    AddressPk TEXT,
    SaonStartNumber TEXT,
    SaonStartNumberSuffix TEXT,
    SaonEndNumber TEXT,
    SaonEndNumberSuffix TEXT,
    SaonDescription TEXT,
    PaonStartNumber TEXT,
    PaonStartNumberSuffix TEXT,
    PaonEndNumber TEXT,
    PaonEndNumberSuffix TEXT,
    PaonDescription TEXT,
    Street TEXT,
    Town TEXT,
    Locality TEXT,
    UkInternalCode TEXT,
    Postcode TEXT,
    CountryCode TEXT,
    Udprn TEXT,
    Easting TEXT,
    Northing TEXT,
    OsMapReference TEXT
);

CREATE TABLE target.Herd (
    Id TEXT PRIMARY KEY,
    HoldingId TEXT NOT NULL,
    Herdmark TEXT NOT NULL,
    Cphh TEXT NOT NULL,
    AnimalSpeciesCode TEXT,
    AnimalPurposeCode TEXT,
    DiseaseType TEXT,
    Intervals TEXT,
    IntervalUnitOfTime TEXT,
    MovementRestrictionReasonCode TEXT,
    AnimalGroupFromDate INTEGER,
    AnimalGroupToDate INTEGER,
    UNIQUE (Herdmark, Cphh)
);

CREATE TABLE target.HoldingAnimalProfile (
    Id TEXT PRIMARY KEY,
    HoldingId TEXT NOT NULL,
    AnimalSpeciesCode TEXT NOT NULL,
    AnimalProductionUsageCode TEXT,
    DiseaseType TEXT,
    Interval TEXT,
    IntervalUnitOfTime TEXT,
    UNIQUE (HoldingId, AnimalSpeciesCode, AnimalProductionUsageCode, DiseaseType, Interval, IntervalUnitOfTime)
);

CREATE TABLE target.PartyRole (
    Id TEXT PRIMARY KEY,
    PartyId TEXT NOT NULL,
    HoldingId TEXT NOT NULL,
    HerdId TEXT,
    Role TEXT NOT NULL CHECK (Role IN ('owner', 'holder', 'keeper')),
    UNIQUE (PartyId, HoldingId, HerdId, Role)
);

-- The source extracts do not carry a database primary key, so IDs are derived
-- from stable source keys. This is SHA-1 name-derived and UUID-shaped, with
-- RFC 4122 version/variant bits set for interoperability.
CREATE OR REPLACE TEMP MACRO source_digest(kind, source_key) AS (
    sha1('sam-read-model-v1|' || kind || '|' || source_key)
);

CREATE OR REPLACE TEMP MACRO source_guid(kind, source_key) AS (
    CASE WHEN source_key IS NULL THEN NULL ELSE
        concat(
            substr(source_digest(kind, source_key), 1, 8), '-',
            substr(source_digest(kind, source_key), 9, 4), '-',
            '5', substr(source_digest(kind, source_key), 14, 3), '-',
            '8', substr(source_digest(kind, source_key), 18, 3), '-',
            substr(source_digest(kind, source_key), 21, 12)
        )
    END
);

CREATE OR REPLACE TEMP MACRO holding_guid(cph) AS (
    source_guid('holding', cph)
);

-- Source extracts use a single hyphen and empty strings as missing-value
-- sentinels. Keep source identifiers separate and normalise descriptive data.
CREATE OR REPLACE TEMP MACRO null_dash(value) AS (
    NULLIF(NULLIF(NULLIF(trim(value), ''), '-'), ',')
);

-- Words the source writes in place of a value. Compared lower-cased because the
-- extracts are inconsistent about capitalisation.
CREATE OR REPLACE TEMP MACRO null_placeholder(value) AS (
    CASE
        WHEN lower(trim(value)) IN ('unknown', 'not known', 'notknown', 'no organisation name')
        THEN NULL
        ELSE null_dash(value)
    END
);

-- DuckDB has no initcap. Word-wise, so a multi-word value keeps every initial.
CREATE OR REPLACE TEMP MACRO title_case(value) AS (
    CASE WHEN null_dash(value) IS NULL THEN NULL ELSE
        array_to_string(
            list_transform(
                string_split(lower(null_dash(value)), ' '),
                word -> CASE WHEN length(word) = 0 THEN word
                             ELSE upper(word[1]) || word[2:] END
            ),
            ' '
        )
    END
);

-- Dates are carried as text and are date-only with no zone, so they are read as
-- UTC to preserve the calendar date. TRY_CAST keeps an unparsable value from
-- failing the run.
CREATE OR REPLACE TEMP MACRO epoch_seconds(value) AS (
    epoch(TRY_CAST(null_dash(value) AS TIMESTAMP))::BIGINT
);

-- Email is looked up, not just displayed, and the source capitalises it inconsistently. The obvious
-- fixes are both unavailable here: DuckDB rejects CREATE INDEX ... COLLATE, and a column-level
-- COLLATE NOCASE is accepted but dropped on the way into SQLite, so the index would silently stay
-- case-sensitive. Folding the stored value keeps ix_party_email usable for an equality match;
-- callers lower-case the term they search for. Matching NOCASE instead costs a full scan.
CREATE OR REPLACE TEMP MACRO normalized_email(value) AS (
    lower(null_dash(value))
);

CREATE OR REPLACE TEMP MACRO valid_cphh(value) AS (
    regexp_matches(value, '^[0-9]{2}/[0-9]{3}/[0-9]{4}/[0-9]{2}$')
);

CREATE OR REPLACE TEMP VIEW normalized_role_party_ids AS
SELECT DISTINCT trim(token) AS PARTY_ID
FROM sam_herd,
     UNNEST(string_split(COALESCE(KEEPER_PARTY_IDS, ''), ',')) AS split(token)
WHERE trim(token) <> ''
UNION
SELECT DISTINCT trim(token) AS PARTY_ID
FROM sam_herd,
     UNNEST(string_split(COALESCE(OWNER_PARTY_IDS, ''), ',')) AS split(token)
WHERE trim(token) <> '';

CREATE OR REPLACE TEMP VIEW normalized_party AS
-- Sentinels are normalised before the fallback, not after. The source ticket coalesced the raw
-- values, which let a '-' in sam_party mask a real name in sam_cph_holder and then normalise away to
-- null - losing data that was present. A sentinel means "absent", so it must never win.
SELECT
    ids.PARTY_ID,
    p.PERSON_TITLE,
    COALESCE(null_dash(p.PERSON_GIVEN_NAME), null_dash(h.PERSON_GIVEN_NAME)) AS PERSON_GIVEN_NAME,
    COALESCE(null_dash(p.PERSON_GIVEN_NAME2), null_dash(h.PERSON_GIVEN_NAME2)) AS PERSON_GIVEN_NAME2,
    COALESCE(null_dash(p.PERSON_INITIALS), null_dash(h.PERSON_INITIALS)) AS PERSON_INITIALS,
    COALESCE(null_dash(p.PERSON_FAMILY_NAME), null_dash(h.PERSON_FAMILY_NAME)) AS PERSON_FAMILY_NAME,
    COALESCE(null_placeholder(p.ORGANISATION_NAME), null_placeholder(h.ORGANISATION_NAME)) AS ORGANISATION_NAME,
    p.TELEPHONE_NUMBER,
    p.MOBILE_NUMBER,
    p.INTERNET_EMAIL_ADDRESS,
    p.ROLES
FROM (
    SELECT PARTY_ID FROM sam_party
    UNION
    SELECT PARTY_ID FROM sam_cph_holder
    UNION
    SELECT PARTY_ID FROM normalized_role_party_ids
) ids
LEFT JOIN sam_party p USING (PARTY_ID)
LEFT JOIN sam_cph_holder h USING (PARTY_ID);

INSERT INTO target.Party
SELECT
    source_guid('party', PARTY_ID),
    PARTY_ID,
    null_dash(PERSON_TITLE),
    null_dash(PERSON_GIVEN_NAME),
    null_dash(PERSON_GIVEN_NAME2),
    null_dash(PERSON_INITIALS),
    null_dash(PERSON_FAMILY_NAME),
    null_dash(ORGANISATION_NAME),
    null_dash(TELEPHONE_NUMBER),
    null_dash(MOBILE_NUMBER),
    normalized_email(INTERNET_EMAIL_ADDRESS),
    null_dash(ROLES)
FROM normalized_party;

-- sam_cph_holdings is the canonical holding population. Relationship extracts may reference CPHs
-- absent from this snapshot; those references are intentionally not materialised below.
CREATE OR REPLACE TEMP VIEW normalized_holding_cph AS
SELECT DISTINCT null_dash(CPH) AS Cph
FROM sam_cph_holdings
WHERE null_dash(CPH) IS NOT NULL;

-- Every source row for a CPH is current: the extract filters out ended records, so a CPH with
-- several rows has several concurrent features rather than a history. FEATURE_ADDRESS_FROM_DATE is
-- therefore a deterministic tie-break, not a currency rule - which is the point, because any_value
-- could return different values between runs for identical input. arg_max skips rows whose argument
-- is null, so each column takes the most recent value it actually has.
--
-- The date alone does not settle it: rows can share one. The row fingerprint breaks those ties, and
-- being row-level rather than value-level it settles every column on the same row. The leading flag
-- keeps an undated row from outranking a dated one, which a bare concatenation would do because '|'
-- sorts above the digits a date starts with.
CREATE OR REPLACE TEMP VIEW holding_source AS
SELECT
    h.*,
    concat(
        CASE WHEN h.FEATURE_ADDRESS_FROM_DATE IS NULL THEN '0' ELSE '1' END,
        COALESCE(h.FEATURE_ADDRESS_FROM_DATE, ''), '|',
        md5(to_json(h)::VARCHAR)
    ) AS record_order
FROM sam_cph_holdings h;

CREATE OR REPLACE TEMP VIEW holding_attributes AS
SELECT
    null_dash(CPH) AS Cph,
    arg_max(null_placeholder(FEATURE_NAME), record_order) AS FeatureName,
    arg_max(lower(null_dash(CPH_TYPE)), record_order) AS CphType,
    max(epoch_seconds(FEATURE_ADDRESS_FROM_DATE)) AS StartDate,
    arg_max(epoch_seconds(FEATURE_ADDRESS_TO_DATE), record_order) AS EndDate,
    arg_max(null_dash(ADDRESS_PK), record_order) AS AddressPk,
    arg_max(null_dash(SAON_START_NUMBER), record_order) AS SaonStartNumber,
    arg_max(null_dash(SAON_START_NUMBER_SUFFIX), record_order) AS SaonStartNumberSuffix,
    arg_max(null_dash(SAON_END_NUMBER), record_order) AS SaonEndNumber,
    arg_max(null_dash(SAON_END_NUMBER_SUFFIX), record_order) AS SaonEndNumberSuffix,
    arg_max(null_dash(SAON_DESCRIPTION), record_order) AS SaonDescription,
    arg_max(null_dash(PAON_START_NUMBER), record_order) AS PaonStartNumber,
    arg_max(null_dash(PAON_START_NUMBER_SUFFIX), record_order) AS PaonStartNumberSuffix,
    arg_max(null_dash(PAON_END_NUMBER), record_order) AS PaonEndNumber,
    arg_max(null_dash(PAON_END_NUMBER_SUFFIX), record_order) AS PaonEndNumberSuffix,
    arg_max(null_dash(PAON_DESCRIPTION), record_order) AS PaonDescription,
    arg_max(null_dash(STREET), record_order) AS Street,
    arg_max(null_dash(TOWN), record_order) AS Town,
    arg_max(null_dash(LOCALITY), record_order) AS Locality,
    arg_max(title_case(UK_INTERNAL_CODE), record_order) AS UkInternalCode,
    arg_max(null_dash(POSTCODE), record_order) AS Postcode,
    arg_max(null_dash(COUNTRY_CODE), record_order) AS CountryCode,
    arg_max(null_dash(UDPRN), record_order) AS Udprn,
    arg_max(null_dash(EASTING), record_order) AS Easting,
    arg_max(null_dash(NORTHING), record_order) AS Northing,
    arg_max(null_dash(OS_MAP_REFERENCE), record_order) AS OsMapReference
FROM holding_source
WHERE null_dash(CPH) IS NOT NULL
GROUP BY null_dash(CPH);

INSERT INTO target.Holding
SELECT
    holding_guid(h.Cph),
    h.Cph,
    a.FeatureName,
    a.CphType,
    a.StartDate,
    a.EndDate,
    a.AddressPk,
    a.SaonStartNumber,
    a.SaonStartNumberSuffix,
    a.SaonEndNumber,
    a.SaonEndNumberSuffix,
    a.SaonDescription,
    a.PaonStartNumber,
    a.PaonStartNumberSuffix,
    a.PaonEndNumber,
    a.PaonEndNumberSuffix,
    a.PaonDescription,
    a.Street,
    a.Town,
    a.Locality,
    a.UkInternalCode,
    a.Postcode,
    a.CountryCode,
    a.Udprn,
    a.Easting,
    a.Northing,
    a.OsMapReference
FROM normalized_holding_cph h
LEFT JOIN holding_attributes a ON a.Cph = h.Cph;

-- Rows sharing a herdmark and CPHH all carry the same dates, the upstream extract having already
-- collapsed them, so there is nothing to order by. min is arbitrary but stable, which is what the
-- ticket asks for where no precedence is defined.
CREATE OR REPLACE TEMP VIEW normalized_herd AS
SELECT
    HERDMARK,
    CPHH,
    left(CPHH, 11) AS Cph,
    min(null_dash(ANIMAL_SPECIES_CODE)) AS AnimalSpeciesCode,
    min(null_dash(ANIMAL_PURPOSE_CODE)) AS AnimalPurposeCode,
    min(null_dash(DISEASE_TYPE)) AS DiseaseType,
    min(null_dash(INTERVALS)) AS Intervals,
    min(null_dash(INTERVAL_UNIT_OF_TIME)) AS IntervalUnitOfTime,
    min(null_dash(MOVEMENT_RSTRCTN_RSN_CODE)) AS MovementRestrictionReasonCode,
    min(epoch_seconds(ANIMAL_GROUP_ID_MCH_FRM_DAT)) AS AnimalGroupFromDate,
    max(epoch_seconds(ANIMAL_GROUP_ID_MCH_TO_DAT)) AS AnimalGroupToDate
FROM sam_herd
WHERE CPHH IS NOT NULL
    AND valid_cphh(CPHH)
GROUP BY HERDMARK, CPHH;

INSERT INTO target.Herd
SELECT
    source_guid('herd', h.HERDMARK || '|' || h.CPHH),
    holding_guid(h.Cph),
    h.HERDMARK,
    h.CPHH,
    h.AnimalSpeciesCode,
    h.AnimalPurposeCode,
    h.DiseaseType,
    h.Intervals,
    h.IntervalUnitOfTime,
    h.MovementRestrictionReasonCode,
    h.AnimalGroupFromDate,
    h.AnimalGroupToDate
FROM normalized_herd h
JOIN normalized_holding_cph holding ON holding.Cph = h.Cph;

CREATE OR REPLACE TEMP VIEW normalized_holding_animal_profile AS
SELECT DISTINCT
    null_dash(CPH) AS Cph,
    null_dash(ANIMAL_SPECIES_CODE) AS AnimalSpeciesCode,
    null_dash(ANIMAL_PRODUCTION_USAGE_CODE) AS AnimalProductionUsageCode,
    null_dash(DISEASE_TYPE) AS DiseaseType,
    null_dash(INTERVAL) AS Interval,
    null_dash(INTERVAL_UNIT_OF_TIME) AS IntervalUnitOfTime
FROM sam_cph_holdings
WHERE null_dash(CPH) IS NOT NULL
  AND null_dash(ANIMAL_SPECIES_CODE) IS NOT NULL;

INSERT INTO target.HoldingAnimalProfile
SELECT
    source_guid(
        'holding-animal-profile',
        profile.Cph || '|' || profile.AnimalSpeciesCode || '|' || COALESCE(profile.AnimalProductionUsageCode, '') || '|' ||
        COALESCE(profile.DiseaseType, '') || '|' || COALESCE(profile.Interval, '') || '|' || COALESCE(profile.IntervalUnitOfTime, '')
    ),
    holding_guid(profile.Cph),
    profile.AnimalSpeciesCode,
    profile.AnimalProductionUsageCode,
    profile.DiseaseType,
    profile.Interval,
    profile.IntervalUnitOfTime
FROM normalized_holding_animal_profile profile
JOIN normalized_holding_cph holding ON holding.Cph = profile.Cph;

CREATE OR REPLACE TEMP VIEW normalized_party_role AS
SELECT DISTINCT
    PARTY_ID,
    trim(token) AS Cph,
    NULL AS HerdId,
    'holder' AS Role
FROM sam_cph_holder,
     UNNEST(string_split(CPHS, ',')) AS split(token)
WHERE CPHS IS NOT NULL AND trim(token) <> ''
UNION
SELECT DISTINCT
    trim(token) AS PARTY_ID,
    left(CPHH, 11) AS Cph,
    source_guid('herd', HERDMARK || '|' || CPHH) AS HerdId,
    'keeper' AS Role
FROM sam_herd,
     UNNEST(string_split(COALESCE(KEEPER_PARTY_IDS, ''), ',')) AS split(token)
WHERE trim(token) <> ''
    AND valid_cphh(CPHH)
UNION
SELECT DISTINCT
    trim(token) AS PARTY_ID,
    left(CPHH, 11) AS Cph,
    source_guid('herd', HERDMARK || '|' || CPHH) AS HerdId,
    'owner' AS Role
FROM sam_herd,
     UNNEST(string_split(COALESCE(OWNER_PARTY_IDS, ''), ',')) AS split(token)
WHERE trim(token) <> ''
    AND valid_cphh(CPHH);

INSERT INTO target.PartyRole
SELECT
    source_guid('party-role', role.PARTY_ID || '|' || role.Cph || '|' || COALESCE(role.HerdId, '') || '|' || role.Role),
    source_guid('party', role.PARTY_ID),
    holding_guid(role.Cph),
    role.HerdId,
    role.Role
FROM normalized_party_role role
JOIN normalized_party party ON party.PARTY_ID = role.PARTY_ID
JOIN normalized_holding_cph holding ON holding.Cph = role.Cph
LEFT JOIN target.Herd herd ON herd.Id = role.HerdId
WHERE role.HerdId IS NULL
    OR herd.Id IS NOT NULL;

CREATE INDEX ix_party_email ON target.main.Party (Email);
CREATE INDEX ix_holding_cph ON target.main.Holding (Cph);
CREATE INDEX ix_herd_holding ON target.main.Herd (HoldingId);
CREATE INDEX ix_party_role_herd_role ON target.main.PartyRole (HerdId, Role);
CREATE INDEX ix_party_role_holding_role ON target.main.PartyRole (HoldingId, Role);
CREATE INDEX ix_party_role_party_role ON target.main.PartyRole (PartyId, Role);
