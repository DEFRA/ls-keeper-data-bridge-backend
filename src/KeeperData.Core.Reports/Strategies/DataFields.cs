namespace KeeperData.Core.Reports.Strategies;

/// <summary>
/// Common field names used across CTS and SAM data sets.
/// </summary>
public static class DataFields
{
    public const string IsDeleted = "IsDeleted";

    public static class SamHerd
    {
        public const string AnimalGroupIdMchFromDate = "ANIMAL_GROUP_ID_MCH_FRM_DAT";
        public const string AnimalGroupIdMchToDate = "ANIMAL_GROUP_ID_MCH_TO_DAT";
        public const string AnimalPurposeCode = "ANIMAL_PURPOSE_CODE";
        public const string AnimalSpeciesCode = "ANIMAL_SPECIES_CODE";
        public const string BatchId = "BATCH_ID";
        public const string ChangeType = "CHANGE_TYPE";
        public const string Cphh = "CPHH";
        public const string CreatedAtUtc = "CreatedAtUtc";
        public const string DiseaseType = "DISEASE_TYPE";
        public const string Herdmark = "HERDMARK";
        public const string Intervals = "INTERVALS";
        public const string IntervalUnitOfTime = "INTERVAL_UNIT_OF_TIME";
        public const string KeeperPartyIds = "KEEPER_PARTY_IDS";
        public const string MovementRestrictionReasonCode = "MOVEMENT_RSTRCTN_RSN_CODE";
        public const string OwnerPartyIds = "OWNER_PARTY_IDS";
        public const string UpdatedAtUtc = "UpdatedAtUtc";
    }

    public static class CtsCphHoldingFields
    {
        public const string AdrAddress2 = "ADR_ADDRESS_2";
        public const string AdrAddress3 = "ADR_ADDRESS_3";
        public const string AdrAddress4 = "ADR_ADDRESS_4";
        public const string AdrAddress5 = "ADR_ADDRESS_5";
        public const string AdrName = "ADR_NAME";
        public const string AdrPostCode = "ADR_POST_CODE";
        public const string BatchId = "BATCH_ID";
        public const string ChangeType = "CHANGE_TYPE";
        public const string CreatedAtUtc = "CreatedAtUtc";
        public const string LidFullIdentifier = "LID_FULL_IDENTIFIER";
        public const string LocEffectiveFrom = "LOC_EFFECTIVE_FROM";
        public const string LocEffectiveTo = "LOC_EFFECTIVE_TO";
        public const string LocMapReference = "LOC_MAP_REFERENCE";
        public const string LocMobileNumber = "LOC_MOBILE_NUMBER";
        public const string LocTelNumber = "LOC_TEL_NUMBER";
        public const string LtyLocType = "LTY_LOC_TYPE";
        public const string UpdatedAtUtc = "UpdatedAtUtc";
    }

    public static class CtsKeeperFields
    {
        public const string AdrAddress2 = "ADR_ADDRESS_2";
        public const string AdrAddress3 = "ADR_ADDRESS_3";
        public const string AdrAddress4 = "ADR_ADDRESS_4";
        public const string AdrAddress5 = "ADR_ADDRESS_5";
        public const string AdrName = "ADR_NAME";
        public const string AdrPostCode = "ADR_POST_CODE";
        public const string BatchId = "BATCH_ID";
        public const string ChangeType = "CHANGE_TYPE";
        public const string CreatedAtUtc = "CreatedAtUtc";
        public const string LidFullIdentifier = "LID_FULL_IDENTIFIER";
        public const string LprEffectiveFromDate = "LPR_EFFECTIVE_FROM_DATE";
        public const string LprEffectiveToDate = "LPR_EFFECTIVE_TO_DATE";
        public const string ParEmailAddress = "PAR_EMAIL_ADDRESS";
        public const string ParId = "PAR_ID";
        public const string ParInitials = "PAR_INITIALS";
        public const string ParMobileNumber = "PAR_MOBILE_NUMBER";
        public const string ParSurname = "PAR_SURNAME";
        public const string ParTelNumber = "PAR_TEL_NUMBER";
        public const string ParTitle = "PAR_TITLE";
        public const string UpdatedAtUtc = "UpdatedAtUtc";
    }

    public static class SamPartyFields
    {
        public const string BatchId = "BATCH_ID";
        public const string ChangeType = "CHANGE_TYPE";
        public const string CountryCode = "COUNTRY_CODE";
        public const string CreatedAtUtc = "CreatedAtUtc";
        public const string InternetEmailAddress = "INTERNET_EMAIL_ADDRESS";
        public const string Locality = "LOCALITY";
        public const string MobileNumber = "MOBILE_NUMBER";
        public const string OrganisationName = "ORGANISATION_NAME";
        public const string PaonDescription = "PAON_DESCRIPTION";
        public const string PaonEndNumber = "PAON_END_NUMBER";
        public const string PaonEndNumberSuffix = "PAON_END_NUMBER_SUFFIX";
        public const string PaonStartNumber = "PAON_START_NUMBER";
        public const string PaonStartNumberSuffix = "PAON_START_NUMBER_SUFFIX";
        public const string PartyId = "PARTY_ID";
        public const string PartyRoleFromDate = "PARTY_ROLE_FROM_DATE";
        public const string PartyRoleToDate = "PARTY_ROLE_TO_DATE";
        public const string PersonFamilyName = "PERSON_FAMILY_NAME";
        public const string PersonGivenName = "PERSON_GIVEN_NAME";
        public const string PersonGivenName2 = "PERSON_GIVEN_NAME2";
        public const string PersonInitials = "PERSON_INITIALS";
        public const string PersonTitle = "PERSON_TITLE";
        public const string Postcode = "POSTCODE";
        public const string PreferredContactMethodInd = "PREFERRED_CONTACT_METHOD_IND";
        public const string Roles = "ROLES";
        public const string SaonDescription = "SAON_DESCRIPTION";
        public const string SaonEndNumber = "SAON_END_NUMBER";
        public const string SaonEndNumberSuffix = "SAON_END_NUMBER_SUFFIX";
        public const string SaonStartNumber = "SAON_START_NUMBER";
        public const string SaonStartNumberSuffix = "SAON_START_NUMBER_SUFFIX";
        public const string Street = "STREET";
        public const string TelephoneNumber = "TELEPHONE_NUMBER";
        public const string Town = "TOWN";
        public const string Udprn = "UDPRN";
        public const string UkInternalCode = "UK_INTERNAL_CODE";
        public const string UpdatedAtUtc = "UpdatedAtUtc";
    }

    public static class SamCphHoldingFields
    {
        public const string AddressPk = "ADDRESS_PK";
        public const string AnimalProductionUsageCode = "ANIMAL_PRODUCTION_USAGE_CODE";
        public const string AnimalSpeciesCode = "ANIMAL_SPECIES_CODE";
        public const string BatchId = "BATCH_ID";
        public const string ChangeType = "CHANGE_TYPE";
        public const string CountryCode = "COUNTRY_CODE";
        public const string Cph = "CPH";
        public const string CphRelationshipType = "CPH_RELATIONSHIP_TYPE";
        public const string CphType = "CPH_TYPE";
        public const string CreatedAtUtc = "CreatedAtUtc";
        public const string DiseaseType = "DISEASE_TYPE";
        public const string Easting = "EASTING";
        public const string FacilityBusinessActivityCode = "FACILITY_BUSINSS_ACTVTY_CODE";
        public const string FacilityTypeCode = "FACILITY_TYPE_CODE";
        public const string FacilitySubBusinessActivityCode = "FCLTY_SUB_BSNSS_ACTVTY_CODE";
        public const string FeatureAddressFromDate = "FEATURE_ADDRESS_FROM_DATE";
        public const string FeatureAddressToDate = "FEATURE_ADDRESS_TO_DATE";
        public const string FeatureName = "FEATURE_NAME";
        public const string FeatureStatusCode = "FEATURE_STATUS_CODE";
        public const string Interval = "INTERVAL";
        public const string IntervalUnitOfTime = "INTERVAL_UNIT_OF_TIME";
        public const string Locality = "LOCALITY";
        public const string MovementRestrictionReasonCode = "MOVEMENT_RSTRCTN_RSN_CODE";
        public const string Northing = "NORTHING";
        public const string OsMapReference = "OS_MAP_REFERENCE";
        public const string PaonEndNumber = "PAON_END_NUMBER";
        public const string PaonEndNumberSuffix = "PAON_END_NUMBER_SUFFIX";
        public const string PaonStartNumber = "PAON_START_NUMBER";
        public const string PaonStartNumberSuffix = "PAON_START_NUMBER_SUFFIX";
        public const string Postcode = "POSTCODE";
        public const string SaonEndNumber = "SAON_END_NUMBER";
        public const string SaonEndNumberSuffix = "SAON_END_NUMBER_SUFFIX";
        public const string SaonStartNumber = "SAON_START_NUMBER";
        public const string SaonStartNumberSuffix = "SAON_START_NUMBER_SUFFIX";
        public const string SecondaryCph = "SECONDARY_CPH";
        public const string Street = "STREET";
        public const string Town = "TOWN";
        public const string Udprn = "UDPRN";
        public const string UkInternalCode = "UK_INTERNAL_CODE";
        public const string UpdatedAtUtc = "UpdatedAtUtc";
        public const string PaonDescription = "PAON_DESCRIPTION";
        public const string SaonDescription = "SAON_DESCRIPTION";
    }


}
