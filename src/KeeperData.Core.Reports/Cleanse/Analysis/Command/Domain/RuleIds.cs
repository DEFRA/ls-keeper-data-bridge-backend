namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

/// <summary>
/// Issue codes for cleanse analysis.
/// </summary>
public static class RuleIds
{
    /// <summary>
    ///     Iterate cts holdings; if holding is not found in SAM then raise issue
    /// </summary>
    /// <remarks>
    ///     PRIORITY 1A: RULE 2A - CPH present in CTS but missing in SAM
    /// </remarks>
    public const string CTS_CPH_NOT_IN_SAM = "CTS_CPH_NOT_IN_SAM";

    /// <summary>
    ///     Iterate sam holdings; if holding is not found in SAM then raise issue
    /// </summary>
    /// <remarks>
    ///     PRIORITY 1B: RULE 2B - CPH present in SAM but missing in CTS
    /// </remarks>
    public const string SAM_CPH_NOT_IN_CTS = "SAM_CPH_NOT_IN_CTS";

    /// <summary>
    ///     Iterate cts; when exists in cts and sam, if no email addresses in either system, raise issue
    /// </summary>
    /// <remarks>
    ///     PRIORITY 2:  RULE 4  - CPH present in both CTS and SAM but no email addresses in either system
    /// </remarks>
    public const string CTS_SAM_NO_EMAIL_ADDRESSES = "CTS_SAM_NO_EMAIL_ADDRESSES";


    /// <summary>
    ///     Iterate cts; for each email address in CTS, that is NOT in SAM, raise issue
    /// </summary>
    /// <remarks>
    ///     PRIORITY 3:  RULE 12 - Email addresses in CTS missing from SAM
    /// </remarks>
    public const string SAM_MISSING_EMAIL_ADDRESSES = "SAM_MISSING_EMAIL_ADDRESS";

    /// <summary>
    ///     Iterate cts; when exists in cts and sam, if no phone nos in either system, raise issue
    /// </summary>
    /// <remarks>
    ///     PRIORITY 4:  RULE 5  - CPH present in both CTS and SAM but no phone numbers in either system
    /// </remarks>
    public const string CTS_SAM_NO_PHONE_NUMBERS = "CTS_SAM_NO_PHONE_NUMBERS";


    /// <summary>
    ///     Iterate cts; where exists in both - for each phone number in CTS, that is NOT in SAM, raise issue
    /// </summary>
    /// <remarks>
    ///     PRIORITY 5:  RULE 11 - CTS phone numbers missing from SAM
    /// </remarks>
    public const string SAM_MISSING_PHONE_NUMBERS = "SAM_MISSING_PHONE_NUMBERS";


    /// <summary>
    ///     Iterate cts; where exists in both - if no cattle unit defined in SAM, raise issue
    /// </summary>
    /// <remarks>
    ///     PRIORITY 6:  RULE 1  - No cattle unit defined in SAM
    /// </remarks>
    public const string SAM_NO_CATTLE_UNIT = "SAM_NO_CATTLE_UNIT";


    ///// <summary>
    /////     Iterate cts; where exists in both - for each cts keeper email that does not exist somewhere in SAM, raise issue
    ///// </summary>
    ///// <remarks>
    /////     PRIORITY 7:  RULE 6 - Email addresses inconsistent between CTS and SAM
    ///// </remarks>
    ///// BLOCKED: same as rule 12
    //public const string CTS_SAM_INCONSISTENT_EMAIL_ADDRESSES = "CTS_SAM_INCONSIS_EMAILS";


    /// <summary>
    /// iterate cts; where exists in both - for each phone no that does not exist somewhere in SAM, raise issue
    /// </summary>
    // BLOCKED same as rule 11
    //public const string CTS_SAM_INCONSISTENT_PHONENOS = "CTS_SAM_INCONSIS_PHONENOS";        // [*TODO] PRIORITY 8:  RULE 9 - Phone nos inconsistent between CTS and SAM
    
    
    ///// <summary>
    ///// ON_HOLD
    ///// </summary>
    //public const string CTS_SAM_INCONSISTENT_ADDRESSES = "CTS_SAM_INCONSIS_ADDRESS";        // [!HOLD] PRIORITY 9:  RULE 10 - Correspondence address details inconsistent between CTS and SAM

    /// <summary>
    /// iterate cts; where exists in both and ANIMAL_SPECIES_CODE=CTT - if SAM.FEATURE_NAME=['Unknown','Not known','Notknown','',null] OR CTS.ADR_NAME != SAM.FEATURE_NAME then raise issue
    /// </summary>
    public const string SAM_CATTLE_RELATED_CPHs = "SAM_CATTLE_RELATED_CPHs";                // [*TODO] PRIORITY 10: RULE 3 - Cattle-related CPHs in SAM (e.g. those with relevant animal species or purpose codes) that are not present in CTS

    ///// <summary>
    ///// ON_HOLD; iterate cts where LTY_LOC_TYPE='SH', lidfullidentifier format 'SH-{FSANUMBER:d4}' 'SH-8357' --- cannot identify the SAM record using the logic provided.
    ///// </summary>
    //public const string SAM_MISSING_FSA_NO = "SAM_MISSING_FSA_NO";                          // [!HOLD] PRIORITY 11: RULE 7 - CPHs in SAM missing FSA number

    ///// <summary>
    ///// ON_HOLD; virtually the same as Rule 1B
    ///// </summary>
    //public const string CTS_CATTLE_CPH_MISSING = "CTS_CATTLE_CPH_MISSING";                   // [!HOLD] PRIORITY 12: RULE 8 - Cattle-related CPHs missing in CTS
}







// CPH | Rule No | Error Code | Error Description | Email CTS | Email SAM | Tel CTS | Tel SAM | FSA 