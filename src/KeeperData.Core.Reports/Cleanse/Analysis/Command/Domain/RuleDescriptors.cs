using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

public static class RuleDescriptors
{
    public static RuleDescriptor CtsCphNotInSam => new(RuleIds.CTS_CPH_NOT_IN_SAM, "2A", "02A", "Active CTS CPH inactive / missing in Sam", "ULITP-3389");
    public static RuleDescriptor SamCphNotInCts => new(RuleIds.SAM_CPH_NOT_IN_CTS, "2B", "2B", "Active SAM CPH inactive / missing in CTS", "ULITP-3217");
    public static RuleDescriptor CtsSamNoEmailAddresses => new(RuleIds.CTS_SAM_NO_EMAIL_ADDRESSES, "4", "04",
        "CPH present in both CTS and SAM but no email addresses in either system", "ULITP-3307");
    public static RuleDescriptor SamMissingEmailAddresses => new(RuleIds.SAM_MISSING_EMAIL_ADDRESSES, "12", "12", "Email address in CTS but missing in SAM", "ULITP-3493");
    public static RuleDescriptor CtsSamNoPhoneNumbers => new(RuleIds.CTS_SAM_NO_PHONE_NUMBERS, "5", "05", "No telephone numbers in CTS or Sam", "ULITP-3351");
    public static RuleDescriptor SamMissingPhoneNumbers => new(RuleIds.SAM_MISSING_PHONE_NUMBERS, "11", "11", "Phone number in CTS but missing in SAM", "ULITP-3579");
    public static RuleDescriptor SamNoCattleUnit => new(RuleIds.SAM_NO_CATTLE_UNIT, "1", "01", "No cattle unit defined in SAM", "ULITP-3439");
    //public static CleanseRuleDescriptor CtsSamEmailAddressesInconsistent => new(IssueCodes.CTS_SAM_INCONSISTENT_EMAIL_ADDRESSES, "6", "06", "SAM is missing email addresses found in CTS", "ULITP-3461");
    //public static CleanseRuleDescriptor CtsSamPhoneNosInconsistent => new(IssueCodes.CTS_SAM_INCONSISTENT_PHONENOS, "9", "09", "SAM is missing phone numbers found in CTS", "ULITP-3578");
    public static RuleDescriptor CtsSamLocationsDiffer => new(RuleIds.SAM_CATTLE_RELATED_CPHs, "3", "03", "Cattle-related CPHs in SAM (e.g. those with relevant animal species or purpose codes) that are not present in CTS", "ULITP-3440");

}







// CPH | Rule No | Error Code | Error Description | Email CTS | Email SAM | Tel CTS | Tel SAM | FSA 