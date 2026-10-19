using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Config;

[ExcludeFromCodeCoverage]
public class ApiAuthenticationOptions
{
    public const string SectionName = "ApiAuthentication";

    public string ApiKey { get; set; } = string.Empty;
    public bool ApiKeyEnabled { get; set; } = true;
    public StsAuthenticationOptions Sts { get; set; } = new();
}

[ExcludeFromCodeCoverage]
public class StsAuthenticationOptions
{
    public bool Enabled { get; set; } = true;
    public string Endpoint { get; set; } = "https://sts.eu-west-2.amazonaws.com";
    public int MaxExpiresSeconds { get; set; } = 300;
    public int ClockSkewSeconds { get; set; } = 30;
    public int VerifyTimeoutSeconds { get; set; } = 10;
    public int CacheTtlSeconds { get; set; } = 300;
    public List<AllowedStsCaller> AllowedCallers { get; set; } = [];
}

[ExcludeFromCodeCoverage]
public class AllowedStsCaller
{
    public string AccountId { get; set; } = string.Empty;
    public string RolePattern { get; set; } = string.Empty;
}