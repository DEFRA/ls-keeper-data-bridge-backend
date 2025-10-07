using Microsoft.AspNetCore.Authentication;

namespace KeeperData.Bridge.Authentication;

public class ApiKeyAuthenticationSchemeOptions : AuthenticationSchemeOptions
{
    public const string DefaultScheme = "ApiKey";
    public string Scheme => DefaultScheme;
}