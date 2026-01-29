using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Authentication;

namespace KeeperData.Bridge.Authentication;

[ExcludeFromCodeCoverage(Justification = "Simple options class with constants - no logic to test.")]
public class ApiKeyAuthenticationSchemeOptions : AuthenticationSchemeOptions
{
    public const string DefaultScheme = "ApiKey";
    public string Scheme => DefaultScheme;
}