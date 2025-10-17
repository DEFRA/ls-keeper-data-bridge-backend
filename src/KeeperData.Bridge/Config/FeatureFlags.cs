using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Config;

[ExcludeFromCodeCoverage]
public class FeatureFlags
{
    public const string SectionName = "FeatureFlags";

    /// <summary>
    /// Controls whether API key authentication is required for API endpoints.
    /// Default: false (authentication disabled, all APIs accessible anonymously).
    /// Set to true to enable API key authentication.
    /// </summary>
    public bool AuthenticationEnabled { get; set; } = false;
}