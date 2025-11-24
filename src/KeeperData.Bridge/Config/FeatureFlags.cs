using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Config;

[ExcludeFromCodeCoverage]
public class FeatureFlags
{
    public const string SectionName = "FeatureFlags";

    public bool AuthenticationEnabled { get; set; } = true;
}