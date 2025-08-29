using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Config;

[ExcludeFromCodeCoverage]
public static class Environment
{
    public static bool IsDevMode(this WebApplicationBuilder builder)
    {
        return !builder.Environment.IsProduction();
    }
}