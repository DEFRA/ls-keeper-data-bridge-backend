using System.Reflection;
using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Core.EtlPipeline.Views;

/// <summary>The transformation the export stage runs, carried in the assembly rather than deployed
/// alongside it, so the script and the code that runs it can never be different versions.</summary>
public static class SqliteViewDefinition
{
    /// <summary>Bumped by hand when the stage changes the meaning of the output without the script
    /// itself changing. Part of <see cref="Version"/>.</summary>
    private const int SchemaVersion = 1;

    private const string ResourceName = "KeeperData.Core.EtlPipeline.Views.Sql.krds-read-model.sql";

    public static string Sql { get; } = Load();

    /// <summary>Identifies this build of the transformation. Stored against the exported object so a
    /// changed script rebuilds rather than being skipped as already present.</summary>
    public static string Version { get; } = Fingerprint(Sql);

    /// <summary>The tables the script produces, counted after a run for reporting.</summary>
    public static IReadOnlyList<string> TableNames { get; } =
        ["Party", "Holding", "Herd", "HoldingAnimalProfile", "PartyRole"];

    private static string Load()
    {
        using var stream = typeof(SqliteViewDefinition).Assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{ResourceName}' is missing. It is declared as an EmbeddedResource in " +
                $"{Assembly.GetExecutingAssembly().GetName().Name}.csproj.");

        using var reader = new StreamReader(stream, Encoding.UTF8);

        return reader.ReadToEnd();
    }

    private static string Fingerprint(string sql)
    {
        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(sql));

        return $"v{SchemaVersion}-{Convert.ToHexString(digest)[..16].ToLowerInvariant()}";
    }
}
