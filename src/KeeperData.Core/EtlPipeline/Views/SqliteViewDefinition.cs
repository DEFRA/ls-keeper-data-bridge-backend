using System.Reflection;
using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Core.EtlPipeline.Views;

/// <summary>The transformation the export stage runs, carried in the assembly rather than deployed
/// alongside it, so the script and the code that runs it can never be different versions.
///
/// It is assembled from one script per source system - SAM, then CTS - because they share nothing
/// but the database they are written into, and reading either is easier without the other around
/// it. They still run as a single command, so each must define every macro it uses and none may
/// take a name another has taken.</summary>
public static class SqliteViewDefinition
{
    /// <summary>Bumped by hand when the stage changes the meaning of the output without the script
    /// itself changing. Part of <see cref="Version"/>.</summary>
    private const int SchemaVersion = 2;

    private const string ResourcePrefix = "KeeperData.Core.EtlPipeline.Views.Sql.";

    /// <summary>In execution order. Each is independent of the others, but a stable order keeps the
    /// fingerprint stable.</summary>
    private static readonly string[] ResourceNames =
    [
        "krds-read-model.sql",
        "cts-open-locations.sql"
    ];

    public static string Sql { get; } = Load();

    /// <summary>Identifies this build of the transformation. Stored against the exported object so a
    /// changed script rebuilds rather than being skipped as already present. Taken over the
    /// assembled text, so editing any one script invalidates the export.</summary>
    public static string Version { get; } = Fingerprint(Sql);

    /// <summary>The tables the script produces, counted after a run for reporting.</summary>
    public static IReadOnlyList<string> TableNames { get; } =
        ["Party", "Holding", "Herd", "HoldingAnimalProfile", "PartyRole", "CtsOpenLocation"];

    private static string Load()
    {
        var builder = new StringBuilder();

        foreach (var name in ResourceNames)
        {
            // A trailing statement and a leading comment would otherwise run together into one line
            // when the scripts are concatenated.
            builder.AppendLine(LoadResource(ResourcePrefix + name));
            builder.AppendLine();
        }

        return builder.ToString();
    }

    private static string LoadResource(string resourceName)
    {
        using var stream = typeof(SqliteViewDefinition).Assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{resourceName}' is missing. It is declared as an EmbeddedResource in " +
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
