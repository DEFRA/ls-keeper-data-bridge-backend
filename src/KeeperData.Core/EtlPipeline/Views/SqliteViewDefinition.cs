using System.Reflection;
using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Core.EtlPipeline.Views;

/// <summary>The transformation the export stage runs, carried in the assembly rather than deployed
/// alongside it, so the script and the code that runs it can never be different versions.
///
/// It is assembled from one script per source system - SAM, then CTS - because they share nothing
/// but the database they are written into, and reading either is easier without the other around
/// it. Each must define every macro it uses and none may take a name another has taken.
///
/// Each part names the staging tables it binds against, so the writer can skip one whose source
/// system this environment does not load. SAM is marked required: a read model with no holdings in
/// it is a broken run, not a smaller one.</summary>
public static class SqliteViewDefinition
{
    /// <summary>Bumped by hand when the stage changes the meaning of the output without the script
    /// itself changing. Part of <see cref="Version"/>.</summary>
    private const int SchemaVersion = 3;

    private const string ResourcePrefix = "KeeperData.Core.EtlPipeline.Views.Sql.";

    /// <summary>In execution order. Each is independent of the others, but a stable order keeps the
    /// fingerprint stable.</summary>
    public static IReadOnlyList<SqliteViewPart> Parts { get; } =
    [
        Part(
            "krds-read-model",
            ["sam_party", "sam_cph_holder", "sam_herd", "sam_cph_holdings"],
            ["Party", "Holding", "Herd", "HoldingAnimalProfile", "PartyRole"],
            required: true),
        Part(
            "cts-open-locations",
            ["cts_locations", "cts_location_identifiers", "cts_location_party_rels",
             "cts_parties", "cts_addresses", "cts_counties"],
            ["CtsOpenLocation"],
            required: false)
    ];

    /// <summary>Every part in order, as the one script they used to be. Only <see cref="Version"/>
    /// reads it, so editing any one script invalidates every export whatever subset of parts a
    /// given environment runs.</summary>
    public static string Sql { get; } = Assemble(Parts);

    /// <summary>Identifies this build of the transformation. Stored against the exported object so a
    /// changed script rebuilds rather than being skipped as already present. Taken over the
    /// assembled text, so editing any one script invalidates the export.</summary>
    public static string Version { get; } = Fingerprint(Sql);

    /// <summary>Every table the transformation can produce. What a run actually produced is narrower
    /// whenever a part was skipped, so this is not the export's contract - that is the table list
    /// recorded against the exported object.</summary>
    public static IReadOnlyList<string> TableNames { get; } =
        [.. Parts.SelectMany(part => part.TableNames)];

    private static SqliteViewPart Part(
        string name,
        string[] requiredSourceTables,
        string[] tableNames,
        bool required)
        => new(name, LoadResource($"{ResourcePrefix}{name}.sql"), requiredSourceTables, tableNames, required);

    private static string Assemble(IReadOnlyList<SqliteViewPart> parts)
    {
        var builder = new StringBuilder();

        foreach (var part in parts)
        {
            // A trailing statement and a leading comment would otherwise run together into one line
            // when the scripts are concatenated.
            builder.AppendLine(part.Sql);
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
