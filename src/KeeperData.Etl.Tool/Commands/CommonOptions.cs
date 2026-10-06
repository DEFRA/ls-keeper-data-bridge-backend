using System.CommandLine;

namespace KeeperData.Etl.Tool.Commands;

/// <summary>Options more than one command takes. Declared once so that <c>--staging</c> means the
/// same thing to a run as it does to the history commands reading that run back.</summary>
internal static class CommonOptions
{
    public static readonly Option<string?> Environment = new("--env")
    {
        Description = "Which supplier bucket to read: dev, preprod, prod or sys. Selects the S3<ENV>_* credential set."
    };

    public static readonly Option<string?> Staging = new("--staging")
    {
        Description = "Data-staging root for every interstitial artefact and the SQLite read model."
    };

    public static readonly Option<bool> Json = new("--json")
    {
        Description = "Emit JSON instead of a table."
    };

    /// <summary>Only ever turns seeding on: off is the default, and a flag that could also assert the
    /// default would make "did I ask for this?" unanswerable from the command line alone.</summary>
    public static readonly Option<bool> SeedTestData = new("--seed-test-data")
    {
        Description = "Write the known test keepers into the SQLite read model, replacing any real data sharing their CPHs."
    };
}
