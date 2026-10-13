using KeeperData.Core.EtlPipeline.Views.TestData;
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

    /// <summary>Left unspecified the configured policy stands, so a run only overrides what it was
    /// asked to.</summary>
    public static readonly Option<TestDataPolicy?> TestData = new("--test-data")
    {
        Description = "Test data policy for the SQLite read model: None, Additive (adds a keeper to holdings that already exist) or Full (seeded keepers own their CPHs, replacing real data)."
    };
}
