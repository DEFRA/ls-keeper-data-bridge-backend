using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.EtlPipeline;

/// <summary>The pipeline's half of the host's feature flags. Bound to the same section the API binds
/// its own flags to, because Core cannot see that class and the operators setting the key should not
/// have to know there are two.</summary>
[ExcludeFromCodeCoverage]
public sealed class EtlFeatureFlags
{
    public const string SectionName = "FeatureFlags";

    /// <summary>Writes the known test keepers into the SQLite read model, replacing any real data
    /// that shares their CPHs. Off everywhere the data has to be trusted.</summary>
    public bool SeedTestDataEnabled { get; set; }
}
