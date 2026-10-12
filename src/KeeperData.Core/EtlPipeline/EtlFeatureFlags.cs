using KeeperData.Core.EtlPipeline.Views.TestData;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.EtlPipeline;

/// <summary>The pipeline's half of the host's feature flags. Bound to the same section the API binds
/// its own flags to, because Core cannot see that class and the operators setting the key should not
/// have to know there are two.</summary>
[ExcludeFromCodeCoverage]
public sealed class EtlFeatureFlags
{
    public const string SectionName = "FeatureFlags";

    /// <summary>How much of the read model the seeded test keepers may touch. Production runs
    /// <see cref="TestDataPolicy.Additive"/> so that a keeper exists to test with; everywhere else
    /// runs <see cref="TestDataPolicy.Full"/>.</summary>
    public TestDataPolicy TestDataPolicy { get; set; }
}
