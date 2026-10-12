namespace KeeperData.Core.EtlPipeline.Views.TestData;

/// <summary>How much of the read model the seeded test keepers are allowed to touch.
///
/// Named for what it does to real data rather than for how much it writes, because that is the
/// property that matters when the value is read off a production environment.</summary>
public enum TestDataPolicy
{
    /// <summary>No test data. What production ran before there was a policy.</summary>
    None = 0,

    /// <summary>Only adds rows. A seeded persona attaches itself to holdings the extracts already
    /// carry and changes nothing that was there - safe where the data has to stay trustworthy.</summary>
    Additive = 1,

    /// <summary>Personas own their holdings outright: a seeded CPH loses whatever herds, animal
    /// profiles and party roles the extract gave it. Destroys real data, so non-production only.</summary>
    Full = 2
}
