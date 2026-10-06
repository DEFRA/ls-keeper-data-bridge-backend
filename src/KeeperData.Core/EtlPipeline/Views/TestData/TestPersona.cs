using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.EtlPipeline.Views.TestData;

/// <summary>A known keeper seeded into the read model so a test can look one up by email and get a
/// predictable holding back.
///
/// Shaped as source data rather than as rows: every id the read model is keyed by is derived from
/// the source key - the party's source id, the holding's CPH, the herd's mark and CPHH - so a
/// persona naming a CPH the real extracts also carry lands on that holding's row rather than beside
/// it. Which is the point: the seed replaces real data where the two meet.
///
/// One party holding many holdings, because the personas differ in how much they hold.</summary>
[ExcludeFromCodeCoverage(Justification = "Test data definition - no logic to test.")]
public sealed record TestPersona
{
    /// <summary>Identifies the persona in logs and in the email it is reachable on. Not stored.</summary>
    public required string Name { get; init; }

    public required TestParty Party { get; init; }

    public required IReadOnlyList<TestHolding> Holdings { get; init; }
}

/// <summary>The keeper themselves. <see cref="SourcePartyId"/> stands in for a SAM PARTY_ID and must
/// be one no extract could carry: real ones are a C followed by digits.</summary>
[ExcludeFromCodeCoverage(Justification = "Test data definition - no logic to test.")]
public sealed record TestParty
{
    public required string SourcePartyId { get; init; }

    /// <summary>Lower case. The read model folds the real column and indexes it case-sensitively, so
    /// a mixed-case seed would be unfindable by the equality match callers use.</summary>
    public required string Email { get; init; }

    public string? PersonTitle { get; init; }
    public string? GivenName { get; init; }
    public string? GivenName2 { get; init; }
    public string? Initials { get; init; }
    public string? FamilyName { get; init; }
    public string? OrganisationName { get; init; }
    public string? Telephone { get; init; }
    public string? Mobile { get; init; }
    public string? AddressLine1 { get; init; }
    public string? AddressStreet { get; init; }
    public string? AddressTown { get; init; }
    public string? AddressLocality { get; init; }
    public string? AddressNation { get; init; }
    public string? AddressPostcode { get; init; }
    public string? AddressCountryCode { get; init; }
    public string? Roles { get; init; }
}

/// <summary>A holding the persona is attached to, with everything hanging off it. The herds, profiles
/// and roles here are the whole of what the seeded holding carries - a persona owns its CPH
/// outright.</summary>
[ExcludeFromCodeCoverage(Justification = "Test data definition - no logic to test.")]
public sealed record TestHolding
{
    public required string Cph { get; init; }

    public string? FeatureName { get; init; }
    public string? CphType { get; init; }
    public DateOnly? StartDate { get; init; }
    public DateOnly? EndDate { get; init; }
    public string? AddressPk { get; init; }
    public string? SaonStartNumber { get; init; }
    public string? SaonStartNumberSuffix { get; init; }
    public string? SaonEndNumber { get; init; }
    public string? SaonEndNumberSuffix { get; init; }
    public string? SaonDescription { get; init; }
    public string? PaonStartNumber { get; init; }
    public string? PaonStartNumberSuffix { get; init; }
    public string? PaonEndNumber { get; init; }
    public string? PaonEndNumberSuffix { get; init; }
    public string? PaonDescription { get; init; }
    public string? Street { get; init; }
    public string? Town { get; init; }
    public string? Locality { get; init; }
    public string? UkInternalCode { get; init; }
    public string? Postcode { get; init; }
    public string? CountryCode { get; init; }
    public long? Udprn { get; init; }
    public long? Easting { get; init; }
    public long? Northing { get; init; }
    public string? OsMapReference { get; init; }

    public IReadOnlyList<TestHerd> Herds { get; init; } = [];
    public IReadOnlyList<TestAnimalProfile> AnimalProfiles { get; init; } = [];
    public IReadOnlyList<TestPartyRole> Roles { get; init; } = [];
}

/// <summary>An animal group on the holding. <see cref="Cphh"/> is the holding's CPH with the group's
/// two-digit suffix: the read model derives the holding from its first eleven characters.</summary>
[ExcludeFromCodeCoverage(Justification = "Test data definition - no logic to test.")]
public sealed record TestHerd
{
    public required string Herdmark { get; init; }
    public required string Cphh { get; init; }

    public string? AnimalSpeciesCode { get; init; }
    public string? AnimalPurposeCode { get; init; }
    public string? DiseaseType { get; init; }
    public string? Intervals { get; init; }
    public string? IntervalUnitOfTime { get; init; }
    public string? MovementRestrictionReasonCode { get; init; }
    public DateOnly? AnimalGroupFromDate { get; init; }
    public DateOnly? AnimalGroupToDate { get; init; }
}

/// <summary>What the holding is registered to keep, independently of whether a group exists for it.</summary>
[ExcludeFromCodeCoverage(Justification = "Test data definition - no logic to test.")]
public sealed record TestAnimalProfile
{
    public required string AnimalSpeciesCode { get; init; }

    public string? AnimalProductionUsageCode { get; init; }
    public string? DiseaseType { get; init; }
    public string? Interval { get; init; }
    public string? IntervalUnitOfTime { get; init; }
}

/// <summary>How the persona is attached to the holding.</summary>
/// <param name="Role">One of owner, holder or keeper - the read model constrains the column to those.</param>
/// <param name="Cphh">The herd the role is against, or null for a role on the holding itself. Must name
/// one of the holding's own herds.</param>
[ExcludeFromCodeCoverage(Justification = "Test data definition - no logic to test.")]
public sealed record TestPartyRole(string Role, string? Cphh = null);
