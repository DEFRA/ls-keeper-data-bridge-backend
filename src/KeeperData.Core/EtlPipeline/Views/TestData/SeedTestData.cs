using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.EtlPipeline.Views.TestData;

/// <summary>The keepers the read model is seeded with when SeedTestDataEnabled is set.
///
/// Source rather than data file: the values only change through a build and release, so a resource
/// the pipeline parses at runtime would buy nothing and could disagree with the code that reads it.
///
/// Email addresses are plus-tagged off one real mailbox, so a test can receive what the service
/// sends while the tag says which persona it was sent to.</summary>
[ExcludeFromCodeCoverage(Justification = "Test data definition - no logic to test.")]
public static class SeedTestData
{
    public static IReadOnlyList<TestPersona> Personas { get; } =
    [
        new TestPersona
        {
            Name = "single-holding",
            Party = new TestParty
            {
                // Real source party ids are a C followed by digits, so nothing upstream can collide.
                SourcePartyId = "TEST-SINGLE-HOLDING",
                Email = "defralivestock+single-holding@gmail.com",
                PersonTitle = "MR",
                GivenName = "Joe",
                FamilyName = "Bloggs",
                Telephone = "01632 960 890",
                Mobile = "07700 900123",
                AddressLine1 = "1 Old Farm",
                AddressStreet = "Digger Lane",
                AddressTown = "Leeds",
                AddressNation = "England",
                AddressPostcode = "LS1 1AA",
                AddressCountryCode = "GB"
            },
            Holdings =
            [
                new TestHolding
                {
                    Cph = "03/202/0021",
                    FeatureName = "Old Farm",
                    CphType = "permanent",
                    StartDate = new DateOnly(2000, 1, 1),
                    PaonStartNumber = "1",
                    PaonDescription = "Old Farm",
                    Street = "Digger Lane",
                    Town = "Leeds",
                    UkInternalCode = "England",
                    Postcode = "LS1 1AA",
                    CountryCode = "GB",
                    // A real OS grid reference roughly 40km off the Holderness coast: plotting a
                    // seeded holding puts it in the North Sea rather than on top of a real farm.
                    Easting = 560000,
                    Northing = 460000,
                    OsMapReference = "TA6000060000",
                    Herds =
                    [
                        new TestHerd
                        {
                            Herdmark = "930021",
                            Cphh = "03/202/0021/01",
                            AnimalSpeciesCode = "CTT",
                            AnimalPurposeCode = "CTT-BEEF-SCK",
                            DiseaseType = "TB",
                            Intervals = "12",
                            IntervalUnitOfTime = "MONTH",
                            AnimalGroupFromDate = new DateOnly(2000, 1, 1)
                        }
                    ],
                    AnimalProfiles =
                    [
                        new TestAnimalProfile
                        {
                            AnimalSpeciesCode = "CTT",
                            AnimalProductionUsageCode = "CTT-BEEF",
                            DiseaseType = "TB",
                            Interval = "12",
                            IntervalUnitOfTime = "MONTH"
                        }
                    ],
                    Roles =
                    [
                        new TestPartyRole("holder"),
                        new TestPartyRole("keeper", "03/202/0021/01"),
                        new TestPartyRole("owner", "03/202/0021/01")
                    ]
                }
            ]
        }
    ];
}
