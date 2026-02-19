using FluentAssertions;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.SamCtsHoldings.Query.Models;

public class SamCphHoldingModelTests
{
    private static readonly Cph TestCph = Cph.Parse("12/345/6789");

    [Fact]
    public void AnimalSpeciesCode_ShouldReturnFromHolding()
    {
        var model = CreateModel(animalSpeciesCode: "CTT");

        model.AnimalSpeciesCode.Should().Be("CTT");
    }

    [Fact]
    public void LocationName_ShouldReturnFeatureName()
    {
        var model = CreateModel(featureName: "Green Farm");

        model.LocationName.Should().Be("Green Farm");
    }

    [Fact]
    public void GetEmailAddresses_ShouldCombinePartiesAndHolders()
    {
        var model = CreateModel(
            partyData: new[]
            {
                new Dictionary<string, object?>
                {
                    [DataFields.SamPartyFields.InternetEmailAddress] = "party@test.com",
                    [DataFields.SamPartyFields.TelephoneNumber] = null,
                    [DataFields.SamPartyFields.MobileNumber] = null,
                }
            },
            holderData: new[]
            {
                new Dictionary<string, object?>
                {
                    [DataFields.SamCphHolderFields.InternetEmailAddress] = "holder@test.com",
                    [DataFields.SamCphHolderFields.TelephoneNumber] = null,
                    [DataFields.SamCphHolderFields.MobileNumber] = null,
                }
            });

        model.GetEmailAddresses().Should().BeEquivalentTo(["holder@test.com", "party@test.com"]);
    }

    [Fact]
    public void GetEmailAddresses_ShouldDeduplicateCaseInsensitive()
    {
        var model = CreateModel(
            partyData: new[]
            {
                new Dictionary<string, object?>
                {
                    [DataFields.SamPartyFields.InternetEmailAddress] = "Shared@Test.com",
                    [DataFields.SamPartyFields.TelephoneNumber] = null,
                    [DataFields.SamPartyFields.MobileNumber] = null,
                }
            },
            holderData: new[]
            {
                new Dictionary<string, object?>
                {
                    [DataFields.SamCphHolderFields.InternetEmailAddress] = "shared@test.com",
                    [DataFields.SamCphHolderFields.TelephoneNumber] = null,
                    [DataFields.SamCphHolderFields.MobileNumber] = null,
                }
            });

        model.GetEmailAddresses().Should().HaveCount(1);
    }

    [Fact]
    public void GetPhoneNumbers_ShouldCombinePartiesAndHolders()
    {
        var model = CreateModel(
            partyData: new[]
            {
                new Dictionary<string, object?>
                {
                    [DataFields.SamPartyFields.InternetEmailAddress] = null,
                    [DataFields.SamPartyFields.TelephoneNumber] = "01234",
                    [DataFields.SamPartyFields.MobileNumber] = "07700",
                }
            },
            holderData: new[]
            {
                new Dictionary<string, object?>
                {
                    [DataFields.SamCphHolderFields.InternetEmailAddress] = null,
                    [DataFields.SamCphHolderFields.TelephoneNumber] = "01999",
                    [DataFields.SamCphHolderFields.MobileNumber] = null,
                }
            });

        var phones = model.GetPhoneNumbers();
        phones.Should().HaveCount(3);
        phones.Should().Contain("01234");
        phones.Should().Contain("07700");
        phones.Should().Contain("01999");
    }

    [Fact]
    public void GetPhoneNumbers_WhenEmpty_ShouldReturnEmpty()
    {
        var model = CreateModel();

        model.GetPhoneNumbers().Should().BeEmpty();
    }

    [Fact]
    public void GetEmailAddresses_WhenEmpty_ShouldReturnEmpty()
    {
        var model = CreateModel();

        model.GetEmailAddresses().Should().BeEmpty();
    }

    private static SamCphHoldingModel CreateModel(
        string featureName = "Farm",
        string animalSpeciesCode = "CTT",
        Dictionary<string, object?>[]? partyData = null,
        Dictionary<string, object?>[]? holderData = null)
    {
        var holding = new Dictionary<string, object?>
        {
            [DataFields.SamCphHoldingFields.Cph] = TestCph.Value,
            [DataFields.SamCphHoldingFields.FeatureName] = featureName,
            [DataFields.SamCphHoldingFields.AnimalSpeciesCode] = animalSpeciesCode,
        };

        return new SamCphHoldingModel
        {
            Cph = TestCph,
            Holding = holding,
            Herd = new QueryResult { CollectionName = "sam_herd", Data = [], Count = 0 },
            Parties = new QueryResult { CollectionName = "sam_party", Data = partyData?.ToList() ?? [], Count = partyData?.Length ?? 0 },
            Holders = new QueryResult { CollectionName = "sam_cph_holder", Data = holderData?.ToList() ?? [], Count = holderData?.Length ?? 0 },
        };
    }
}
