using FluentAssertions;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.SamCtsHoldings.Query.Models;

public class CtsCphHoldingModelTests
{
    [Fact]
    public void LocationName_ShouldReturnAdrName()
    {
        var model = CreateModel(
            holding: new Dictionary<string, object?>
            {
                [DataFields.CtsCphHoldingFields.LidFullIdentifier] = "UK-12/345/6789",
                [DataFields.CtsCphHoldingFields.AdrName] = "Test Farm",
                [DataFields.CtsCphHoldingFields.LocMobileNumber] = null,
                [DataFields.CtsCphHoldingFields.LocTelNumber] = null,
            });

        model.LocationName.Should().Be("Test Farm");
    }

    [Fact]
    public void GetEmailAddresses_ShouldExtractDistinctEmailsFromKeepers()
    {
        var model = CreateModel(keeperData: new[]
        {
            new Dictionary<string, object?> { [DataFields.CtsKeeperFields.ParEmailAddress] = "a@test.com", [DataFields.CtsKeeperFields.ParMobileNumber] = null, [DataFields.CtsKeeperFields.ParTelNumber] = null },
            new Dictionary<string, object?> { [DataFields.CtsKeeperFields.ParEmailAddress] = "A@test.com", [DataFields.CtsKeeperFields.ParMobileNumber] = null, [DataFields.CtsKeeperFields.ParTelNumber] = null },
            new Dictionary<string, object?> { [DataFields.CtsKeeperFields.ParEmailAddress] = "b@test.com", [DataFields.CtsKeeperFields.ParMobileNumber] = null, [DataFields.CtsKeeperFields.ParTelNumber] = null },
            new Dictionary<string, object?> { [DataFields.CtsKeeperFields.ParEmailAddress] = null, [DataFields.CtsKeeperFields.ParMobileNumber] = null, [DataFields.CtsKeeperFields.ParTelNumber] = null },
        });

        model.GetEmailAddresses().Should().BeEquivalentTo(["a@test.com", "b@test.com"]);
    }

    [Fact]
    public void GetEmailAddresses_WhenNoKeepers_ShouldReturnEmpty()
    {
        var model = CreateModel();

        model.GetEmailAddresses().Should().BeEmpty();
    }

    [Fact]
    public void GetPhoneNumbers_ShouldCombineHoldingAndKeeperPhones()
    {
        var model = CreateModel(
            holding: new Dictionary<string, object?>
            {
                [DataFields.CtsCphHoldingFields.LidFullIdentifier] = "UK-12/345/6789",
                [DataFields.CtsCphHoldingFields.AdrName] = "Farm",
                [DataFields.CtsCphHoldingFields.LocMobileNumber] = "07700000001",
                [DataFields.CtsCphHoldingFields.LocTelNumber] = "01onal",
            },
            keeperData: new[]
            {
                new Dictionary<string, object?>
                {
                    [DataFields.CtsKeeperFields.ParEmailAddress] = null,
                    [DataFields.CtsKeeperFields.ParMobileNumber] = "07700000002",
                    [DataFields.CtsKeeperFields.ParTelNumber] = "01onal",
                }
            });

        var phones = model.GetPhoneNumbers();
        phones.Should().HaveCount(3);
        phones.Should().Contain("07700000001");
        phones.Should().Contain("07700000002");
    }

    [Fact]
    public void GetPhoneNumbers_WhenAllEmpty_ShouldReturnEmpty()
    {
        var model = CreateModel();

        model.GetPhoneNumbers().Should().BeEmpty();
    }

    private static CtsCphHoldingModel CreateModel(
        Dictionary<string, object?>? holding = null,
        Dictionary<string, object?>[]? keeperData = null)
    {
        holding ??= new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LidFullIdentifier] = "UK-12/345/6789",
            [DataFields.CtsCphHoldingFields.AdrName] = "Farm",
            [DataFields.CtsCphHoldingFields.LocMobileNumber] = null,
            [DataFields.CtsCphHoldingFields.LocTelNumber] = null,
        };

        var keepers = new QueryResult
        {
            CollectionName = "cts_keeper",
            Data = keeperData?.ToList() ?? [],
            Count = keeperData?.Length ?? 0
        };

        return new CtsCphHoldingModel
        {
            Id = LidFullIdentifier.Parse("UK-12/345/6789"),
            Holding = holding,
            Keepers = keepers
        };
    }
}
