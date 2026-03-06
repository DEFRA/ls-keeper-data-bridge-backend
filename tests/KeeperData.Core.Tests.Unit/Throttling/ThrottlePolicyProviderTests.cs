using FluentAssertions;
using KeeperData.Core.Throttling;
using KeeperData.Core.Throttling.Impl;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Tests.Unit.Throttling;

public class ThrottlePolicyProviderTests
{
    [Fact]
    public void Current_Initially_ShouldReturnNormalDefaults()
    {
        var sut = new ThrottlePolicyProvider();

        sut.Current.Should().Be(ThrottlePolicyDefaults.NormalPolicy.Settings);
        sut.ActivePolicyName.Should().Be(ThrottlePolicyDefaults.NormalName);
    }

    [Fact]
    public void Refresh_WithPolicy_ShouldSwapSettings()
    {
        var sut = new ThrottlePolicyProvider();
        var policy = new ThrottlePolicy
        {
            Slug = "test",
            Name = "Test",
            IsActive = true,
            Settings = new ThrottlePolicySettings
            {
                Ingestion = new() { BatchSize = 999 }
            }
        };

        sut.Refresh(policy);

        sut.Current.Ingestion.BatchSize.Should().Be(999);
        sut.ActivePolicyName.Should().Be("Test");
    }

    [Fact]
    public void Refresh_WithNull_ShouldRevertToNormal()
    {
        var sut = new ThrottlePolicyProvider();
        sut.Refresh(new ThrottlePolicy
        {
            Slug = "test", Name = "Test", IsActive = true,
            Settings = new ThrottlePolicySettings { Ingestion = new() { BatchSize = 999 } }
        });

        sut.Refresh(null);

        sut.ActivePolicyName.Should().Be(ThrottlePolicyDefaults.NormalName);
        sut.Current.Ingestion.BatchSize.Should().Be(ThrottlePolicyDefaults.NormalPolicy.Settings.Ingestion.BatchSize);
    }
}
