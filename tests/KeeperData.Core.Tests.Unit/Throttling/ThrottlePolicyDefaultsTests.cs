using FluentAssertions;
using KeeperData.Core.Throttling;

namespace KeeperData.Core.Tests.Unit.Throttling;

public class ThrottlePolicyDefaultsTests
{
    [Fact]
    public void NormalPolicy_ShouldBeReadOnly()
    {
        ThrottlePolicyDefaults.NormalPolicy.IsReadOnly.Should().BeTrue();
    }

    [Fact]
    public void NormalPolicy_ShouldNotBeActive()
    {
        ThrottlePolicyDefaults.NormalPolicy.IsActive.Should().BeFalse();
    }

    [Fact]
    public void NormalPolicy_ShouldHaveSlug()
    {
        ThrottlePolicyDefaults.NormalPolicy.Slug.Should().Be("normal");
    }

    [Fact]
    public void SeedPolicies_ShouldContainThree()
    {
        ThrottlePolicyDefaults.SeedPolicies.Should().HaveCount(3);
    }

    [Fact]
    public void SeedPolicies_ShouldHaveUniqueNames()
    {
        var names = ThrottlePolicyDefaults.SeedPolicies.Select(p => p.Name).ToList();
        names.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void SeedPolicies_ShouldHaveUniqueSlugs()
    {
        var slugs = ThrottlePolicyDefaults.SeedPolicies.Select(p => p.Slug).ToList();
        slugs.Should().OnlyHaveUniqueItems();
    }

    [Theory]
    [InlineData("L1 Light Throttle", "l1-light-throttle")]
    [InlineData("L2 Moderate Throttle", "l2-moderate-throttle")]
    [InlineData("L3 Heavy Throttle", "l3-heavy-throttle")]
    [InlineData("  My Policy  ", "my-policy")]
    [InlineData("Test!@#Policy", "test-policy")]
    public void ToSlug_ShouldGenerateUrlFriendlySlug(string name, string expected)
    {
        ThrottlePolicyDefaults.ToSlug(name).Should().Be(expected);
    }
}
