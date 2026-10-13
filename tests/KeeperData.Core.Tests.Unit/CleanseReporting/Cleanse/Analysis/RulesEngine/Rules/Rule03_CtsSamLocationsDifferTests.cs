using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Rules;

public class Rule03_CtsSamLocationsDifferTests
{
    private readonly Rule03_CtsSamLocationsDiffer _sut = new();

    [Fact]
    public void Descriptor_ShouldIdentifyRuleThree()
    {
        _sut.Descriptor.RuleId.Should().Be(RuleIds.SAM_CATTLE_RELATED_CPHs);
        _sut.Descriptor.UserRuleNo.Should().Be("3");
        _sut.Priority.Should().Be(10);
    }

    [Fact]
    public void AppliesTo_WhenSamIsNotACattleUnit_ShouldBeFalse()
    {
        var context = CtsSamRuleContextBuilder.Build(samAnimalSpeciesCode: "SHP");

        _sut.AppliesTo(context).Should().BeFalse();
    }

    [Fact]
    public void AppliesTo_WhenSamIsACattleUnit_ShouldBeTrue()
    {
        _sut.AppliesTo(CtsSamRuleContextBuilder.Build()).Should().BeTrue();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("Unknown")]
    [InlineData("not known")]
    [InlineData("NOTKNOWN")]
    [InlineData("Other Farm")]
    public void Evaluate_WhenSamLocationNameIsUnknownOrMismatched_ShouldRaiseIssue(string? samLocationName)
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsLocationName: "Green Farm",
            samLocationName: samLocationName);

        var result = _sut.Evaluate(context);

        result.Should().NotBeNull();
        result!.IssueContext!.LocationNameSAM.Should().Be(samLocationName);
        result.IssueContext.LocationNameCTS.Should().Be("Green Farm");
    }

    [Theory]
    [InlineData("Green Farm")]
    [InlineData("GREEN FARM")]
    public void Evaluate_WhenLocationNamesMatch_ShouldReturnNoIssue(string samLocationName)
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsLocationName: "Green Farm",
            samLocationName: samLocationName);

        _sut.Evaluate(context).Should().BeNull();
    }
}
