using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Registry;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Registry;

public class CleanseRuleRegistryTests
{
    private readonly CleanseRuleRegistry _sut = new(CleanseRuleRegistry.CreateDefaultRules());

    [Fact]
    public void CreateDefaultRules_ShouldNotBeEmpty()
    {
        CleanseRuleRegistry.CreateDefaultRules().Should().NotBeEmpty();
    }

    [Fact]
    public void Constructor_WhenGivenNoRules_ShouldThrowRatherThanBuildAnEmptyRegistry()
    {
        var act = () => new CleanseRuleRegistry([]);

        act.Should().Throw<ArgumentException>().WithMessage("*cannot be constructed without rules*");
    }

    [Fact]
    public void For_ShouldFindActiveRulesOnBothPasses()
    {
        _sut.For(AnalysisPass.CtsPrimary).Should().NotBeEmpty();
        _sut.For(AnalysisPass.SamPrimary).Should().NotBeEmpty();
    }

    [Fact]
    public void All_ShouldContainEveryKnownRule()
    {
        _sut.All.Should().HaveCount(13);
    }

    [Fact]
    public void All_ShouldBeOrderedByPriority()
    {
        _sut.All.Select(rule => rule.Priority).Should().BeInAscendingOrder();
    }

    [Fact]
    public void All_ShouldNotContainDuplicateRuleNumbersOrRuleIds()
    {
        _sut.All.Select(rule => rule.Descriptor.UserRuleNo).Should().OnlyHaveUniqueItems();
        _sut.All.Select(rule => rule.Descriptor.RuleId).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void For_ShouldExcludeOnHoldRules()
    {
        var scheduled = _sut.For(AnalysisPass.CtsPrimary).Concat(_sut.For(AnalysisPass.SamPrimary));

        scheduled.Should().OnlyContain(rule => rule.Status == RuleStatus.Active);
        scheduled.Should().HaveCount(10);
    }

    [Fact]
    public void For_ShouldReturnOnlyRulesDeclaringThatPass()
    {
        _sut.For(AnalysisPass.CtsPrimary).Should().OnlyContain(rule => rule.Pass == AnalysisPass.CtsPrimary);
        _sut.For(AnalysisPass.SamPrimary).Should().OnlyContain(rule => rule.Pass == AnalysisPass.SamPrimary);
    }

    [Fact]
    public void For_SamPrimary_ShouldContainOnlyRuleTwoB()
    {
        _sut.For(AnalysisPass.SamPrimary).Select(rule => rule.Descriptor.RuleId)
            .Should().BeEquivalentTo([RuleIds.SAM_CPH_NOT_IN_CTS]);
    }

    [Theory]
    [InlineData("1", RuleIds.SAM_NO_CATTLE_UNIT)]
    [InlineData("2A", RuleIds.CTS_CPH_NOT_IN_SAM)]
    [InlineData("2b", RuleIds.SAM_CPH_NOT_IN_CTS)]
    [InlineData("10", RuleIds.CTS_SAM_INCONSISTENT_ADDRESSES)]
    public void ByNumber_ShouldFindTheRule(string ruleNumber, string expectedRuleId)
    {
        _sut.ByNumber(ruleNumber)!.Descriptor.RuleId.Should().Be(expectedRuleId);
    }

    [Fact]
    public void ByNumber_WhenRuleNumberIsUnknown_ShouldReturnNull()
    {
        _sut.ByNumber("99").Should().BeNull();
    }

    [Fact]
    public void Describe_ShouldListEveryRuleOnItsOwnLine()
    {
        var lines = _sut.Describe().Split(Environment.NewLine);

        lines.Should().HaveCount(_sut.All.Count);
        lines.Should().Contain(line => line.Contains(RuleIds.SAM_MISSING_FSA_NO) && line.Contains("OnHold"));
    }
}
