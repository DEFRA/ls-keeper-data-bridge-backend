using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Rules;

public class Rule01_SamNoCattleUnitTests
{
    private readonly Rule01_SamNoCattleUnit _sut = new();

    [Fact]
    public void Descriptor_ShouldIdentifyRuleOne()
    {
        _sut.Descriptor.RuleId.Should().Be(RuleIds.SAM_NO_CATTLE_UNIT);
        _sut.Descriptor.UserRuleNo.Should().Be("1");
        _sut.Priority.Should().Be(6);
        _sut.Pass.Should().Be(AnalysisPass.CtsPrimary);
        _sut.Status.Should().Be(RuleStatus.Active);
    }

    [Fact]
    public void AppliesTo_WhenHoldingExistsInBothSystems_ShouldBeTrue()
    {
        var context = CtsSamRuleContextBuilder.Build();

        _sut.AppliesTo(context).Should().BeTrue();
    }

    [Fact]
    public void AppliesTo_WhenSamHoldingMissing_ShouldBeFalse()
    {
        var context = CtsSamRuleContextBuilder.Build(includeSam: false);

        _sut.AppliesTo(context).Should().BeFalse();
    }

    [Fact]
    public void AppliesTo_WhenCtsHoldingMissing_ShouldBeFalse()
    {
        var context = CtsSamRuleContextBuilder.Build(includeCts: false);

        _sut.AppliesTo(context).Should().BeFalse();
    }

    [Fact]
    public void Evaluate_WhenSamDeclaresCattleUnit_ShouldReturnNoIssue()
    {
        var context = CtsSamRuleContextBuilder.Build(samAnimalSpeciesCode: "CTT");

        _sut.Evaluate(context).Should().BeNull();
    }

    [Theory]
    [InlineData("SHP")]
    [InlineData("")]
    [InlineData(null)]
    public void Evaluate_WhenSamDoesNotDeclareCattleUnit_ShouldRaiseIssue(string? animalSpeciesCode)
    {
        var context = CtsSamRuleContextBuilder.Build(samAnimalSpeciesCode: animalSpeciesCode);

        var result = _sut.Evaluate(context);

        result.Should().NotBeNull();
        result!.Descriptor.RuleId.Should().Be(RuleIds.SAM_NO_CATTLE_UNIT);
        result.IssueContext!.AnimalSpeciesCode.Should().Be(animalSpeciesCode);
    }

    [Fact]
    public void Evaluate_WhenIssueRaised_ShouldCarryRecordIdentity()
    {
        var context = CtsSamRuleContextBuilder.Build(samAnimalSpeciesCode: "SHP");

        var result = _sut.Evaluate(context);

        result!.IssueContext!.SamCph.Should().Be(CtsSamRuleContextBuilder.DefaultCph);
        result.IssueContext.CtsLidFullIdentifier.Should().Be(CtsSamRuleContextBuilder.DefaultLid);
    }
}
