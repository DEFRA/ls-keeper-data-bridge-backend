using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Rules;

public class Rule02A_CtsCphNotInSamTests
{
    private readonly Rule02A_CtsCphNotInSam _sut = new();

    [Fact]
    public void Descriptor_ShouldIdentifyRuleTwoA()
    {
        _sut.Descriptor.RuleId.Should().Be(RuleIds.CTS_CPH_NOT_IN_SAM);
        _sut.Descriptor.UserRuleNo.Should().Be("2A");
        _sut.Priority.Should().Be(1);
        _sut.Pass.Should().Be(AnalysisPass.CtsPrimary);
    }

    [Fact]
    public void AppliesTo_ShouldAlwaysBeTrue()
    {
        _sut.AppliesTo(CtsSamRuleContextBuilder.Build(includeSam: false)).Should().BeTrue();
        _sut.AppliesTo(CtsSamRuleContextBuilder.Build()).Should().BeTrue();
    }

    [Fact]
    public void Evaluate_WhenSamHoldingMissing_ShouldRaiseIssue()
    {
        var result = _sut.Evaluate(CtsSamRuleContextBuilder.Build(includeSam: false));

        result.Should().NotBeNull();
        result!.IssueContext!.SamCph.Should().Be(CtsSamRuleContextBuilder.DefaultCph);
        result.IssueContext.CtsLidFullIdentifier.Should().Be(CtsSamRuleContextBuilder.DefaultLid);
    }

    [Fact]
    public void Evaluate_WhenSamHoldingPresent_ShouldReturnNoIssue()
    {
        _sut.Evaluate(CtsSamRuleContextBuilder.Build()).Should().BeNull();
    }
}

public class Rule02B_SamCphNotInCtsTests
{
    private readonly Rule02B_SamCphNotInCts _sut = new();

    [Fact]
    public void Descriptor_ShouldIdentifyRuleTwoB()
    {
        _sut.Descriptor.RuleId.Should().Be(RuleIds.SAM_CPH_NOT_IN_CTS);
        _sut.Descriptor.UserRuleNo.Should().Be("2B");
        _sut.Priority.Should().Be(1);
        _sut.Pass.Should().Be(AnalysisPass.SamPrimary);
    }

    [Fact]
    public void Evaluate_WhenCtsHoldingMissing_ShouldRaiseIssue()
    {
        var context = CtsSamRuleContextBuilder.Build(pass: AnalysisPass.SamPrimary, includeCts: false);

        var result = _sut.Evaluate(context);

        result.Should().NotBeNull();
        result!.IssueContext!.SamCph.Should().Be(CtsSamRuleContextBuilder.DefaultCph);
        result.IssueContext.CtsLidFullIdentifier.Should().BeNull();
    }

    [Fact]
    public void Evaluate_WhenCtsHoldingPresent_ShouldReturnNoIssue()
    {
        var context = CtsSamRuleContextBuilder.Build(pass: AnalysisPass.SamPrimary);

        _sut.Evaluate(context).Should().BeNull();
    }
}
