using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// Rules 12 and 6 both concern CTS email addresses missing from SAM, and must never both
/// fire for the same record.
/// </summary>
public class Rule06_And_Rule12_EmailRulesTests
{
    private readonly Rule12_SamMissingEmailAddresses _rule12 = new();
    private readonly Rule06_EmailsInconsistent _rule06 = new();

    [Fact]
    public void Descriptors_ShouldIdentifyTheirRules()
    {
        _rule12.Descriptor.RuleId.Should().Be(RuleIds.SAM_MISSING_EMAIL_ADDRESSES);
        _rule12.Priority.Should().Be(3);
        _rule06.Descriptor.RuleId.Should().Be(RuleIds.CTS_SAM_INCONSISTENT_EMAIL_ADDRESSES);
        _rule06.Priority.Should().Be(7);
    }

    [Fact]
    public void WhenSamHoldsNoEmails_OnlyRule12ShouldApply()
    {
        var context = CtsSamRuleContextBuilder.Build(ctsEmails: ["a@farm.uk"], samEmails: []);

        _rule12.AppliesTo(context).Should().BeTrue();
        _rule06.AppliesTo(context).Should().BeFalse();

        _rule12.Evaluate(context)!.IssueContext!.EmailCTS.Should().BeEquivalentTo(["a@farm.uk"]);
    }

    [Fact]
    public void WhenSamHoldsSomeButNotAllEmails_OnlyRule06ShouldApply()
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsEmails: ["a@farm.uk", "b@farm.uk"],
            samEmails: ["a@farm.uk"]);

        _rule12.AppliesTo(context).Should().BeFalse();
        _rule06.AppliesTo(context).Should().BeTrue();

        var issueContext = _rule06.Evaluate(context)!.IssueContext!;
        issueContext.EmailCTS.Should().BeEquivalentTo(["b@farm.uk"]);
        issueContext.EmailSAM.Should().Be("a@farm.uk");
    }

    [Fact]
    public void WhenSamHoldsEveryCtsEmail_NeitherRuleShouldApply()
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsEmails: ["a@farm.uk"],
            samEmails: ["a@farm.uk", "extra@farm.uk"]);

        _rule12.AppliesTo(context).Should().BeFalse();
        _rule06.AppliesTo(context).Should().BeFalse();
    }

    [Fact]
    public void WhenSamHoldingMissing_NeitherRuleShouldApply()
    {
        var context = CtsSamRuleContextBuilder.Build(includeSam: false, ctsEmails: ["a@farm.uk"]);

        _rule12.AppliesTo(context).Should().BeFalse();
        _rule06.AppliesTo(context).Should().BeFalse();
    }
}
