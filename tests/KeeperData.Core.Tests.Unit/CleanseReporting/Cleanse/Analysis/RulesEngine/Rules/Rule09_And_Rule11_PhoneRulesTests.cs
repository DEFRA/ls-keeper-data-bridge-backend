using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// Rules 11 and 9 both concern CTS phone numbers missing from SAM, and must never both
/// fire for the same record.
/// </summary>
public class Rule09_And_Rule11_PhoneRulesTests
{
    private readonly Rule11_SamMissingPhoneNumbers _rule11 = new();
    private readonly Rule09_PhonesInconsistent _rule09 = new();

    [Fact]
    public void Descriptors_ShouldIdentifyTheirRules()
    {
        _rule11.Descriptor.RuleId.Should().Be(RuleIds.SAM_MISSING_PHONE_NUMBERS);
        _rule11.Priority.Should().Be(5);
        _rule09.Descriptor.RuleId.Should().Be(RuleIds.CTS_SAM_INCONSISTENT_PHONENOS);
        _rule09.Descriptor.UserRuleNo.Should().Be("9");
        _rule09.Priority.Should().Be(8);
    }

    [Fact]
    public void WhenSamHoldsNoPhoneNumbers_OnlyRule11ShouldApply()
    {
        var context = CtsSamRuleContextBuilder.Build(ctsPhones: ["0111"], samPhones: []);

        _rule11.AppliesTo(context).Should().BeTrue();
        _rule09.AppliesTo(context).Should().BeFalse();

        _rule11.Evaluate(context)!.IssueContext!.TelCTS.Should().BeEquivalentTo(["0111"]);
    }

    [Fact]
    public void WhenSamHoldsSomeButNotAllPhoneNumbers_OnlyRule09ShouldApply()
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsPhones: ["0111", "0222"],
            samPhones: ["0111"]);

        _rule11.AppliesTo(context).Should().BeFalse();
        _rule09.AppliesTo(context).Should().BeTrue();

        var issueContext = _rule09.Evaluate(context)!.IssueContext!;
        issueContext.TelCTS.Should().BeEquivalentTo(["0222"]);
        issueContext.TelSAM.Should().Be("0111");
    }

    [Fact]
    public void WhenSamHoldsEveryCtsPhoneNumber_NeitherRuleShouldApply()
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsPhones: ["0111"],
            samPhones: ["0111", "0999"]);

        _rule11.AppliesTo(context).Should().BeFalse();
        _rule09.AppliesTo(context).Should().BeFalse();
    }
}
