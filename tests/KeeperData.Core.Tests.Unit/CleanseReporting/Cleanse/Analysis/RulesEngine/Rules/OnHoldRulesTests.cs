using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules.OnHold;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Rules;

public class OnHoldRulesTests
{
    [Fact]
    public void Rule07_ShouldBeRegisteredButNeverEvaluated()
        => AssertOnHold(new Rule07_SamMissingFsaNumber(), "7");

    [Fact]
    public void Rule08_ShouldBeRegisteredButNeverEvaluated()
        => AssertOnHold(new Rule08_CtsCattleCphMissing(), "8");

    [Fact]
    public void Rule10_ShouldBeRegisteredButNeverEvaluated()
        => AssertOnHold(new Rule10_AddressesInconsistent(), "10");

    private static void AssertOnHold(OnHoldCleanseRule rule, string expectedRuleNumber)
    {
        rule.Descriptor.UserRuleNo.Should().Be(expectedRuleNumber);
        rule.Status.Should().Be(RuleStatus.OnHold);
        rule.HoldReason.Should().NotBeNullOrWhiteSpace();

        var context = CtsSamRuleContextBuilder.Build();

        rule.AppliesTo(context).Should().BeFalse();
        rule.Evaluate(context).Should().BeNull();
    }
}
