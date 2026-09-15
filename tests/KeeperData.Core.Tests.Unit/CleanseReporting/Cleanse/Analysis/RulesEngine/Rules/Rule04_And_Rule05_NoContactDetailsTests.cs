using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Rules;

public class Rule04_NoEmailInEitherSystemTests
{
    private readonly Rule04_NoEmailInEitherSystem _sut = new();

    [Fact]
    public void Descriptor_ShouldIdentifyRuleFour()
    {
        _sut.Descriptor.RuleId.Should().Be(RuleIds.CTS_SAM_NO_EMAIL_ADDRESSES);
        _sut.Priority.Should().Be(2);
    }

    [Fact]
    public void AppliesTo_WhenSamHoldingMissing_ShouldBeFalse()
    {
        _sut.AppliesTo(CtsSamRuleContextBuilder.Build(includeSam: false)).Should().BeFalse();
    }

    [Fact]
    public void Evaluate_WhenNeitherSystemHoldsAnEmail_ShouldRaiseIssue()
    {
        var context = CtsSamRuleContextBuilder.Build();

        _sut.Evaluate(context).Should().NotBeNull();
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void Evaluate_WhenEitherSystemHoldsAnEmail_ShouldReturnNoIssue(bool cts, bool sam)
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsEmails: cts ? ["a@farm.uk"] : [],
            samEmails: sam ? ["a@farm.uk"] : []);

        _sut.Evaluate(context).Should().BeNull();
    }
}

public class Rule05_NoPhoneInEitherSystemTests
{
    private readonly Rule05_NoPhoneInEitherSystem _sut = new();

    [Fact]
    public void Descriptor_ShouldIdentifyRuleFive()
    {
        _sut.Descriptor.RuleId.Should().Be(RuleIds.CTS_SAM_NO_PHONE_NUMBERS);
        _sut.Priority.Should().Be(4);
    }

    [Fact]
    public void Evaluate_WhenNeitherSystemHoldsAPhoneNumber_ShouldRaiseIssue()
    {
        _sut.Evaluate(CtsSamRuleContextBuilder.Build()).Should().NotBeNull();
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void Evaluate_WhenEitherSystemHoldsAPhoneNumber_ShouldReturnNoIssue(bool cts, bool sam)
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsPhones: cts ? ["01234 567890"] : [],
            samPhones: sam ? ["01234 567890"] : []);

        _sut.Evaluate(context).Should().BeNull();
    }
}
