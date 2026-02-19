using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.Command.Domain;

public class RuleResultTests
{
    private static readonly RuleDescriptor TestDescriptor = new("RULE_1", "1", "01", "Test", "TAG");
    private static readonly LidFullIdentifier TestLid = LidFullIdentifier.Parse("UK-12/345/6789");
    private static readonly Cph TestCph = Cph.Parse("12/345/6789");

    [Fact]
    public void Issue_WithDescriptorOnly_ShouldHaveNullContext()
    {
        var result = RuleResult.Issue(TestDescriptor);

        result.Descriptor.Should().Be(TestDescriptor);
        result.IssueContext.Should().BeNull();
    }

    [Fact]
    public void Issue_WithLidFullIdentifier_ShouldPopulateContextFromLid()
    {
        var result = RuleResult.Issue(TestDescriptor, TestLid);

        result.IssueContext.Should().NotBeNull();
        result.IssueContext!.CtsLidFullIdentifier.Should().Be("UK-12/345/6789");
        result.IssueContext.SamCph.Should().Be("12/345/6789");
    }

    [Fact]
    public void Issue_WithCph_ShouldPopulateContextWithCphOnly()
    {
        var result = RuleResult.Issue(TestDescriptor, TestCph);

        result.IssueContext.Should().NotBeNull();
        result.IssueContext!.SamCph.Should().Be("12/345/6789");
        result.IssueContext.CtsLidFullIdentifier.Should().BeNull();
    }

    [Fact]
    public void Issue_WithLidAndCphAndAction_ShouldApplyAction()
    {
        var result = RuleResult.Issue(TestDescriptor, TestLid, TestCph, ctx =>
        {
            ctx.EmailCTS = ["a@b.com"];
            ctx.TelCTS = ["01234"];
        });

        result.IssueContext!.CtsLidFullIdentifier.Should().Be("UK-12/345/6789");
        result.IssueContext.SamCph.Should().Be("12/345/6789");
        result.IssueContext.EmailCTS.Should().BeEquivalentTo(["a@b.com"]);
        result.IssueContext.TelCTS.Should().BeEquivalentTo(["01234"]);
    }

    [Fact]
    public void Issue_WithActionOnly_ShouldInvokeAction()
    {
        var result = RuleResult.Issue(TestDescriptor, ctx => ctx.FSA = "fsa-val");

        result.IssueContext.Should().NotBeNull();
        result.IssueContext!.FSA.Should().Be("fsa-val");
    }
}
