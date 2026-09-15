using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Registry;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Service;
using KeeperData.Core.Reports.Domain;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Service;

public class CleanseRuleServiceTests
{
    private static CleanseRuleService CreateSut(params ICleanseRule[] rules)
        => new(new CleanseRuleRegistry(rules), NullLogger<CleanseRuleService>.Instance);

    [Fact]
    public void Evaluate_ShouldReturnIssuesFromRulesThatFired()
    {
        var sut = CreateSut(
            new StubRule("A", priority: 2, applies: true, raisesIssue: true),
            new StubRule("B", priority: 1, applies: true, raisesIssue: true));

        var results = sut.Evaluate(CtsSamRuleContextBuilder.Build());

        results.Select(result => result.Descriptor.RuleId).Should().ContainInOrder("B", "A");
    }

    [Fact]
    public void Evaluate_ShouldSkipRulesThatDoNotApply()
    {
        var skipped = new StubRule("A", priority: 1, applies: false, raisesIssue: true);
        var sut = CreateSut(skipped);

        sut.Evaluate(CtsSamRuleContextBuilder.Build()).Should().BeEmpty();
        skipped.WasEvaluated.Should().BeFalse();
    }

    [Fact]
    public void Evaluate_ShouldOmitRulesThatFoundNoIssue()
    {
        var sut = CreateSut(new StubRule("A", priority: 1, applies: true, raisesIssue: false));

        sut.Evaluate(CtsSamRuleContextBuilder.Build()).Should().BeEmpty();
    }

    [Fact]
    public void Evaluate_ShouldIgnoreRulesDeclaringAnotherPass()
    {
        var sut = CreateSut(new StubRule("A", priority: 1, applies: true, raisesIssue: true, pass: AnalysisPass.SamPrimary));

        sut.Evaluate(CtsSamRuleContextBuilder.Build(pass: AnalysisPass.CtsPrimary)).Should().BeEmpty();
    }

    [Fact]
    public void Evaluate_WhenOneRuleThrows_ShouldStillEvaluateTheOthers()
    {
        var sut = CreateSut(
            new StubRule("Faulty", priority: 1, applies: true, raisesIssue: true, throws: true),
            new StubRule("Healthy", priority: 2, applies: true, raisesIssue: true));

        var results = sut.Evaluate(CtsSamRuleContextBuilder.Build());

        results.Select(result => result.Descriptor.RuleId).Should().BeEquivalentTo(["Healthy"]);
    }

    [Fact]
    public void Evaluate_WithTheRealRuleSet_ShouldReturnIssuesInPriorityOrder()
    {
        var sut = new CleanseRuleService(new CleanseRuleRegistry(), NullLogger<CleanseRuleService>.Instance);

        var context = CtsSamRuleContextBuilder.Build(
            samAnimalSpeciesCode: "SHP",
            ctsEmails: ["a@farm.uk"],
            samEmails: []);

        var results = sut.Evaluate(context);

        results.Select(result => result.Descriptor.RuleId).Should().ContainInOrder(
            RuleIds.SAM_MISSING_EMAIL_ADDRESSES,
            RuleIds.CTS_SAM_NO_PHONE_NUMBERS,
            RuleIds.SAM_NO_CATTLE_UNIT);
    }

    private sealed class StubRule(string ruleId, int priority, bool applies, bool raisesIssue,
        AnalysisPass pass = AnalysisPass.CtsPrimary, bool throws = false) : ICleanseRule
    {
        public bool WasEvaluated { get; private set; }

        public RuleDescriptor Descriptor { get; } = new(ruleId, ruleId, ruleId, ruleId, "TEST");

        public int Priority => priority;

        public AnalysisPass Pass => pass;

        public RuleStatus Status => RuleStatus.Active;

        public bool AppliesTo(CtsSamRuleContext context) => applies;

        public RuleResult? Evaluate(CtsSamRuleContext context)
        {
            WasEvaluated = true;

            if (throws)
            {
                throw new InvalidOperationException("Rule failed on malformed data.");
            }

            return raisesIssue ? context.Issue(Descriptor) : null;
        }
    }
}
