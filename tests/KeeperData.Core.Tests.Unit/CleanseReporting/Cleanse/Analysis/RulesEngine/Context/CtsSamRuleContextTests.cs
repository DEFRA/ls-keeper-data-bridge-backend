using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine.Context;

public class CtsSamRuleContextTests
{
    [Fact]
    public void ExistenceFlags_ShouldReflectTheHoldingsSupplied()
    {
        CtsSamRuleContextBuilder.Build().ExistsInBoth.Should().BeTrue();

        var ctsOnly = CtsSamRuleContextBuilder.Build(includeSam: false);
        ctsOnly.ExistsInCts.Should().BeTrue();
        ctsOnly.ExistsInSam.Should().BeFalse();
        ctsOnly.ExistsInBoth.Should().BeFalse();

        var samOnly = CtsSamRuleContextBuilder.Build(includeCts: false);
        samOnly.ExistsInCts.Should().BeFalse();
        samOnly.ExistsInSam.Should().BeTrue();
        samOnly.ExistsInBoth.Should().BeFalse();
    }

    [Fact]
    public void ContactCollections_WhenHoldingIsMissing_ShouldBeEmptyRatherThanNull()
    {
        var context = CtsSamRuleContextBuilder.Build(includeCts: false, includeSam: false);

        context.CtsEmails.Should().BeEmpty();
        context.SamEmails.Should().BeEmpty();
        context.CtsPhones.Should().BeEmpty();
        context.SamPhones.Should().BeEmpty();
        context.EmailsMissingFromSam.Should().BeEmpty();
        context.PhonesMissingFromSam.Should().BeEmpty();
    }

    [Fact]
    public void MissingFromSam_ShouldReturnOnlyTheValuesSamDoesNotHold()
    {
        var context = CtsSamRuleContextBuilder.Build(
            ctsEmails: ["a@farm.uk", "b@farm.uk"],
            samEmails: ["a@farm.uk"],
            ctsPhones: ["0111", "0222"],
            samPhones: ["0222"]);

        context.EmailsMissingFromSam.Should().BeEquivalentTo(["b@farm.uk"]);
        context.PhonesMissingFromSam.Should().BeEquivalentTo(["0111"]);
    }

    [Fact]
    public void DerivedCollections_ShouldBeComputedOnceAndReused()
    {
        var context = CtsSamRuleContextBuilder.Build(ctsEmails: ["a@farm.uk"]);

        context.EmailsMissingFromSam.Should().BeSameAs(context.EmailsMissingFromSam);
        context.PhonesMissingFromSam.Should().BeSameAs(context.PhonesMissingFromSam);
        context.CtsEmails.Should().BeSameAs(context.CtsEmails);
        context.SamPhones.Should().BeSameAs(context.SamPhones);
    }

    [Fact]
    public void Issue_ShouldCarryRecordIdentityAndApplyTheRuleContext()
    {
        var context = CtsSamRuleContextBuilder.Build();
        var descriptor = new RuleDescriptor("TEST_RULE", "99", "99", "Test rule", "TEST");

        var result = context.Issue(descriptor, x => x.AnimalSpeciesCode = "SHP");

        result.Descriptor.Should().Be(descriptor);
        result.IssueContext!.SamCph.Should().Be(CtsSamRuleContextBuilder.DefaultCph);
        result.IssueContext.CtsLidFullIdentifier.Should().Be(CtsSamRuleContextBuilder.DefaultLid);
        result.IssueContext.AnimalSpeciesCode.Should().Be("SHP");
    }

    [Fact]
    public void Issue_OnTheSamPass_ShouldNotCarryALidFullIdentifier()
    {
        var context = CtsSamRuleContextBuilder.Build(pass: AnalysisPass.SamPrimary);
        var descriptor = new RuleDescriptor("TEST_RULE", "99", "99", "Test rule", "TEST");

        var result = context.Issue(descriptor);

        result.IssueContext!.CtsLidFullIdentifier.Should().BeNull();
        result.IssueContext.SamCph.Should().Be(CtsSamRuleContextBuilder.DefaultCph);
    }
}
