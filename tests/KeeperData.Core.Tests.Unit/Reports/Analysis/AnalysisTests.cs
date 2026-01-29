using FluentAssertions;
using KeeperData.Core.Reports.Analysis;

namespace KeeperData.Core.Tests.Unit.Reports.Analysis;

public class RuleResultTests
{
    [Fact]
    public void NoIssue_CreatesResultWithHasIssueFalse()
    {
        var result = RuleResult.NoIssue();

        result.HasIssue.Should().BeFalse();
        result.IssueCode.Should().BeNull();
        result.ContextData.Should().BeNull();
    }

    [Fact]
    public void Issue_WithCodeOnly_CreatesCorrectResult()
    {
        var result = RuleResult.Issue("ERR001");

        result.HasIssue.Should().BeTrue();
        result.IssueCode.Should().Be("ERR001");
        result.ContextData.Should().BeNull();
    }

    [Fact]
    public void Issue_WithCodeAndContextData_CreatesCorrectResult()
    {
        var contextData = new Dictionary<string, object?> 
        { 
            { "field", "email" }, 
            { "value", null } 
        };

        var result = RuleResult.Issue("ERR002", contextData);

        result.HasIssue.Should().BeTrue();
        result.IssueCode.Should().Be("ERR002");
        result.ContextData.Should().NotBeNull();
        result.ContextData.Should().ContainKey("field");
        result.ContextData!["field"].Should().Be("email");
    }

    [Fact]
    public void Record_IsSealed()
    {
        typeof(RuleResult).IsSealed.Should().BeTrue();
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var result1 = RuleResult.NoIssue();
        var result2 = RuleResult.NoIssue();
        var result3 = RuleResult.Issue("ERR001");

        result1.Should().Be(result2);
        result1.Should().NotBe(result3);
    }
}

public class PipelineRuleResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var ruleResult = RuleResult.Issue("ERR001");

        var pipelineResult = new PipelineRuleResult
        {
            Result = ruleResult,
            RuleCode = "RULE001"
        };

        pipelineResult.Result.Should().Be(ruleResult);
        pipelineResult.RuleCode.Should().Be("RULE001");
    }

    [Fact]
    public void StopProcessing_DefaultsToFalse()
    {
        var pipelineResult = new PipelineRuleResult
        {
            Result = RuleResult.NoIssue(),
            RuleCode = "RULE001"
        };

        pipelineResult.StopProcessing.Should().BeFalse();
    }

    [Fact]
    public void StopProcessing_CanBeSetToTrue()
    {
        var pipelineResult = new PipelineRuleResult
        {
            Result = RuleResult.Issue("CRITICAL"),
            RuleCode = "RULE_CRITICAL",
            StopProcessing = true
        };

        pipelineResult.StopProcessing.Should().BeTrue();
    }

    [Fact]
    public void Record_IsSealed()
    {
        typeof(PipelineRuleResult).IsSealed.Should().BeTrue();
    }
}

public class StrategyMetricsTests
{
    [Fact]
    public void DefaultValues_AreZero()
    {
        var metrics = new StrategyMetrics();

        metrics.RecordsAnalyzed.Should().Be(0);
        metrics.IssuesFound.Should().Be(0);
        metrics.IssuesResolved.Should().Be(0);
    }

    [Fact]
    public void Properties_CanBeSet()
    {
        var metrics = new StrategyMetrics
        {
            RecordsAnalyzed = 1000,
            IssuesFound = 50,
            IssuesResolved = 30
        };

        metrics.RecordsAnalyzed.Should().Be(1000);
        metrics.IssuesFound.Should().Be(50);
        metrics.IssuesResolved.Should().Be(30);
    }

    [Fact]
    public void Properties_CanBeModified()
    {
        var metrics = new StrategyMetrics();

        metrics.RecordsAnalyzed++;
        metrics.IssuesFound += 5;
        metrics.IssuesResolved += 2;

        metrics.RecordsAnalyzed.Should().Be(1);
        metrics.IssuesFound.Should().Be(5);
        metrics.IssuesResolved.Should().Be(2);
    }

    [Fact]
    public void Class_IsSealed()
    {
        typeof(StrategyMetrics).IsSealed.Should().BeTrue();
    }
}

public class IssueRecordResultTests
{
    [Theory]
    [InlineData(IssueRecordResult.Created)]
    [InlineData(IssueRecordResult.Reactivated)]
    [InlineData(IssueRecordResult.Resolved)]
    [InlineData(IssueRecordResult.NoChange)]
    public void Enum_HasExpectedValues(IssueRecordResult result)
    {
        Enum.IsDefined(typeof(IssueRecordResult), result).Should().BeTrue();
    }

    [Fact]
    public void Enum_HasFourValues()
    {
        var values = Enum.GetValues<IssueRecordResult>();

        values.Should().HaveCount(4);
    }
}
