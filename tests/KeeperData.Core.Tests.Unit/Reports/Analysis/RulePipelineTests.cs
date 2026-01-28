using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Rules;
using Moq;

namespace KeeperData.Core.Tests.Unit.Reports.Analysis;

public class RuleDescriptorTests
{
    [Fact]
    public void RequiredProperties_CanBeSet()
    {
        var mockRule = new Mock<ICleanseRule<string>>();
        mockRule.Setup(r => r.RuleCode).Returns("TEST_RULE");
        Func<RuleResult, bool> predicate = r => r.HasIssue;

        var descriptor = new RuleDescriptor<string>
        {
            Rule = mockRule.Object,
            ShouldStopProcessing = predicate
        };

        descriptor.Rule.Should().NotBeNull();
        descriptor.Rule.RuleCode.Should().Be("TEST_RULE");
        descriptor.ShouldStopProcessing.Should().NotBeNull();
    }

    [Fact]
    public void ShouldStopProcessing_CanEvaluatePredicate()
    {
        var mockRule = new Mock<ICleanseRule<string>>();
        Func<RuleResult, bool> predicate = r => r.HasIssue;

        var descriptor = new RuleDescriptor<string>
        {
            Rule = mockRule.Object,
            ShouldStopProcessing = predicate
        };

        var issueResult = RuleResult.Issue("TEST");
        var noIssueResult = RuleResult.NoIssue();

        descriptor.ShouldStopProcessing(issueResult).Should().BeTrue();
        descriptor.ShouldStopProcessing(noIssueResult).Should().BeFalse();
    }
}

public class RulePipelineBuilderTests
{
    [Fact]
    public void Create_ReturnsNewInstance()
    {
        var builder = RulePipelineBuilder<string>.Create();

        builder.Should().NotBeNull();
    }

    [Fact]
    public void Build_WithNoRules_ReturnsEmptyPipeline()
    {
        var pipeline = RulePipelineBuilder<string>.Create().Build();

        pipeline.Should().NotBeNull();
    }

    [Fact]
    public void AddRule_WithContinueAlways_AddsRuleWithNeverStopPredicate()
    {
        var mockRule = new Mock<ICleanseRule<string>>();
        mockRule.Setup(r => r.RuleCode).Returns("RULE1");

        var pipeline = RulePipelineBuilder<string>.Create()
            .AddRule(mockRule.Object).ContinueAlways()
            .Build();

        pipeline.Should().NotBeNull();
    }

    [Fact]
    public void AddRule_WithStopOnIssue_AddsRuleWithIssueStopPredicate()
    {
        var mockRule = new Mock<ICleanseRule<string>>();
        mockRule.Setup(r => r.RuleCode).Returns("RULE1");

        var pipeline = RulePipelineBuilder<string>.Create()
            .AddRule(mockRule.Object).StopOnIssue()
            .Build();

        pipeline.Should().NotBeNull();
    }

    [Fact]
    public void AddRule_WithStopProcessingWhen_AddsRuleWithCustomPredicate()
    {
        var mockRule = new Mock<ICleanseRule<string>>();
        mockRule.Setup(r => r.RuleCode).Returns("RULE1");

        var pipeline = RulePipelineBuilder<string>.Create()
            .AddRule(mockRule.Object).StopProcessingWhen(r => r.IssueCode == "CRITICAL")
            .Build();

        pipeline.Should().NotBeNull();
    }

    [Fact]
    public void FluentChaining_CanAddMultipleRules()
    {
        var mockRule1 = new Mock<ICleanseRule<string>>();
        var mockRule2 = new Mock<ICleanseRule<string>>();
        var mockRule3 = new Mock<ICleanseRule<string>>();
        mockRule1.Setup(r => r.RuleCode).Returns("RULE1");
        mockRule2.Setup(r => r.RuleCode).Returns("RULE2");
        mockRule3.Setup(r => r.RuleCode).Returns("RULE3");

        var pipeline = RulePipelineBuilder<string>.Create()
            .AddRule(mockRule1.Object).ContinueAlways()
            .AddRule(mockRule2.Object).StopOnIssue()
            .AddRule(mockRule3.Object).ContinueAlways()
            .Build();

        pipeline.Should().NotBeNull();
    }
}

public class RulePipelineTests
{
    private readonly Mock<IAnalysisContext> _mockContext;

    public RulePipelineTests()
    {
        _mockContext = new Mock<IAnalysisContext>();
        _mockContext.Setup(c => c.OperationId).Returns("test-op");
    }

    [Fact]
    public async Task ExecuteAsync_WithNoRules_ReturnsEmptyResults()
    {
        var pipeline = new RulePipeline<string>(new List<RuleDescriptor<string>>());

        var results = await pipeline.ExecuteAsync("input", _mockContext.Object, CancellationToken.None);

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_WithSingleRule_ExecutesRule()
    {
        var mockRule = new Mock<ICleanseRule<string>>();
        mockRule.Setup(r => r.RuleCode).Returns("RULE1");
        mockRule.Setup(r => r.ExecuteAsync(It.IsAny<string>(), It.IsAny<IAnalysisContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(RuleResult.NoIssue());

        var descriptor = new RuleDescriptor<string>
        {
            Rule = mockRule.Object,
            ShouldStopProcessing = _ => false
        };

        var pipeline = new RulePipeline<string>(new List<RuleDescriptor<string>> { descriptor });

        var results = await pipeline.ExecuteAsync("input", _mockContext.Object, CancellationToken.None);

        results.Should().ContainSingle();
        results[0].RuleCode.Should().Be("RULE1");
        results[0].Result.HasIssue.Should().BeFalse();
        results[0].StopProcessing.Should().BeFalse();
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleRules_ExecutesAllRules()
    {
        var mockRule1 = CreateMockRule("RULE1", RuleResult.NoIssue());
        var mockRule2 = CreateMockRule("RULE2", RuleResult.NoIssue());
        var mockRule3 = CreateMockRule("RULE3", RuleResult.NoIssue());

        var descriptors = new List<RuleDescriptor<string>>
        {
            new() { Rule = mockRule1.Object, ShouldStopProcessing = _ => false },
            new() { Rule = mockRule2.Object, ShouldStopProcessing = _ => false },
            new() { Rule = mockRule3.Object, ShouldStopProcessing = _ => false }
        };

        var pipeline = new RulePipeline<string>(descriptors);

        var results = await pipeline.ExecuteAsync("input", _mockContext.Object, CancellationToken.None);

        results.Should().HaveCount(3);
        results[0].RuleCode.Should().Be("RULE1");
        results[1].RuleCode.Should().Be("RULE2");
        results[2].RuleCode.Should().Be("RULE3");
    }

    [Fact]
    public async Task ExecuteAsync_WhenRuleStopsProcessing_StopsExecution()
    {
        var mockRule1 = CreateMockRule("RULE1", RuleResult.Issue("ISSUE1"));
        var mockRule2 = CreateMockRule("RULE2", RuleResult.NoIssue());

        var descriptors = new List<RuleDescriptor<string>>
        {
            new() { Rule = mockRule1.Object, ShouldStopProcessing = r => r.HasIssue },
            new() { Rule = mockRule2.Object, ShouldStopProcessing = _ => false }
        };

        var pipeline = new RulePipeline<string>(descriptors);

        var results = await pipeline.ExecuteAsync("input", _mockContext.Object, CancellationToken.None);

        results.Should().ContainSingle();
        results[0].RuleCode.Should().Be("RULE1");
        results[0].StopProcessing.Should().BeTrue();

        // Rule2 should not have been executed
        mockRule2.Verify(r => r.ExecuteAsync(It.IsAny<string>(), It.IsAny<IAnalysisContext>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_WhenMiddleRuleStops_ExecutesPreviousRulesOnly()
    {
        var mockRule1 = CreateMockRule("RULE1", RuleResult.NoIssue());
        var mockRule2 = CreateMockRule("RULE2", RuleResult.Issue("CRITICAL"));
        var mockRule3 = CreateMockRule("RULE3", RuleResult.NoIssue());

        var descriptors = new List<RuleDescriptor<string>>
        {
            new() { Rule = mockRule1.Object, ShouldStopProcessing = _ => false },
            new() { Rule = mockRule2.Object, ShouldStopProcessing = r => r.HasIssue },
            new() { Rule = mockRule3.Object, ShouldStopProcessing = _ => false }
        };

        var pipeline = new RulePipeline<string>(descriptors);

        var results = await pipeline.ExecuteAsync("input", _mockContext.Object, CancellationToken.None);

        results.Should().HaveCount(2);
        results[0].RuleCode.Should().Be("RULE1");
        results[0].StopProcessing.Should().BeFalse();
        results[1].RuleCode.Should().Be("RULE2");
        results[1].StopProcessing.Should().BeTrue();

        mockRule3.Verify(r => r.ExecuteAsync(It.IsAny<string>(), It.IsAny<IAnalysisContext>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_PassesInputToAllRules()
    {
        var mockRule = new Mock<ICleanseRule<string>>();
        mockRule.Setup(r => r.RuleCode).Returns("RULE1");
        mockRule.Setup(r => r.ExecuteAsync("test-input", It.IsAny<IAnalysisContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(RuleResult.NoIssue());

        var descriptor = new RuleDescriptor<string>
        {
            Rule = mockRule.Object,
            ShouldStopProcessing = _ => false
        };

        var pipeline = new RulePipeline<string>(new List<RuleDescriptor<string>> { descriptor });

        await pipeline.ExecuteAsync("test-input", _mockContext.Object, CancellationToken.None);

        mockRule.Verify(r => r.ExecuteAsync("test-input", _mockContext.Object, It.IsAny<CancellationToken>()), Times.Once);
    }

    private Mock<ICleanseRule<string>> CreateMockRule(string ruleCode, RuleResult result)
    {
        var mock = new Mock<ICleanseRule<string>>();
        mock.Setup(r => r.RuleCode).Returns(ruleCode);
        mock.Setup(r => r.ExecuteAsync(It.IsAny<string>(), It.IsAny<IAnalysisContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(result);
        return mock;
    }
}
