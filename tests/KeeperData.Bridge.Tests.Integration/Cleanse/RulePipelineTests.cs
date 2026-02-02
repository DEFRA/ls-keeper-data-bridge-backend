using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Rules;
using KeeperData.Core.Reports.Strategies;
using KeeperData.Core.Reports.Strategies.Rules;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Cleanse;

/// <summary>
/// Integration tests for the rule pipeline and individual rules.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class RulePipelineTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly string _testDatabaseName = "rule-pipeline-test-db";

    private IQueryService _queryService = null!;
    private DataSetDefinitions _dataSets = null!;
    private IAnalysisContext _context = null!;

    private const string SamCphHoldingCollection = "sam_cph_holdings";

    public RulePipelineTests(
        ITestOutputHelper testOutputHelper,
        MongoDbFixture mongoDbFixture)
    {
        _testOutputHelper = testOutputHelper;
        _mongoDbFixture = mongoDbFixture;
        _mongoClient = _mongoDbFixture.MongoClient;
    }

    public async Task InitializeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Initialized test database: {_testDatabaseName}");

        InitializeServices();
    }

    public async Task DisposeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Cleaned up test database: {_testDatabaseName}");
    }

    private void InitializeServices()
    {
        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = _testDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString
        });

        _dataSets = StandardDataSetDefinitionsBuilder.Build();

        var queryServiceLogger = new Mock<ILogger<QueryService>>();
        _queryService = new QueryService(
            _mongoClient,
            mongoConfig,
            _dataSets,
            queryServiceLogger.Object);

        _context = new AnalysisContext("test-operation", _queryService, _dataSets);
    }

    #region SamCphRecordDoesNotExistRule Tests

    [Fact]
    public async Task SamCphRecordDoesNotExistRule_WhenSamRecordExists_ShouldReturnNoIssue()
    {
        // Arrange
        await InsertSamCphHoldingAsync("12/345/6789");
        
        var rule = new SamCphRecordDoesNotExistRule();
        var input = CreateRuleInput("AH-12/345/6789");

        // Act
        var result = await rule.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert
        result.HasIssue.Should().BeFalse();
        result.IssueCode.Should().BeNull();
        input.SamCphHoldingRecord.Should().NotBeNull("Rule should populate SAM record for subsequent rules");

        _testOutputHelper.WriteLine("Rule correctly returns no issue when SAM record exists");
    }

    [Fact]
    public async Task SamCphRecordDoesNotExistRule_WhenSamRecordMissing_ShouldReturnIssue()
    {
        // Arrange - No SAM record inserted
        var rule = new SamCphRecordDoesNotExistRule();
        var input = CreateRuleInput("AH-12/345/6789");

        // Act
        var result = await rule.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert
        result.HasIssue.Should().BeTrue();
        result.IssueCode.Should().Be(IssueCodes.CTS_CPH_NOT_IN_SAM);
        result.ContextData.Should().NotBeNull();
        result.ContextData!["Cph"].Should().Be("12/345/6789");
        input.SamCphHoldingRecord.Should().BeNull();

        _testOutputHelper.WriteLine($"Rule correctly returns issue: {result.IssueCode}");
    }

    [Fact]
    public async Task SamCphRecordDoesNotExistRule_ShouldUseCache()
    {
        // Arrange
        await InsertSamCphHoldingAsync("12/345/6789");
        
        var rule = new SamCphRecordDoesNotExistRule();
        var input1 = CreateRuleInput("AH-12/345/6789");
        var input2 = CreateRuleInput("AH-12/345/6789");

        // Act
        var result1 = await rule.ExecuteAsync(input1, _context, CancellationToken.None);
        
        // Delete the SAM record
        await DeleteSamCphHoldingAsync("12/345/6789");
        
        var result2 = await rule.ExecuteAsync(input2, _context, CancellationToken.None);

        // Assert - Second call should use cached result
        result1.HasIssue.Should().BeFalse();
        result2.HasIssue.Should().BeFalse("Cached result should be used");

        _testOutputHelper.WriteLine("Rule correctly uses context cache");
    }

    #endregion

    #region SamCphRecordComparisonRule Tests

    [Fact]
    public async Task SamCphRecordComparisonRule_WhenSamRecordNotPopulated_ShouldReturnNoIssue()
    {
        // Arrange
        var rule = new SamCphRecordNoEmailAddressesRule();
        var input = CreateRuleInput("AH-12/345/6789");
        // SamCphHoldingRecord is not set

        // Act
        var result = await rule.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert
        result.HasIssue.Should().BeFalse();

        _testOutputHelper.WriteLine("Comparison rule skips when SAM record not populated");
    }

    [Fact]
    public async Task SamCphRecordComparisonRule_WhenSamRecordPopulated_ShouldExecuteRule()
    {
        // Arrange
        // Insert SAM Herd with party IDs
        await InsertSamHerdAsync("12/345/6789-001", "PARTY001", "PARTY002");
        // Insert SAM Party with email address
        await InsertSamPartyAsync("PARTY001", "test@example.com");
        
        var rule = new SamCphRecordNoEmailAddressesRule();
        var input = CreateRuleInput("AH-12/345/6789");
        input.SamCphHoldingRecord = new Dictionary<string, object?>
        {
            ["CPH"] = "12/345/6789"
        };

        // Act
        var result = await rule.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert - No issue because email address exists
        result.HasIssue.Should().BeFalse();

        _testOutputHelper.WriteLine("Comparison rule correctly processes SAM data");
    }

    [Fact]
    public async Task SamCphRecordComparisonRule_WhenNoEmailAddresses_ShouldReturnIssue()
    {
        // Arrange
        // Insert SAM Herd with party IDs but no email addresses in SAM Party
        await InsertSamHerdAsync("12/345/6789-001", "PARTY001", "PARTY002");
        await InsertSamPartyAsync("PARTY001", null);
        
        var rule = new SamCphRecordNoEmailAddressesRule();
        var input = CreateRuleInput("AH-12/345/6789");
        input.SamCphHoldingRecord = new Dictionary<string, object?>
        {
            ["CPH"] = "12/345/6789"
        };

        // Act
        var result = await rule.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert - Issue because no email addresses exist
        result.HasIssue.Should().BeTrue();
        result.IssueCode.Should().Be("CTS_SAM_NO_EMAIL_ADDRESSES");

        _testOutputHelper.WriteLine("Comparison rule correctly identifies missing email addresses");
    }

    #endregion

    #region Pipeline Builder Tests

    [Fact]
    public async Task Pipeline_StopOnIssue_ShouldStopProcessingAfterIssue()
    {
        // Arrange - No SAM record, so first rule will find an issue
        var pipeline = RulePipelineBuilder<CtsSamRuleInput>.Create()
            .AddRule(new SamCphRecordDoesNotExistRule())
                .StopOnIssue()
            .AddRule(new SamCphRecordNoEmailAddressesRule())
                .ContinueAlways()
            .Build();

        var input = CreateRuleInput("AH-12/345/6789");

        // Act
        var results = await pipeline.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert
        results.Should().HaveCount(1, "Pipeline should stop after first rule finds issue");
        results[0].Result.HasIssue.Should().BeTrue();
        results[0].StopProcessing.Should().BeTrue();
        results[0].RuleCode.Should().Be(IssueCodes.CTS_CPH_NOT_IN_SAM);

        _testOutputHelper.WriteLine("Pipeline correctly stops on issue");
    }

    [Fact]
    public async Task Pipeline_ContinueAlways_ShouldProcessAllRules()
    {
        // Arrange - SAM record exists and SAM Party with email exists
        await InsertSamCphHoldingAsync("12/345/6789");
        await InsertSamHerdAsync("12/345/6789-001", "PARTY001", null);
        await InsertSamPartyAsync("PARTY001", "test@example.com");
        
        var pipeline = RulePipelineBuilder<CtsSamRuleInput>.Create()
            .AddRule(new SamCphRecordDoesNotExistRule())
                .StopOnIssue()
            .AddRule(new SamCphRecordNoEmailAddressesRule())
                .ContinueAlways()
            .Build();

        var input = CreateRuleInput("AH-12/345/6789");

        // Act
        var results = await pipeline.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert
        results.Should().HaveCount(2, "Pipeline should process all rules when no issues");
        results[0].Result.HasIssue.Should().BeFalse();
        results[0].StopProcessing.Should().BeFalse();
        results[1].Result.HasIssue.Should().BeFalse();

        _testOutputHelper.WriteLine("Pipeline correctly processes all rules when no issues");
    }

    [Fact]
    public async Task Pipeline_CustomStopCondition_ShouldRespectPredicate()
    {
        // Arrange
        var executionCount = 0;
        var customRule = new TestRule("TEST_RULE", _ =>
        {
            executionCount++;
            return RuleResult.NoIssue();
        });

        var pipeline = RulePipelineBuilder<CtsSamRuleInput>.Create()
            .AddRule(customRule)
                .StopProcessingWhen(_ => true) // Always stop
            .AddRule(customRule)
                .ContinueAlways()
            .Build();

        var input = CreateRuleInput("AH-12/345/6789");

        // Act
        var results = await pipeline.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert
        results.Should().HaveCount(1);
        results[0].StopProcessing.Should().BeTrue();
        executionCount.Should().Be(1, "Second rule should not execute");

        _testOutputHelper.WriteLine("Custom stop condition correctly stops pipeline");
    }

    [Fact]
    public async Task Pipeline_EmptyPipeline_ShouldReturnEmptyResults()
    {
        // Arrange
        var pipeline = RulePipelineBuilder<CtsSamRuleInput>.Create().Build();
        var input = CreateRuleInput("AH-12/345/6789");

        // Act
        var results = await pipeline.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert
        results.Should().BeEmpty();

        _testOutputHelper.WriteLine("Empty pipeline returns empty results");
    }

    #endregion

    #region Pipeline with Data Enrichment Tests

    [Fact]
    public async Task Pipeline_RulesShouldShareEnrichedData()
    {
        // Arrange
        await InsertSamCphHoldingAsync("12/345/6789");
        await InsertSamHerdAsync("12/345/6789-001", "PARTY001", null);
        await InsertSamPartyAsync("PARTY001", "test@example.com");

        var pipeline = RulePipelineBuilder<CtsSamRuleInput>.Create()
            .AddRule(new SamCphRecordDoesNotExistRule())
                .ContinueAlways()
            .AddRule(new SamCphRecordNoEmailAddressesRule())
                .ContinueAlways()
            .Build();

        var input = CreateRuleInput("AH-12/345/6789");

        // Act
        var results = await pipeline.ExecuteAsync(input, _context, CancellationToken.None);

        // Assert
        input.SamCphHoldingRecord.Should().NotBeNull("First rule should enrich with SAM Holding");
        results.Should().HaveCount(2, "Pipeline should process both rules");
        results[0].Result.HasIssue.Should().BeFalse();
        results[1].Result.HasIssue.Should().BeFalse();

        _testOutputHelper.WriteLine("Rules correctly share enriched data through input object");
    }

    #endregion

    #region Helper Methods

    private const string SamHerdCollection = "sam_herd";
    private const string SamPartyCollection = "sam_party";

    private static CtsSamRuleInput CreateRuleInput(string lidFullIdentifier)
    {
        var parsed = LidFullIdentifier.Parse(lidFullIdentifier);
        return new CtsSamRuleInput
        {
            CtsRecord = new Dictionary<string, object?>
            {
                ["LID_FULL_IDENTIFIER"] = lidFullIdentifier
            },
            LidFullIdentifier = parsed,
            Thumbprint = $"thumbprint-{lidFullIdentifier}"
        };
    }

    private async Task InsertSamCphHoldingAsync(string cph)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(SamCphHoldingCollection);

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "CPH", cph },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    private async Task DeleteSamCphHoldingAsync(string cph)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(SamCphHoldingCollection);

        await collection.DeleteOneAsync(d => d["CPH"] == cph);
    }

    private async Task InsertSamHerdAsync(string cphh, string? ownerPartyIds, string? keeperPartyIds)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(SamHerdCollection);

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "CPHH", cphh },
            { "OWNER_PARTY_IDS", ownerPartyIds is not null ? BsonValue.Create(ownerPartyIds) : BsonNull.Value },
            { "KEEPER_PARTY_IDS", keeperPartyIds is not null ? BsonValue.Create(keeperPartyIds) : BsonNull.Value },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    private async Task InsertSamPartyAsync(string partyId, string? emailAddress)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(SamPartyCollection);

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "PARTY_ID", partyId },
            { "INTERNET_EMAIL_ADDRESS", emailAddress is not null ? BsonValue.Create(emailAddress) : BsonNull.Value },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    #endregion

    #region Test Helpers

    private class TestRule : ICleanseRule<CtsSamRuleInput>
    {
        private readonly Func<CtsSamRuleInput, RuleResult> _execute;

        public TestRule(string ruleCode, Func<CtsSamRuleInput, RuleResult> execute)
        {
            RuleCode = ruleCode;
            _execute = execute;
        }

        public string RuleCode { get; }

        public Task<RuleResult> ExecuteAsync(CtsSamRuleInput input, IAnalysisContext context, CancellationToken ct)
        {
            return Task.FromResult(_execute(input));
        }
    }

    #endregion
}
