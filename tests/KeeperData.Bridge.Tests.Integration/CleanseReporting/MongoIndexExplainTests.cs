using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.Reports;
using KeeperData.Core.Reports.Initialisation;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.CleanseReporting;

/// <summary>
/// TDD tests that prove missing indexes cause COLLSCAN on bulk query patterns.
/// Uses the production DI wiring and <see cref="ICleanseReportInitialisation"/> to
/// create indexes, then runs explain() on exact production query patterns.
///
/// Phase 1 (RED):  Tests FAIL because the production index managers do not yet
///                 create indexes for these query patterns.
/// Phase 2 (GREEN): Index managers are created/updated, tests pass.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class MongoIndexExplainTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly MongoDbFixture _mongoFixture;
    private ServiceProvider _serviceProvider = null!;
    private IMongoDatabase _db = null!;

    private const string TestDatabaseName = "test-index-explain";
    private const string IssuesCollectionName = "ca_issues";
    private const string IssueHistoryCollectionName = "ca_issue_history";
    private const string OperationsCollectionName = "ca_operations";

    public MongoIndexExplainTests(ITestOutputHelper output, MongoDbFixture mongoFixture)
    {
        _output = output;
        _mongoFixture = mongoFixture;
    }

    public async Task InitializeAsync()
    {
        await _mongoFixture.MongoClient.DropDatabaseAsync(TestDatabaseName);
        _db = _mongoFixture.MongoClient.GetDatabase(TestDatabaseName);

        _serviceProvider = BuildServiceProvider();

        // Run production initialisation — creates only the indexes that currently exist
        var initialisation = _serviceProvider.GetRequiredService<ICleanseReportInitialisation>();
        await initialisation.InitialiseAsync();
    }

    public async Task DisposeAsync()
    {
        await _serviceProvider.DisposeAsync();
        await _mongoFixture.MongoClient.DropDatabaseAsync(TestDatabaseName);
    }

    /// <summary>
    /// IssueAggRootRepository.DeactivateStaleAsync runs:
    ///   UpdateMany({ is_active: true, operation_id: { $ne: currentOpId } }, ...)
    ///
    /// Requires compound index on (is_active, operation_id).
    /// Currently missing from IssueIndexManager.
    /// </summary>
    [Fact]
    public async Task DeactivateStale_ShouldUseIndex_OnActiveAndOperationId()
    {
        var collection = _db.GetCollection<BsonDocument>(IssuesCollectionName);
        await SeedIssueDocumentsAsync(collection, count: 50);

        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("is_active", true),
            Builders<BsonDocument>.Filter.Ne("operation_id", "current-op"));

        var winningPlan = await GetExplainWinningPlanAsync(collection, filter);
        _output.WriteLine($"DeactivateStale explain:\n{winningPlan.ToJson(new JsonWriterSettings { Indent = true })}");

        AssertUsesIndexScan(winningPlan, "idx_issues_active_operationid");
    }

    /// <summary>
    /// IssueQueries.GetIssueHistoryAsync runs:
    ///   Find({ issue_id: X }).Sort({ occurred_at: 1 })
    ///
    /// Requires compound index on (issue_id, occurred_at).
    /// No index manager exists for the issue_history collection.
    /// </summary>
    [Fact]
    public async Task IssueHistory_GetByIssueId_ShouldUseIndex_OnIssueIdAndOccurredAt()
    {
        var collection = _db.GetCollection<BsonDocument>(IssueHistoryCollectionName);
        await SeedIssueHistoryDocumentsAsync(collection, count: 50);

        var filter = Builders<BsonDocument>.Filter.Eq("issue_id", "issue-001");
        var sort = Builders<BsonDocument>.Sort.Ascending("occurred_at");

        var winningPlan = await GetExplainWinningPlanAsync(collection, filter, sort);
        _output.WriteLine($"IssueHistory explain:\n{winningPlan.ToJson(new JsonWriterSettings { Indent = true })}");

        AssertUsesIndexScan(winningPlan, "idx_issue_history_issueid_occurredat");
    }

    /// <summary>
    /// CleanseAnalysisOperationsQueries.GetOperationsAsync runs:
    ///   Find({}).Sort({ started_at_utc: -1 }).Skip(n).Limit(m)
    ///
    /// Requires index on (started_at_utc: -1).
    /// No index manager exists for the operations collection.
    /// </summary>
    [Fact]
    public async Task Operations_SortByStartedAt_ShouldUseIndex()
    {
        var collection = _db.GetCollection<BsonDocument>(OperationsCollectionName);
        await SeedOperationDocumentsAsync(collection, count: 20);

        var filter = Builders<BsonDocument>.Filter.Empty;
        var sort = Builders<BsonDocument>.Sort.Descending("started_at_utc");

        var winningPlan = await GetExplainWinningPlanAsync(collection, filter, sort);
        _output.WriteLine($"Operations sort explain:\n{winningPlan.ToJson(new JsonWriterSettings { Indent = true })}");

        AssertUsesIndexScan(winningPlan, "idx_operations_started_at");
    }

    /// <summary>
    /// CleanseAnalysisOperationsQueries.GetCurrentOperationAsync runs:
    ///   Find({ status: "Running" }).FirstOrDefault()
    ///
    /// Requires index on (status).
    /// No index manager exists for the operations collection.
    /// </summary>
    [Fact]
    public async Task Operations_FilterByStatus_ShouldUseIndex()
    {
        var collection = _db.GetCollection<BsonDocument>(OperationsCollectionName);
        await SeedOperationDocumentsAsync(collection, count: 20);

        var filter = Builders<BsonDocument>.Filter.Eq("status", "Running");

        var winningPlan = await GetExplainWinningPlanAsync(collection, filter);
        _output.WriteLine($"Operations status explain:\n{winningPlan.ToJson(new JsonWriterSettings { Indent = true })}");

        AssertUsesIndexScan(winningPlan, "idx_operations_status");
    }

    #region Service provider

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole());

        var mongoConfig = new MongoConfig
        {
            DatabaseName = TestDatabaseName,
            DatabaseUri = "not-used-in-test",
            EnableTransactions = false,
            HealthcheckEnabled = false
        };
        services.AddSingleton(Options.Create(mongoConfig));
        services.AddSingleton<IOptions<IDatabaseConfig>>(Options.Create<IDatabaseConfig>(mongoConfig));
        services.AddSingleton(_mongoFixture.MongoClient);

        // Use production DI wiring — registers collections, index managers, initialisation
        services.AddCleanseReportDependencies();

        return services.BuildServiceProvider();
    }

    #endregion

    #region Explain helpers

    private static async Task<BsonDocument> GetExplainWinningPlanAsync(
        IMongoCollection<BsonDocument> collection,
        FilterDefinition<BsonDocument> filter,
        SortDefinition<BsonDocument>? sort = null)
    {
        var explainCommand = new BsonDocument
        {
            { "explain", new BsonDocument
                {
                    { "find", collection.CollectionNamespace.CollectionName },
                    { "filter", filter.Render(
                        collection.DocumentSerializer,
                        collection.Settings.SerializerRegistry) },
                }
            },
            { "verbosity", "executionStats" }
        };

        if (sort is not null)
        {
            explainCommand["explain"]["sort"] = sort.Render(
                collection.DocumentSerializer,
                collection.Settings.SerializerRegistry);
        }

        var explainResult = await collection.Database.RunCommandAsync<BsonDocument>(explainCommand);
        return explainResult["queryPlanner"]["winningPlan"].AsBsonDocument;
    }

    /// <summary>
    /// Walks the winning plan tree to find all stage names.
    /// Asserts that IXSCAN is present (with the expected index name) and COLLSCAN is absent.
    /// </summary>
    private void AssertUsesIndexScan(BsonDocument winningPlan, string expectedIndexName)
    {
        var stages = CollectStages(winningPlan);
        var stageNames = stages.Select(s => s.GetValue("stage", "").AsString).ToList();

        _output.WriteLine($"Plan stages: [{string.Join(" → ", stageNames)}]");

        stageNames.Should().Contain("IXSCAN",
            $"query should use an index scan, but got stages: [{string.Join(", ", stageNames)}]");
        stageNames.Should().NotContain("COLLSCAN",
            "query should not fall back to a collection scan");

        var ixScanStage = stages.First(s => s.GetValue("stage", "").AsString == "IXSCAN");
        ixScanStage.GetValue("indexName", "").AsString.Should().Be(expectedIndexName,
            $"the IXSCAN should use the expected index '{expectedIndexName}'");
    }

    private static List<BsonDocument> CollectStages(BsonDocument plan)
    {
        var stages = new List<BsonDocument> { plan };

        if (plan.TryGetValue("inputStage", out var inputStage) && inputStage.IsBsonDocument)
        {
            stages.AddRange(CollectStages(inputStage.AsBsonDocument));
        }

        if (plan.TryGetValue("inputStages", out var inputStages) && inputStages.IsBsonArray)
        {
            foreach (var child in inputStages.AsBsonArray.Where(s => s.IsBsonDocument))
            {
                stages.AddRange(CollectStages(child.AsBsonDocument));
            }
        }

        return stages;
    }

    #endregion

    #region Seed helpers

    private static async Task SeedIssueDocumentsAsync(IMongoCollection<BsonDocument> collection, int count)
    {
        var docs = Enumerable.Range(0, count).Select(i => new BsonDocument
        {
            { "_id", $"issue-{i:D4}" },
            { "operation_id", i % 2 == 0 ? "old-op" : "current-op" },
            { "issue_code", "CTS_CPH_NOT_IN_SAM" },
            { "rule_code", "2A" },
            { "error_code", "02A" },
            { "error_description", "Test issue" },
            { "cts_lid_full_identifier", $"UK-12/345/{6000 + i}" },
            { "cph", $"12/345/{6000 + i}" },
            { "created_at", DateTime.UtcNow },
            { "last_updated_at", DateTime.UtcNow },
            { "is_active", true },
            { "is_ignored", false },
            { "resolution_status", "None" }
        });

        await collection.InsertManyAsync(docs);
    }

    private static async Task SeedIssueHistoryDocumentsAsync(IMongoCollection<BsonDocument> collection, int count)
    {
        var docs = Enumerable.Range(0, count).Select(i => new BsonDocument
        {
            { "_id", $"hist-{i:D4}" },
            { "issue_id", i < 10 ? "issue-001" : $"issue-{i:D4}" },
            { "action", "Created" },
            { "performed_by", "system" },
            { "occurred_at", DateTime.UtcNow.AddMinutes(-count + i) }
        });

        await collection.InsertManyAsync(docs);
    }

    private static async Task SeedOperationDocumentsAsync(IMongoCollection<BsonDocument> collection, int count)
    {
        var docs = Enumerable.Range(0, count).Select(i => new BsonDocument
        {
            { "_id", $"op-{i:D4}" },
            { "status", i == 0 ? "Running" : "Completed" },
            { "started_at_utc", DateTime.UtcNow.AddHours(-count + i) },
            { "progress_percentage", i == 0 ? 50.0 : 100.0 },
            { "status_description", i == 0 ? "In progress" : "Done" },
            { "records_analyzed", 100 * i },
            { "total_records", 1000 },
            { "issues_found", 10 * i },
            { "issues_resolved", 5 * i }
        });

        await collection.InsertManyAsync(docs);
    }

    #endregion
}
