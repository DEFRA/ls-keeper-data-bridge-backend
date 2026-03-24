using KeeperData.Infrastructure.Benchmarking.Analysis;
using KeeperData.Infrastructure.Benchmarking.Metrics;
using KeeperData.Infrastructure.Benchmarking.Models;
using KeeperData.Infrastructure.Benchmarking.Scenarios;
using KeeperData.Infrastructure.Benchmarking.Throttling;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using System.Diagnostics;

namespace KeeperData.Infrastructure.Benchmarking.Services;

/// <inheritdoc />
public sealed class BenchmarkOrchestrator : IBenchmarkOrchestrator
{
    private readonly MongoClientSettings _baseSettings;
    private readonly IBenchmarkThrottler _throttler;
    private readonly ILogger<BenchmarkOrchestrator> _logger;

    private volatile BenchmarkReport? _lastReport;
    private int _running;

    public BenchmarkOrchestrator(
        MongoClientSettings baseSettings,
        IBenchmarkThrottler throttler,
        ILogger<BenchmarkOrchestrator> logger)
    {
        _baseSettings = baseSettings;
        _throttler = throttler;
        _logger = logger;
    }

    public BenchmarkReport? LastReport => _lastReport;
    public bool IsRunning => _running == 1;

    public async Task<BenchmarkReport?> RunAsync(BenchmarkConfig config, CancellationToken ct)
    {
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
        {
            return null; // already running
        }

        var overallSw = Stopwatch.StartNew();
        var eventCollector = new DriverEventCollector();
        IMongoClient? benchClient = null;

        try
        {
            benchClient = CreateInstrumentedClient(eventCollector);
            var report = await ExecuteBenchmarkAsync(config, benchClient, eventCollector, overallSw, ct);
            _lastReport = report;
            return _lastReport;
        }
        catch (OperationCanceledException)
        {
            overallSw.Stop();
            _lastReport = BuildReport(config, "Cancelled", overallSw, eventCollector);
            return _lastReport;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Benchmark run failed");
            overallSw.Stop();
            _lastReport = BuildReport(config, $"Failed: {ex.Message}", overallSw, eventCollector);
            return _lastReport;
        }
        finally
        {
            if (benchClient is not null)
            {
                await TearDownAsync(benchClient, config.CollectionPrefix);
            }

            Interlocked.Exchange(ref _running, 0);
        }
    }

    private async Task<BenchmarkReport> ExecuteBenchmarkAsync(
        BenchmarkConfig config,
        IMongoClient benchClient,
        DriverEventCollector eventCollector,
        Stopwatch overallSw,
        CancellationToken ct)
    {
        var db = GetBenchmarkDatabase(benchClient);
        var prefix = config.CollectionPrefix;
        var sourceCol = db.GetCollection<BsonDocument>($"{prefix}source");
        var lookupCol = db.GetCollection<BsonDocument>($"{prefix}lookup");
        var writeCol = db.GetCollection<BsonDocument>($"{prefix}write");

        _logger.LogInformation("Benchmark: creating collections and indexes");
        await CreateIndexesAsync(sourceCol, lookupCol, writeCol, ct);

        _logger.LogInformation("Benchmark: seeding {Count} deterministic records", config.SeedCount);
        var baseDate = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var seedCtx = new SeedContext(sourceCol, lookupCol, config.SeedCount, baseDate, config.ThrottleDelay, _throttler);
        await SeedDataAsync(seedCtx, ct);

        var datasetFingerprints = await CollectDatasetFingerprintsAsync(db, prefix, ct);
        var indexFingerprints = await CollectIndexFingerprintsAsync(db, prefix, ct);

        var scenarioResults = await RunScenariosAsync(config, sourceCol, lookupCol, writeCol, baseDate, ct);
        var explains = await CollectExplainPlansAsync(sourceCol, baseDate, ct);

        var status = ct.IsCancellationRequested ? "Cancelled" : "Completed";
        overallSw.Stop();

        return BuildReport(config, status, overallSw, eventCollector,
            datasetFingerprints, indexFingerprints, scenarioResults, explains);
    }

    private async Task<List<ScenarioResult>> RunScenariosAsync(
        BenchmarkConfig config,
        IMongoCollection<BsonDocument> sourceCol,
        IMongoCollection<BsonDocument> lookupCol,
        IMongoCollection<BsonDocument> writeCol,
        DateTime baseDate,
        CancellationToken ct)
    {
        var scenarios = new IBenchmarkScenario[]
        {
            new PointLookupScenario(sourceCol, config.SeedCount),
            new RangeQueryScenario(sourceCol, baseDate),
            new AggregationScenario(sourceCol),
            new BulkWriteScenario(writeCol),
            new MiniEtlScenario(sourceCol, lookupCol, writeCol, config.SeedCount)
        };

        _logger.LogInformation("Benchmark: running {Count} scenarios for {Duration}",
            scenarios.Length, config.Duration);

        var results = new List<ScenarioResult>();
        foreach (var scenario in scenarios)
        {
            if (ct.IsCancellationRequested) break;

            _logger.LogInformation("Benchmark: starting scenario {Name}", scenario.Name);
            var result = await scenario.RunAsync(config, _throttler, ct);
            results.Add(result);
            _logger.LogInformation(
                "Benchmark: scenario {Name} finished – {Ops} ops, {Errors} errors, {OpsPerSec} ops/s",
                result.ScenarioName, result.TotalOperations, result.ErrorCount, result.OpsPerSecond);
        }

        return results;
    }

    private static BenchmarkReport BuildReport(
        BenchmarkConfig config,
        string status,
        Stopwatch overallSw,
        DriverEventCollector eventCollector,
        IReadOnlyList<DatasetFingerprint>? datasetFingerprints = null,
        IReadOnlyList<IndexFingerprint>? indexFingerprints = null,
        IReadOnlyList<ScenarioResult>? scenarioResults = null,
        IReadOnlyList<ExplainResult>? explainResults = null)
    {
        var report = new BenchmarkReport
        {
            Config = config,
            Status = status,
            TotalElapsedSeconds = Math.Round(overallSw.Elapsed.TotalSeconds, 2),
            DatasetFingerprints = datasetFingerprints ?? Array.Empty<DatasetFingerprint>(),
            IndexFingerprints = indexFingerprints ?? Array.Empty<IndexFingerprint>(),
            ScenarioResults = scenarioResults ?? Array.Empty<ScenarioResult>(),
            DriverMetrics = eventCollector.ToMetrics(),
            ExplainResults = explainResults ?? Array.Empty<ExplainResult>()
        };

        return report with
        {
            NoisyNeighbourAnalysis = NoisyNeighbourAnalyser.Analyse(report)
        };
    }

    // ── Private helpers ───────────────────────────────────────────────

    private IMongoClient CreateInstrumentedClient(DriverEventCollector collector)
    {
        var settings = _baseSettings.Clone();
        settings.ClusterConfigurator = cb =>
        {
            cb.Subscribe<CommandStartedEvent>(collector.OnCommandStarted);
            cb.Subscribe<CommandSucceededEvent>(collector.OnCommandSucceeded);
            cb.Subscribe<CommandFailedEvent>(collector.OnCommandFailed);
            cb.Subscribe<ConnectionPoolCheckingOutConnectionEvent>(collector.OnCheckingOut);
            cb.Subscribe<ConnectionPoolCheckedOutConnectionEvent>(collector.OnCheckedOut);
            cb.Subscribe<ConnectionPoolCheckingOutConnectionFailedEvent>(collector.OnCheckoutFailed);
            cb.Subscribe<ConnectionCreatedEvent>(collector.OnConnectionCreated);
            cb.Subscribe<ConnectionClosedEvent>(collector.OnConnectionClosed);
            cb.Subscribe<ConnectionPoolClearedEvent>(collector.OnPoolCleared);
        };

        return new MongoClient(settings);
    }

    private IMongoDatabase GetBenchmarkDatabase(IMongoClient client)
    {
        // Extract the database name from the original settings
        var dbName = _baseSettings.Credential?.Source;
        if (string.IsNullOrEmpty(dbName) || dbName == "admin")
        {
            dbName = _baseSettings.ApplicationName ?? "keeperdata_benchmark";
        }

        return client.GetDatabase(dbName);
    }

    private static async Task CreateIndexesAsync(
        IMongoCollection<BsonDocument> source,
        IMongoCollection<BsonDocument> lookup,
        IMongoCollection<BsonDocument> write,
        CancellationToken ct)
    {
        // Source: compound index on createdAt + status (mirrors cleanse query patterns)
        await source.Indexes.CreateManyAsync(
        [
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys
                    .Ascending("createdAt")
                    .Ascending("status"),
                new CreateIndexOptions { Name = "ix_createdAt_status" }),

            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("status").Ascending("category"),
                new CreateIndexOptions { Name = "ix_status_category" }),

            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("referenceId"),
                new CreateIndexOptions { Name = "ix_referenceId" })
        ], ct);

        // Lookup: _id only (default)
        // Write: _id only (default)
        await write.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("sourceId"),
                new CreateIndexOptions { Name = "ix_sourceId" }),
            cancellationToken: ct);
    }

    private static async Task SeedDataAsync(
        SeedContext ctx,
        CancellationToken ct)
    {
        await SeedSourceCollectionAsync(ctx, ct);
        await SeedLookupCollectionAsync(ctx, ct);
    }

    private static async Task SeedSourceCollectionAsync(SeedContext ctx, CancellationToken ct)
    {
        var statuses = new[] { "Active", "Pending", "Archived" };
        var categories = new[] { "CattleBovine", "SheepOvine", "PigPorcine", "GoatCaprine", "DeerCervine" };
        const int batchSize = 500;

        for (var batchStart = 0; batchStart < ctx.Count; batchStart += batchSize)
        {
            ct.ThrowIfCancellationRequested();
            var models = new List<WriteModel<BsonDocument>>(batchSize);
            var end = Math.Min(batchStart + batchSize, ctx.Count);

            for (var i = batchStart; i < end; i++)
            {
                var id = $"bench-{i:D8}";
                var doc = new BsonDocument
                {
                    { "_id", id },
                    { "status", statuses[i % statuses.Length] },
                    { "category", categories[i % categories.Length] },
                    { "createdAt", ctx.BaseDate.AddDays(i % 30).AddHours(i % 24) },
                    { "numericValue", (i * 7) % 10000 },
                    { "referenceId", $"ref-{i % (ctx.Count / 5):D8}" },
                    { "payload", new string('A', 150 + (i % 100)) }
                };

                models.Add(new ReplaceOneModel<BsonDocument>(
                    Builders<BsonDocument>.Filter.Eq("_id", id), doc)
                { IsUpsert = true });
            }

            await ctx.Source.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false }, ct);
            await ctx.Throttler.DelayAsync(ctx.ThrottleDelay, ct);
        }
    }

    private static async Task SeedLookupCollectionAsync(SeedContext ctx, CancellationToken ct)
    {
        const int batchSize = 500;
        var lookupCount = ctx.Count / 5;

        for (var batchStart = 0; batchStart < lookupCount; batchStart += batchSize)
        {
            ct.ThrowIfCancellationRequested();
            var models = new List<WriteModel<BsonDocument>>(batchSize);
            var end = Math.Min(batchStart + batchSize, lookupCount);

            for (var i = batchStart; i < end; i++)
            {
                var id = $"ref-{i:D8}";
                var doc = new BsonDocument
                {
                    { "_id", id },
                    { "numericValue", (i * 13) % 5000 },
                    { "label", $"Reference-{i}" }
                };

                models.Add(new ReplaceOneModel<BsonDocument>(
                    Builders<BsonDocument>.Filter.Eq("_id", id), doc)
                { IsUpsert = true });
            }

            await ctx.Lookup.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false }, ct);
            await ctx.Throttler.DelayAsync(ctx.ThrottleDelay, ct);
        }
    }

    /// <summary>Groups seed-data parameters to keep method signatures under 5 params.</summary>
    private sealed record SeedContext(
        IMongoCollection<BsonDocument> Source,
        IMongoCollection<BsonDocument> Lookup,
        int Count,
        DateTime BaseDate,
        TimeSpan ThrottleDelay,
        IBenchmarkThrottler Throttler);

    private static async Task<IReadOnlyList<DatasetFingerprint>> CollectDatasetFingerprintsAsync(
        IMongoDatabase db, string prefix, CancellationToken ct)
    {
        var result = new List<DatasetFingerprint>();

        var collectionNames = new[] { $"{prefix}source", $"{prefix}lookup", $"{prefix}write" };
        foreach (var name in collectionNames)
        {
            var col = db.GetCollection<BsonDocument>(name);
            var count = await col.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty, cancellationToken: ct);
            if (count == 0)
            {
                result.Add(new DatasetFingerprint { CollectionName = name, DocumentCount = 0 });
                continue;
            }

            // Compute avg and p95 document sizes via aggregation
            var pipeline = new[]
            {
                new BsonDocument("$project", new BsonDocument("size", new BsonDocument("$bsonSize", "$$ROOT"))),
                new BsonDocument("$group", new BsonDocument
                {
                    { "_id", BsonNull.Value },
                    { "avgSize", new BsonDocument("$avg", "$size") },
                    { "sizes", new BsonDocument("$push", "$size") }
                })
            };

            var cursor = await col.AggregateAsync<BsonDocument>(
                PipelineDefinition<BsonDocument, BsonDocument>.Create(pipeline),
                cancellationToken: ct);

            var stats = await cursor.FirstOrDefaultAsync(ct);
            var avgSize = stats?.GetValue("avgSize", 0).ToDouble() ?? 0;

            // p95 from sorted sizes
            double p95Size = 0;
            if (stats is not null && stats.Contains("sizes"))
            {
                var sizes = stats["sizes"].AsBsonArray.Select(s => s.ToDouble()).OrderBy(s => s).ToArray();
                if (sizes.Length > 0)
                {
                    var idx = (int)Math.Ceiling(0.95 * sizes.Length) - 1;
                    p95Size = sizes[Math.Max(0, idx)];
                }
            }

            result.Add(new DatasetFingerprint
            {
                CollectionName = name,
                DocumentCount = count,
                AvgDocumentSizeBytes = Math.Round(avgSize, 2),
                P95DocumentSizeBytes = Math.Round(p95Size, 2)
            });
        }

        return result;
    }

    private static async Task<IReadOnlyList<IndexFingerprint>> CollectIndexFingerprintsAsync(
        IMongoDatabase db, string prefix, CancellationToken ct)
    {
        var result = new List<IndexFingerprint>();
        var collectionNames = new[] { $"{prefix}source", $"{prefix}lookup", $"{prefix}write" };

        foreach (var name in collectionNames)
        {
            var col = db.GetCollection<BsonDocument>(name);
            using var cursor = await col.Indexes.ListAsync(ct);
            var indexes = await cursor.ToListAsync(ct);
            foreach (var idx in indexes)
            {
                result.Add(new IndexFingerprint
                {
                    CollectionName = name,
                    IndexName = idx.GetValue("name", "").AsString,
                    KeyDefinition = idx.GetValue("key", new BsonDocument()).AsBsonDocument,
                    IsUnique = idx.GetValue("unique", false).ToBoolean()
                });
            }
        }

        return result;
    }

    private static async Task<IReadOnlyList<ExplainResult>> CollectExplainPlansAsync(
        IMongoCollection<BsonDocument> source, DateTime baseDate, CancellationToken ct)
    {
        var results = new List<ExplainResult>();

        try
        {
            // Explain: point lookup
            var pointFilter = Builders<BsonDocument>.Filter.Eq("_id", "bench-00000001");
            var pointExplain = await source.Find(pointFilter)
                .As<BsonDocument>()
                .ToCursorAsync(ct);
            // Use the command-based explain approach
            var db = source.Database;

            var pointCmd = new BsonDocument
            {
                { "explain", new BsonDocument
                    {
                        { "find", source.CollectionNamespace.CollectionName },
                        { "filter", new BsonDocument("_id", "bench-00000001") }
                    }
                },
                { "verbosity", "executionStats" }
            };

            var pointResult = await db.RunCommandAsync<BsonDocument>(pointCmd, cancellationToken: ct);
            results.Add(ParseExplain("PointLookup", pointResult));

            // Explain: range query
            var rangeCmd = new BsonDocument
            {
                { "explain", new BsonDocument
                    {
                        { "find", source.CollectionNamespace.CollectionName },
                        { "filter", new BsonDocument
                            {
                                { "createdAt", new BsonDocument
                                    {
                                        { "$gte", baseDate },
                                        { "$lt", baseDate.AddDays(1) }
                                    }
                                },
                                { "status", "Active" }
                            }
                        }
                    }
                },
                { "verbosity", "executionStats" }
            };

            var rangeResult = await db.RunCommandAsync<BsonDocument>(rangeCmd, cancellationToken: ct);
            results.Add(ParseExplain("RangeQuery", rangeResult));

            // Explain: aggregation
            var aggCmd = new BsonDocument
            {
                { "explain", new BsonDocument
                    {
                        { "aggregate", source.CollectionNamespace.CollectionName },
                        { "pipeline", new BsonArray
                            {
                                new BsonDocument("$match", new BsonDocument("status", "Active")),
                                new BsonDocument("$group", new BsonDocument
                                {
                                    { "_id", "$category" },
                                    { "count", new BsonDocument("$sum", 1) }
                                })
                            }
                        },
                        { "cursor", new BsonDocument() }
                    }
                },
                { "verbosity", "executionStats" }
            };

            var aggResult = await db.RunCommandAsync<BsonDocument>(aggCmd, cancellationToken: ct);
            results.Add(ParseExplain("Aggregation", aggResult));
        }
        catch (Exception)
        {
            // Explain may fail on some configurations; non-fatal
        }

        return results;
    }

    private static ExplainResult ParseExplain(string queryName, BsonDocument explain)
    {
        var executionStats = FindNested(explain, "executionStats");
        var queryPlanner = FindNested(explain, "queryPlanner");

        var winningPlan = queryPlanner?.GetValue("winningPlan", BsonNull.Value);
        var winningPlanStr = winningPlan?.ToJson() ?? "N/A";

        return new ExplainResult
        {
            QueryName = queryName,
            WinningPlan = winningPlanStr,
            TotalDocsExamined = executionStats?.GetValue("totalDocsExamined", 0).ToInt64() ?? 0,
            TotalKeysExamined = executionStats?.GetValue("totalKeysExamined", 0).ToInt64() ?? 0,
            NReturned = executionStats?.GetValue("nReturned", 0).ToInt64() ?? 0,
            RawExplain = explain
        };
    }

    private static BsonDocument? FindNested(BsonDocument doc, string key)
    {
        if (doc.Contains(key) && doc[key] is BsonDocument found)
            return found;

        foreach (var element in doc.Elements)
        {
            if (element.Value is BsonDocument nested)
            {
                var result = FindNested(nested, key);
                if (result is not null) return result;
            }
        }

        return null;
    }

    private async Task TearDownAsync(IMongoClient client, string prefix)
    {
        try
        {
            var db = GetBenchmarkDatabase(client);
            var collectionNames = new[] { $"{prefix}source", $"{prefix}lookup", $"{prefix}write" };
            foreach (var name in collectionNames)
            {
                _logger.LogInformation("Benchmark: dropping collection {Name}", name);
                await db.DropCollectionAsync(name);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Benchmark: failed to drop one or more benchmark collections");
        }
    }
}
