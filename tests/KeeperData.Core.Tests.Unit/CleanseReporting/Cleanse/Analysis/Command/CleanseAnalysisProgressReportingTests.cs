using System.Collections.Concurrent;
using System.Text.Json;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Locking;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Cleanse.Analysis.Command;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Impl;
using KeeperData.Core.Reports.Cleanse.Export.Command;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Metadata.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;
using KeeperData.Core.Reports.Issues.Command;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Query.Abstract;
using KeeperData.Core.Reports.Issues.Query.Dtos;
using KeeperData.Core.Reports.Operations;
using KeeperData.Core.Reports.SamCtsHoldings.Query;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using KeeperData.Core.Storage;
using KeeperData.Core.Tests.Unit.Throttling;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.Command;

/// <summary>
/// Proves that cleanse analysis progress reporting works end-to-end:
/// starts analysis on a background thread, polls the in-memory repository
/// from the test thread, and asserts that intermediate progress snapshots
/// are populated with the expected operation tree hierarchy and metrics.
///
/// Data volume is controlled via constants — start large for confidence,
/// scale down for CI speed.
/// </summary>
public sealed class CleanseAnalysisProgressReportingTests
{
    // ── Configurable volume knobs ──────────────────────────────────────
    private const int CtsRecordCount = 200;
    private const int SamRecordCount = 100;
    private const int IssueUpsertDelayMs = 15;
    private const int QueryDelayMs = 100;
    private const int PumpBatchSize = 50;
    private const int PollIntervalMs = 200;
    private const int TimeoutSeconds = 120;

    private readonly ITestOutputHelper _output;

    public CleanseAnalysisProgressReportingTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task Progress_is_reported_during_analysis_with_full_operation_tree()
    {
        // ── Arrange ────────────────────────────────────────────────────
        var repo = new InMemoryOperationRepository();
        var dataSetDefs = BuildDataSetDefinitions();

        var queryServiceMock = BuildQueryServiceMock(dataSetDefs);
        var lockMock = BuildDistributedLockMock();
        var issueRepoMock = BuildIssueRepoMock();
        var historyRepoMock = new Mock<IIssueHistoryAggRootRepository>();
        var issueQueriesMock = BuildIssueQueriesMock();
        var blobFactoryMock = BuildBlobStorageFactoryMock();
        var notificationMock = new Mock<ICleanseReportNotificationService>();
        var exportMetaMock = BuildExportMetadataRepoMock();

        var throttler = new FakeThrottler();
        throttler.Settings = throttler.Settings with
        {
            CleanseAnalysis = throttler.Settings.CleanseAnalysis with { PumpBatchSize = PumpBatchSize }
        };

        var operationQueriesMock = BuildOperationQueriesMock(repo);

        var operationCommandService = new CleanseOperationCommandService(repo);

        var issueCommandService = new IssueCommandService(issueRepoMock.Object, historyRepoMock.Object);

        var dataService = new PreloadedCtsSamDataService(
            dataSetDefs,
            queryServiceMock.Object,
            throttler,
            NullLogger<PreloadedCtsSamDataService>.Instance);

        var engine = new CleanseAnalysisEngine(
            dataService,
            issueCommandService,
            throttler,
            NullLogger<CleanseAnalysisEngine>.Instance);

        var exportCommandService = new CleanseReportExportCommandService(
            issueQueriesMock.Object,
            blobFactoryMock.Object,
            notificationMock.Object,
            operationCommandService,
            operationQueriesMock.Object,
            exportMetaMock.Object,
            throttler,
            NullLogger<CleanseReportExportCommandService>.Instance);

        var sut = new CleanseAnalysisCommandService(
            operationCommandService,
            operationQueriesMock.Object,
            issueCommandService,
            lockMock.Object,
            exportCommandService,
            repo,
            NullLogger<CleanseAnalysisCommandService>.Instance,
            engine);

        // ── Act: start analysis (fire-and-forget) ──────────────────────
        var dto = await sut.StartAnalysisAsync(CancellationToken.None);
        dto.Should().NotBeNull("lock was available, operation should have been created");
        var operationId = dto!.Id;
        _output.WriteLine($"Operation started: {operationId}");

        // ── Poll for progress snapshots ────────────────────────────────
        var snapshots = new List<(DateTime Timestamp, OperationNode Progress)>();
        var deadline = DateTime.UtcNow.AddSeconds(TimeoutSeconds);

        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(PollIntervalMs);

            var op = await repo.GetByIdAsync(operationId);
            if (op is null) continue;

            if (op.Progress is not null)
            {
                snapshots.Add((DateTime.UtcNow, op.Progress));
                LogSnapshot(op);
            }

            if (op.Status is CleanseAnalysisStatus.Completed or CleanseAnalysisStatus.Failed or CleanseAnalysisStatus.Cancelled)
            {
                _output.WriteLine($"Operation finished with status: {op.Status}");
                break;
            }
        }

        // ── Assert: final status ───────────────────────────────────────
        var final = await repo.GetByIdAsync(operationId);
        final.Should().NotBeNull();
        final!.Status.Should().Be(CleanseAnalysisStatus.Completed, "operation should complete successfully");
        final.DurationMs.Should().BeGreaterThan(0);

        var totalExpectedRecords = CtsRecordCount + SamRecordCount;
        // The engine analyses CTS+SAM primary records; each generates at least one issue (CTS_CPH_NOT_IN_SAM / SAM_CPH_NOT_IN_CTS)
        final.RecordsAnalyzed.Should().Be(totalExpectedRecords);
        final.IssuesFound.Should().BeGreaterThan(0, "CTS records without SAM counterparts should generate issues");

        _output.WriteLine($"Total snapshots captured: {snapshots.Count}");
        _output.WriteLine($"RecordsAnalyzed={final.RecordsAnalyzed}, IssuesFound={final.IssuesFound}, IssuesResolved={final.IssuesResolved}, DurationMs={final.DurationMs}");

        // ── Assert: intermediate observation (PRIMARY) ─────────────────
        snapshots.Should().HaveCountGreaterThanOrEqualTo(2,
            "the PeriodicTimer flushes every 2s — with the configured volume we expect multiple intermediate snapshots");

        // At least one snapshot has a root with in-progress status and children
        snapshots.Should().Contain(s =>
            s.Progress.Status == OperationStatuses.InProgress &&
            s.Progress.Children != null &&
            s.Progress.Children.Count > 0,
            "at least one intermediate snapshot should show the root as in-progress with children");

        // At least one snapshot shows the Analysis phase in-progress
        snapshots.Should().Contain(s =>
            s.Progress.Children != null &&
            s.Progress.Children.Any(c =>
                c.Name == OperationPhases.Analysis &&
                c.Status == OperationStatuses.InProgress),
            "at least one snapshot should show Analysis phase in-progress");

        // At least one Analysis snapshot has a pump child with mid-batch progress
        var analysisSnapshots = snapshots
            .Where(s => s.Progress.Children != null)
            .SelectMany(s => s.Progress.Children!)
            .Where(c => c.Name == OperationPhases.Analysis && c.Children != null)
            .SelectMany(c => c.Children!)
            .ToList();

        analysisSnapshots.Should().Contain(child =>
            (child.Name == "Preload" || child.Name == "CTS Pump" || child.Name == "SAM Pump") &&
            child.ProcessedCount > 0 &&
            child.TotalRecords > 0,
            "at least one pump/preload child should show mid-batch progress (ProcessedCount > 0)");

        // ElapsedMs increases across root snapshots (count >= 2 guaranteed by assertion above)
        var firstElapsed = snapshots.First().Progress.ElapsedMs;
        var lastElapsed = snapshots.Last().Progress.ElapsedMs;
        lastElapsed.Should().BeGreaterThan(firstElapsed,
            "ElapsedMs on the root node should increase across successive snapshots");

        // Rate metrics: at least one pump node has a non-null RPM
        var pumpNodes = analysisSnapshots
            .Where(c => c.Name is "CTS Pump" or "SAM Pump")
            .ToList();
        pumpNodes.Should().NotBeEmpty("CTS and SAM pump nodes should appear in intermediate snapshots");
        pumpNodes.Should().Contain(p => p.CurrentRecordsPerMinute != null || p.AverageRecordsPerMinute != null,
            "at least one pump node should have RPM metrics populated");

        // ── Assert: final tree structure ───────────────────────────────
        var finalProgress = final.Progress;
        finalProgress.Should().NotBeNull("final flush should persist the completed operation tree");
        finalProgress!.Name.Should().Be("total");
        finalProgress.Status.Should().Be(OperationStatuses.Completed);
        finalProgress.ElapsedMs.Should().BeGreaterThan(0);
        finalProgress.Children.Should().NotBeNull();
        finalProgress.Children.Should().HaveCount(3, "Analysis + Deactivation + Export");

        var analysisNode = finalProgress.Children!.Single(c => c.Name == OperationPhases.Analysis);
        analysisNode.Status.Should().Be(OperationStatuses.Completed);
        analysisNode.Children.Should().NotBeNull();

        // Analysis should have Preload + CTS Pump + SAM Pump
        var preloadNode = analysisNode.Children!.SingleOrDefault(c => c.Name == "Preload");
        preloadNode.Should().NotBeNull("Analysis should contain a Preload child scope");
        preloadNode!.Status.Should().Be(OperationStatuses.Completed);
        // Preload has 6 collection children
        preloadNode.Children.Should().NotBeNull();
        preloadNode.Children!.Count.Should().BeGreaterThanOrEqualTo(2, "Preload should have multiple collection children");

        var ctsPump = analysisNode.Children!.SingleOrDefault(c => c.Name == "CTS Pump");
        ctsPump.Should().NotBeNull("Analysis should contain a CTS Pump child scope");
        ctsPump!.Status.Should().Be(OperationStatuses.Completed);
        ctsPump.ProcessedCount.Should().Be(CtsRecordCount);
        ctsPump.TotalRecords.Should().Be(CtsRecordCount);

        var samPump = analysisNode.Children!.SingleOrDefault(c => c.Name == "SAM Pump");
        samPump.Should().NotBeNull("Analysis should contain a SAM Pump child scope");
        samPump!.Status.Should().Be(OperationStatuses.Completed);
        samPump.ProcessedCount.Should().Be(SamRecordCount);
        samPump.TotalRecords.Should().Be(SamRecordCount);

        var deactivationNode = finalProgress.Children!.Single(c => c.Name == OperationPhases.Deactivation);
        deactivationNode.Status.Should().Be(OperationStatuses.Completed);

        var exportNode = finalProgress.Children!.Single(c => c.Name == OperationPhases.Export);
        exportNode.Status.Should().Be(OperationStatuses.Completed);

        // Log the final tree for visual inspection
        _output.WriteLine("\n=== Final Operation Tree ===");
        LogOperationNode(finalProgress, indent: 0);
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Helpers: Mock builders
    // ──────────────────────────────────────────────────────────────────────

    private static DataSetDefinitions BuildDataSetDefinitions()
    {
        DataSetDefinition Def(string name) => new(
            name,
            FilePrefixFormat: $"{name}_{{0}}",
            PrimaryKeyHeaderNames: ["Id"],
            ChangeTypeHeaderName: "CHANGE_TYPE",
            Accumulators: []);

        return new DataSetDefinitions
        {
            CTSCPHHolding = Def("cts_cph_holding"),
            CTSKeeper = Def("cts_keeper"),
            SamCPHHolding = Def("sam_cph_holding"),
            SamHerd = Def("sam_herd"),
            SamParty = Def("sam_party"),
            SamCPHHolder = Def("sam_cph_holder"),
            SamTla = Def("sam_tla"),
            Amls2CommonLand = Def("amls2_common_land"),
            Amls2Port = Def("amls2_port"),
            CtsAgent = Def("cts_agent"),
            AmesHaulier = Def("ames_haulier"),
            SamShowground = Def("sam_showground")
        };
    }

    private Mock<IQueryService> BuildQueryServiceMock(DataSetDefinitions defs)
    {
        var mock = new Mock<IQueryService>();

        mock.Setup(q => q.QueryAsync(It.IsAny<QueryParameters>(), It.IsAny<CancellationToken>()))
            .Returns<QueryParameters, CancellationToken>(async (qp, ct) =>
            {
                await Task.Delay(QueryDelayMs, ct);

                // Count queries (Top=0)
                if (qp.Top == 0)
                {
                    long count = qp.CollectionName switch
                    {
                        "cts_cph_holding" => CtsRecordCount,
                        "sam_cph_holding" => SamRecordCount,
                        _ => 0
                    };
                    return new QueryResult
                    {
                        CollectionName = qp.CollectionName,
                        Data = [],
                        Count = 0,
                        TotalCount = count
                    };
                }

                // Data queries — only CTS CPH Holdings and SAM CPH Holdings produce records
                if (qp.CollectionName == defs.CTSCPHHolding.Name)
                    return BuildCtsPage(qp);
                if (qp.CollectionName == defs.SamCPHHolding.Name)
                    return BuildSamPage(qp);

                // All other collections return empty
                return new QueryResult
                {
                    CollectionName = qp.CollectionName,
                    Data = [],
                    Count = 0,
                    TotalCount = 0
                };
            });

        return mock;
    }

    private static QueryResult BuildCtsPage(QueryParameters qp)
    {
        var records = new List<Dictionary<string, object?>>();
        for (var i = qp.Skip; i < CtsRecordCount && records.Count < qp.Top; i++)
        {
            var county = (i % 51) + 1; // 1–51 valid county codes
            var lid = $"AB-{county:00}/{i:000}/{i:0000}";
            records.Add(new Dictionary<string, object?>
            {
                [DataFields.CtsCphHoldingFields.LidFullIdentifier] = lid,
                [DataFields.IsDeleted] = false
            });
        }

        return new QueryResult
        {
            CollectionName = qp.CollectionName,
            Data = records,
            Count = records.Count,
            TotalCount = CtsRecordCount,
            Skip = qp.Skip,
            Top = qp.Top
        };
    }

    private static QueryResult BuildSamPage(QueryParameters qp)
    {
        var records = new List<Dictionary<string, object?>>();
        for (var i = qp.Skip; i < SamRecordCount && records.Count < qp.Top; i++)
        {
            // Use CPH values that DON'T match CTS LIDs to trigger SAM_CPH_NOT_IN_CTS issues
            var cph = $"90/{i:000}/{i:0000}";
            records.Add(new Dictionary<string, object?>
            {
                [DataFields.SamCphHoldingFields.Cph] = cph,
                [DataFields.IsDeleted] = false
            });
        }

        return new QueryResult
        {
            CollectionName = qp.CollectionName,
            Data = records,
            Count = records.Count,
            TotalCount = SamRecordCount,
            Skip = qp.Skip,
            Top = qp.Top
        };
    }

    private static Mock<IDistributedLock> BuildDistributedLockMock()
    {
        var handleMock = new Mock<IDistributedLockHandle>();
        handleMock.Setup(h => h.TryRenewAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var lockMock = new Mock<IDistributedLock>();
        lockMock.Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(handleMock.Object);

        return lockMock;
    }

    private Mock<IIssueAggRootRepository> BuildIssueRepoMock()
    {
        var mock = new Mock<IIssueAggRootRepository>();

        // GetByIdAsync → null (all issues are new)
        mock.Setup(r => r.GetByIdAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((Issue?)null);

        // UpsertAsync → simulate write latency
        mock.Setup(r => r.UpsertAsync(It.IsAny<Issue>(), It.IsAny<CancellationToken>()))
            .Returns<Issue, CancellationToken>(async (_, ct) =>
            {
                await Task.Delay(IssueUpsertDelayMs, ct);
            });

        // DeactivateStaleAsync → simulate the scope interaction pattern from the real repo
        mock.Setup(r => r.DeactivateStaleAsync(
                It.IsAny<string>(),
                It.IsAny<Func<int, int, Task>?>(),
                It.IsAny<CancellationToken>(),
                It.IsAny<OperationScope?>(),
                It.IsAny<Func<bool>?>()))
            .Returns<string, Func<int, int, Task>?, CancellationToken, OperationScope?, Func<bool>?>(
                async (_, onBatch, ct, scope, _) =>
                {
                    const int staleCount = 5;
                    scope?.Start(staleCount, $"Deactivating {staleCount} stale issues");

                    await Task.Delay(50, ct);
                    scope?.TrackElapsed("batch_fetch", 25);
                    scope?.TrackElapsed("batch_update", 20);
                    scope?.UpdateProgress(staleCount);

                    if (onBatch is not null)
                        await onBatch(staleCount, staleCount);

                    scope?.Complete($"Deactivated {staleCount} issues");
                    return staleCount;
                });

        return mock;
    }

    private static Mock<IIssueQueries> BuildIssueQueriesMock()
    {
        var mock = new Mock<IIssueQueries>();

        mock.Setup(q => q.GetActiveIssuesCountAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(0L);
        mock.Setup(q => q.GetActiveIssuesCountAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0L);

        // Return an empty async enumerable for streaming
        mock.Setup(q => q.StreamActiveIssuesByRulePriorityAsync(
                It.IsAny<IReadOnlyList<string>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(EmptyAsyncEnumerable<IssueDto>());
        mock.Setup(q => q.StreamActiveIssuesByRulePriorityAsync(
                It.IsAny<IReadOnlyList<string>>(), It.IsAny<DateTime>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(EmptyAsyncEnumerable<IssueDto>());

        return mock;
    }

    private static Mock<IBlobStorageServiceFactory> BuildBlobStorageFactoryMock()
    {
        var blobMock = new Mock<IBlobStorageService>();
        blobMock.Setup(b => b.UploadAsync(
                It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<string?>(),
                It.IsAny<IReadOnlyDictionary<string, string>?>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        blobMock.Setup(b => b.GeneratePresignedUrl(It.IsAny<string>(), It.IsAny<TimeSpan?>()))
            .Returns("https://test-bucket.s3.amazonaws.com/test-report.zip");

        var factoryMock = new Mock<IBlobStorageServiceFactory>();
        factoryMock.Setup(f => f.GetCleanseReportsBlobService()).Returns(blobMock.Object);

        return factoryMock;
    }

    private static Mock<IExportMetadataRepository> BuildExportMetadataRepoMock()
    {
        var mock = new Mock<IExportMetadataRepository>();
        mock.Setup(r => r.GetLastExportedAtUtcAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((DateTime?)null);
        mock.Setup(r => r.SetLastExportedAtUtcAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return mock;
    }

    private static Mock<ICleanseAnalysisOperationsQueries> BuildOperationQueriesMock(InMemoryOperationRepository repo)
    {
        var mock = new Mock<ICleanseAnalysisOperationsQueries>();

        mock.Setup(q => q.GetOperationAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns<string, CancellationToken>(async (id, ct) =>
            {
                var op = await repo.GetByIdAsync(id, ct);
                if (op is null) return null;
                return MapToDto(op);
            });

        mock.Setup(q => q.GetCurrentOperationAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct =>
            {
                var op = repo.GetLatest();
                if (op is null) return null;
                return MapToDto(await repo.GetByIdAsync(op.Id, ct) ?? op);
            });

        return mock;
    }

    private static CleanseAnalysisOperationDto MapToDto(CleanseAnalysisOperation op) => new()
    {
        Id = op.Id,
        Status = op.Status,
        StartedAtUtc = op.StartedAtUtc,
        CompletedAtUtc = op.CompletedAtUtc,
        ProgressPercentage = op.ProgressPercentage,
        StatusDescription = op.StatusDescription,
        RecordsAnalyzed = op.RecordsAnalyzed,
        TotalRecords = op.TotalRecords,
        IssuesFound = op.IssuesFound,
        IssuesResolved = op.IssuesResolved,
        Error = op.Error,
        DurationMs = op.DurationMs,
        ReportObjectKey = op.ReportObjectKey,
        ReportUrl = op.ReportUrl,
        FinalAverageRpm = op.FinalAverageRpm,
        CancelledAtUtc = op.CancelledAtUtc,
        Progress = op.Progress
    };

    // ──────────────────────────────────────────────────────────────────────
    //  Helpers: Logging
    // ──────────────────────────────────────────────────────────────────────

    private void LogSnapshot(CleanseAnalysisOperation op)
    {
        var p = op.Progress;
        if (p is null) return;

        var childSummary = p.Children is not null
            ? string.Join(", ", p.Children.Select(c => $"{c.Name}={c.Status}({c.ProcessedCount}/{c.TotalRecords})"))
            : "none";

        _output.WriteLine(
            $"[{DateTime.UtcNow:HH:mm:ss.fff}] Status={op.Status}, Tree={p.Status} elapsed={p.ElapsedMs}ms children=[{childSummary}]");
    }

    private void LogOperationNode(OperationNode node, int indent)
    {
        var prefix = new string(' ', indent * 2);
        var rpm = node.CurrentRecordsPerMinute?.ToString("F0") ?? "-";
        _output.WriteLine(
            $"{prefix}{node.Name}: {node.Status} | {node.ProcessedCount}/{node.TotalRecords} | {node.Elapsed} | RPM={rpm}");

        if (node.Children is null) return;
        foreach (var child in node.Children)
            LogOperationNode(child, indent + 1);
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Helpers: Utilities
    // ──────────────────────────────────────────────────────────────────────

    private static async IAsyncEnumerable<T> EmptyAsyncEnumerable<T>()
    {
        await Task.CompletedTask;
        yield break;
    }

    // ──────────────────────────────────────────────────────────────────────
    //  In-memory aggregate root repository
    // ──────────────────────────────────────────────────────────────────────

    private sealed class InMemoryOperationRepository : ICleanseAnalysisOperationAggRootRepository
    {
        private readonly ConcurrentDictionary<string, string> _store = new();

        public Task CreateAsync(CleanseAnalysisOperation operation, CancellationToken ct = default)
        {
            var json = JsonSerializer.Serialize(operation);
            _store[operation.Id] = json;
            return Task.CompletedTask;
        }

        public Task<CleanseAnalysisOperation?> GetByIdAsync(string operationId, CancellationToken ct = default)
        {
            if (!_store.TryGetValue(operationId, out var json))
                return Task.FromResult<CleanseAnalysisOperation?>(null);

            // Deep-copy via JSON round-trip to avoid cross-thread mutation issues
            var copy = JsonSerializer.Deserialize<CleanseAnalysisOperation>(json);
            return Task.FromResult(copy);
        }

        public Task UpdateAsync(CleanseAnalysisOperation operation, CancellationToken ct = default)
        {
            var json = JsonSerializer.Serialize(operation);
            _store[operation.Id] = json;
            return Task.CompletedTask;
        }

        public Task<long> DeleteAllAsync(CancellationToken ct = default)
        {
            var count = _store.Count;
            _store.Clear();
            return Task.FromResult((long)count);
        }

        public CleanseAnalysisOperation? GetLatest()
        {
            var last = _store.Values.LastOrDefault();
            return last is null ? null : JsonSerializer.Deserialize<CleanseAnalysisOperation>(last);
        }
    }
}
