using FluentAssertions;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Operations;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using KeeperData.Core.Tests.Unit.Throttling;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.Command.Abstract;

public class CleanseAnalysisEngineBaseTests
{
    /// <summary>
    /// Concrete test subclass to expose protected static members.
    /// </summary>
    private sealed class TestableEngine(IPreloadedCtsSamDataService ds, IIssueCommandService ics)
        : CleanseAnalysisEngineBase(ds, ics, new FakeThrottler(), NullLogger.Instance)
    {
        public readonly List<(string Id, string OperationId)> CtsRecords = [];
        public readonly List<(string Id, string OperationId)> SamRecords = [];

        protected override Task ProcessCtsPrimaryRecordAsync(string id, string operationId, AnalysisMetrics metrics, CancellationToken ct)
        {
            CtsRecords.Add((id, operationId));
            return Task.CompletedTask;
        }

        protected override Task ProcessSamPrimaryRecordAsync(string id, string operationId, AnalysisMetrics metrics, CancellationToken ct)
        {
            SamRecords.Add((id, operationId));
            return Task.CompletedTask;
        }

        // Expose protected statics for testing
        public static new bool IsCtsCphHoldingRecordActive(IDictionary<string, object?> record)
            => CleanseAnalysisEngineBase.IsCtsCphHoldingRecordActive(record);

        public static new LidFullIdentifier? ParseLidFullIdentifier(IDictionary<string, object?> record)
            => CleanseAnalysisEngineBase.ParseLidFullIdentifier(record);
    }

    private readonly Mock<IPreloadedCtsSamDataService> _dataServiceMock = new();
    private readonly Mock<IIssueCommandService> _issueServiceMock = new();

    private TestableEngine CreateEngine() => new(_dataServiceMock.Object, _issueServiceMock.Object);

    #region IsCtsCphHoldingRecordActive

    [Fact]
    public void IsCtsCphHoldingRecordActive_WhenNoEndDate_ShouldReturnTrue()
    {
        var record = new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LocEffectiveTo] = null
        };

        TestableEngine.IsCtsCphHoldingRecordActive(record).Should().BeTrue();
    }

    [Fact]
    public void IsCtsCphHoldingRecordActive_WhenEndDateInFuture_ShouldReturnTrue()
    {
        var futureDate = DateTime.UtcNow.AddYears(1).ToString("yyyy-MM-dd HH:mm:ss");
        var record = new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LocEffectiveTo] = futureDate
        };

        TestableEngine.IsCtsCphHoldingRecordActive(record).Should().BeTrue();
    }

    [Fact]
    public void IsCtsCphHoldingRecordActive_WhenEndDateInPast_ShouldReturnFalse()
    {
        var pastDate = DateTime.UtcNow.AddYears(-1).ToString("yyyy-MM-dd HH:mm:ss");
        var record = new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LocEffectiveTo] = pastDate
        };

        TestableEngine.IsCtsCphHoldingRecordActive(record).Should().BeFalse();
    }

    [Fact]
    public void IsCtsCphHoldingRecordActive_WhenUnparseableDate_ShouldReturnTrue()
    {
        var record = new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LocEffectiveTo] = "not-a-date"
        };

        TestableEngine.IsCtsCphHoldingRecordActive(record).Should().BeTrue();
    }

    #endregion

    #region ParseLidFullIdentifier

    [Fact]
    public void ParseLidFullIdentifier_WithValidRecord_ShouldReturnLid()
    {
        var record = new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LidFullIdentifier] = "UK-12/345/6789"
        };

        var result = TestableEngine.ParseLidFullIdentifier(record);

        result.Should().NotBeNull();
        result!.Value.Should().Be("UK-12/345/6789");
        result.Cph.Value.Should().Be("12/345/6789");
    }

    [Fact]
    public void ParseLidFullIdentifier_WithNullValue_ShouldReturnNull()
    {
        var record = new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LidFullIdentifier] = null
        };

        TestableEngine.ParseLidFullIdentifier(record).Should().BeNull();
    }

    [Fact]
    public void ParseLidFullIdentifier_WithInvalidFormat_ShouldReturnNull()
    {
        var record = new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LidFullIdentifier] = "INVALID"
        };

        TestableEngine.ParseLidFullIdentifier(record).Should().BeNull();
    }

    #endregion

    #region ExecuteAsync (pump logic)

    [Fact]
    public async Task ExecuteAsync_ShouldProcessBothCtsAndSamRecords()
    {
        _dataServiceMock.Setup(s => s.PreloadAsync(It.IsAny<CancellationToken>(), It.IsAny<OperationScope?>())).Returns(Task.CompletedTask);
        _dataServiceMock.Setup(s => s.GetCtsCphHoldingsCount()).Returns(1);
        _dataServiceMock.Setup(s => s.GetSamCphHoldingsCount()).Returns(1);

        _dataServiceMock.Setup(s => s.ListCtsCphHoldings(0, It.IsAny<int>()))
            .Returns(new QueryResult
            {
                CollectionName = "cts_cph_holding",
                Data = [new Dictionary<string, object?> { [DataFields.CtsCphHoldingFields.LidFullIdentifier] = "UK-12/345/0001" }],
                Count = 1
            });
        _dataServiceMock.Setup(s => s.ListCtsCphHoldings(It.Is<int>(i => i > 0), It.IsAny<int>()))
            .Returns(new QueryResult { CollectionName = "cts_cph_holding", Data = [], Count = 0 });

        _dataServiceMock.Setup(s => s.ListSamCphHoldings(0, It.IsAny<int>()))
            .Returns(new QueryResult
            {
                CollectionName = "sam_cph_holdings",
                Data = [new Dictionary<string, object?> { [DataFields.SamCphHoldingFields.Cph] = "12/345/0002" }],
                Count = 1
            });
        _dataServiceMock.Setup(s => s.ListSamCphHoldings(It.Is<int>(i => i > 0), It.IsAny<int>()))
            .Returns(new QueryResult { CollectionName = "sam_cph_holdings", Data = [], Count = 0 });

        var engine = CreateEngine();
        var metrics = await engine.ExecuteAsync("op-1", CancellationToken.None);

        engine.CtsRecords.Should().ContainSingle().Which.Id.Should().Be("UK-12/345/0001");
        engine.SamRecords.Should().ContainSingle().Which.Id.Should().Be("12/345/0002");
        metrics.RecordsAnalyzed.Should().Be(2, "CTS (1) + SAM (1) records should accumulate");
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyData_ShouldReturnZeroMetrics()
    {
        _dataServiceMock.Setup(s => s.PreloadAsync(It.IsAny<CancellationToken>(), It.IsAny<OperationScope?>())).Returns(Task.CompletedTask);
        _dataServiceMock.Setup(s => s.GetCtsCphHoldingsCount()).Returns(0);
        _dataServiceMock.Setup(s => s.GetSamCphHoldingsCount()).Returns(0);
        _dataServiceMock.Setup(s => s.ListCtsCphHoldings(It.IsAny<int>(), It.IsAny<int>()))
            .Returns(new QueryResult { CollectionName = "cts_cph_holding", Data = [], Count = 0 });
        _dataServiceMock.Setup(s => s.ListSamCphHoldings(It.IsAny<int>(), It.IsAny<int>()))
            .Returns(new QueryResult { CollectionName = "sam_cph_holdings", Data = [], Count = 0 });

        var engine = CreateEngine();
        var metrics = await engine.ExecuteAsync("op-1", CancellationToken.None);

        metrics.RecordsAnalyzed.Should().Be(0);
        engine.CtsRecords.Should().BeEmpty();
        engine.SamRecords.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_ShouldAccumulateRecordsAnalyzedAcrossBothPumps()
    {
        const int ctsCount = 3;
        const int samCount = 2;

        _dataServiceMock.Setup(s => s.PreloadAsync(It.IsAny<CancellationToken>(), It.IsAny<OperationScope?>())).Returns(Task.CompletedTask);
        _dataServiceMock.Setup(s => s.GetCtsCphHoldingsCount()).Returns(ctsCount);
        _dataServiceMock.Setup(s => s.GetSamCphHoldingsCount()).Returns(samCount);

        _dataServiceMock.Setup(s => s.ListCtsCphHoldings(0, It.IsAny<int>()))
            .Returns(new QueryResult
            {
                CollectionName = "cts_cph_holding",
                Data = Enumerable.Range(1, ctsCount)
                    .Select(i => new Dictionary<string, object?>
                    { [DataFields.CtsCphHoldingFields.LidFullIdentifier] = $"UK-01/001/{i:D4}" })
                    .ToList(),
                Count = ctsCount
            });
        _dataServiceMock.Setup(s => s.ListCtsCphHoldings(It.Is<int>(i => i > 0), It.IsAny<int>()))
            .Returns(new QueryResult { CollectionName = "cts_cph_holding", Data = [], Count = 0 });

        _dataServiceMock.Setup(s => s.ListSamCphHoldings(0, It.IsAny<int>()))
            .Returns(new QueryResult
            {
                CollectionName = "sam_cph_holdings",
                Data = Enumerable.Range(1, samCount)
                    .Select(i => new Dictionary<string, object?>
                    { [DataFields.SamCphHoldingFields.Cph] = $"02/002/{i:D4}" })
                    .ToList(),
                Count = samCount
            });
        _dataServiceMock.Setup(s => s.ListSamCphHoldings(It.Is<int>(i => i > 0), It.IsAny<int>()))
            .Returns(new QueryResult { CollectionName = "sam_cph_holdings", Data = [], Count = 0 });

        var engine = CreateEngine();
        var metrics = await engine.ExecuteAsync("op-1", CancellationToken.None);

        metrics.RecordsAnalyzed.Should().Be(ctsCount + samCount,
            "RecordsAnalyzed must accumulate across both the CTS and SAM pumps");
    }

    #endregion
}
