using FluentAssertions;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using Moq;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.Command.Abstract;

public class CleanseAnalysisEngineBaseTests
{
    /// <summary>
    /// Concrete test subclass to expose protected static members.
    /// </summary>
    private sealed class TestableEngine(ICtsSamQueryService ds, IIssueCommandService ics)
        : CleanseAnalysisEngineBase(ds, ics)
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

        public static new bool ShouldUpdateProgress(int recordsAnalyzed)
            => CleanseAnalysisEngineBase.ShouldUpdateProgress(recordsAnalyzed);

        public static new LidFullIdentifier? ParseLidFullIdentifier(IDictionary<string, object?> record)
            => CleanseAnalysisEngineBase.ParseLidFullIdentifier(record);
    }

    private readonly Mock<ICtsSamQueryService> _dataServiceMock = new();
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

    #region ShouldUpdateProgress

    [Theory]
    [InlineData(0, true)]
    [InlineData(100, true)]
    [InlineData(200, true)]
    [InlineData(99, false)]
    [InlineData(1, false)]
    [InlineData(50, false)]
    public void ShouldUpdateProgress_ShouldReturnTrueEvery100Records(int recordsAnalyzed, bool expected)
    {
        TestableEngine.ShouldUpdateProgress(recordsAnalyzed).Should().Be(expected);
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
        _dataServiceMock.Setup(s => s.GetCtsCphHoldingsCountAsync(It.IsAny<CancellationToken>())).ReturnsAsync(1);
        _dataServiceMock.Setup(s => s.GetSamCphHoldingsCountAsync(It.IsAny<CancellationToken>())).ReturnsAsync(1);

        _dataServiceMock.Setup(s => s.ListCtsCphHoldingsAsync(0, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult
            {
                CollectionName = "cts_cph_holding",
                Data = [new Dictionary<string, object?> { [DataFields.CtsCphHoldingFields.LidFullIdentifier] = "UK-12/345/0001" }],
                Count = 1
            });
        _dataServiceMock.Setup(s => s.ListCtsCphHoldingsAsync(It.Is<int>(i => i > 0), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "cts_cph_holding", Data = [], Count = 0 });

        _dataServiceMock.Setup(s => s.ListSamCphHoldingsAsync(0, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult
            {
                CollectionName = "sam_cph_holdings",
                Data = [new Dictionary<string, object?> { [DataFields.SamCphHoldingFields.Cph] = "12/345/0002" }],
                Count = 1
            });
        _dataServiceMock.Setup(s => s.ListSamCphHoldingsAsync(It.Is<int>(i => i > 0), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "sam_cph_holdings", Data = [], Count = 0 });

        var engine = CreateEngine();
        var metrics = await engine.ExecuteAsync("op-1", (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        engine.CtsRecords.Should().ContainSingle().Which.Id.Should().Be("UK-12/345/0001");
        engine.SamRecords.Should().ContainSingle().Which.Id.Should().Be("12/345/0002");
        metrics.RecordsAnalyzed.Should().BeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyData_ShouldReturnZeroMetrics()
    {
        _dataServiceMock.Setup(s => s.GetCtsCphHoldingsCountAsync(It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _dataServiceMock.Setup(s => s.GetSamCphHoldingsCountAsync(It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _dataServiceMock.Setup(s => s.ListCtsCphHoldingsAsync(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "cts_cph_holding", Data = [], Count = 0 });
        _dataServiceMock.Setup(s => s.ListSamCphHoldingsAsync(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "sam_cph_holdings", Data = [], Count = 0 });

        var engine = CreateEngine();
        var metrics = await engine.ExecuteAsync("op-1", (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        metrics.RecordsAnalyzed.Should().Be(0);
        engine.CtsRecords.Should().BeEmpty();
        engine.SamRecords.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_ShouldReportInitialProgressCallback()
    {
        _dataServiceMock.Setup(s => s.GetCtsCphHoldingsCountAsync(It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _dataServiceMock.Setup(s => s.GetSamCphHoldingsCountAsync(It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _dataServiceMock.Setup(s => s.ListCtsCphHoldingsAsync(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "cts_cph_holding", Data = [], Count = 0 });
        _dataServiceMock.Setup(s => s.ListSamCphHoldingsAsync(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "sam_cph_holdings", Data = [], Count = 0 });

        var callbackCalled = false;
        var engine = CreateEngine();
        await engine.ExecuteAsync("op-1", (analyzed, total, _, _) =>
        {
            if (analyzed == 0) callbackCalled = true;
            return Task.CompletedTask;
        }, CancellationToken.None);

        callbackCalled.Should().BeTrue("the initial progress callback with 0 records analyzed should fire");
    }

    #endregion
}
