using System.Text;
using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Parquet;
using XsvHcdtHelper;
using Xunit;

namespace KeeperData.Bridge.Tests.Component.EtlPipeline.Stages;

[Trait("Category", "Component")]
public class NormaliseStageTests
{
    private readonly Mock<IEtlPipelineStorageProvider> _storageProviderMock;
    private readonly Mock<IBlobStorageService> _blobStorageMock;
    private readonly Mock<IXsvHcdtNormaliser> _hcdtNormaliserMock;
    private readonly NormaliseStage _sut;
    private readonly EtlPipelineContext _pipelineContext;
    private readonly DataSetDefinition _dataSetDef;

    public NormaliseStageTests()
    {
        _blobStorageMock = new Mock<IBlobStorageService>();
        _storageProviderMock = new Mock<IEtlPipelineStorageProvider>();
        _storageProviderMock.Setup(p => p.ForFolder(It.IsAny<string>())).Returns(_blobStorageMock.Object);
        _hcdtNormaliserMock = new Mock<IXsvHcdtNormaliser>();
        _sut = new NormaliseStage(_storageProviderMock.Object, _hcdtNormaliserMock.Object, NullLogger<NormaliseStage>.Instance);

        _pipelineContext = new EtlPipelineContext(Guid.NewGuid(), "external");
        _dataSetDef = new DataSetDefinition(
            Name: "sam_cph_holdings",
            FilePrefixFormat: "LITP_SAMCPHHOLDING_{0}",
            PrimaryKeyHeaderNames: ["CPH"],
            ChangeTypeHeaderName: "CHANGETYPE",
            Accumulators: []);
    }

    [Fact]
    public async Task NormaliseStage_ConvertsPsvToParquet_Successfully()
    {
        // Arrange
        var rawFileKey = "raw/sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.csv";
        var expectedDestKey = "sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.parquet";

        const string inputPsv = "CPH|DISEASE_TYPE|CHANGETYPE\n" +
                                "12/345/6789|TB|I\n" +
                                "98/765/4321|BSE|U\n";

        // Setup Source Stream
        _blobStorageMock.Setup(b => b.ExistsAsync(expectedDestKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _blobStorageMock.Setup(b => b.OpenReadAsync("sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.csv", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream(Encoding.UTF8.GetBytes(inputPsv)));

        // Setup Destination Stream (Use our NonClosing stream so we can read it after the stage disposes it)
        var outputStream = new NonClosingMemoryStream();
        _blobStorageMock.Setup(b => b.OpenWriteAsync(expectedDestKey, It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, string>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(outputStream);

        var inputPayload = new RawFileSet(_dataSetDef) { Files = [rawFileKey] };

        // Act
        var results = await RunStageAsync(inputPayload);

        // Assert - Payload updated correctly
        var outputPayload = results.Single();
        outputPayload.Files.Should().ContainSingle().Which.Should().Be(expectedDestKey);

        // Assert - Parquet Content is valid and column-aligned
        outputStream.Position = 0;
        await using var reader = await ParquetReader.CreateAsync(outputStream);
        using var rowGroup = reader.OpenRowGroupReader(0);

        var fields = reader.Schema.GetDataFields();
        fields.Should().HaveCount(3);
        fields[0].Name.Should().Be("CPH");
        fields[1].Name.Should().Be("DISEASE_TYPE");
        fields[2].Name.Should().Be("CHANGETYPE");

        var cphColumn = new string[rowGroup.RowCount];
        await rowGroup.ReadAsync(fields[0], cphColumn);
        cphColumn[0].Should().Be("12/345/6789");
        cphColumn[1].Should().Be("98/765/4321");
        reader.RowGroupCount.Should().Be(1);
        rowGroup.RowCount.Should().Be(2);
    }

    [Fact]
    public async Task NormaliseStage_UsesHcdtNormaliser_ForHcdtDatasets()
    {
        const string rawFileKey = "raw/sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.csv";
        const string relativeRawKey = "sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.csv";
        const string destinationKey = "sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.parquet";

        _blobStorageMock.Setup(b => b.ExistsAsync(destinationKey, It.IsAny<CancellationToken>())).ReturnsAsync(false);
        _blobStorageMock.Setup(b => b.OpenReadAsync(relativeRawKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream());
        _blobStorageMock.Setup(b => b.OpenWriteAsync(destinationKey, It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, string>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NonClosingMemoryStream());
        _hcdtNormaliserMock
            .Setup(n => n.NormaliseAsync(It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<Action<XsvHcdtOptions>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new XsvValidationReport("source", "destination", 1, 1, true, true, []));

        var hcdtDefinition = _dataSetDef with { Format = FileFormat.Hcdt };

        var results = await RunStageAsync(new RawFileSet(hcdtDefinition) { Files = [rawFileKey] });

        results.Single().Files.Should().ContainSingle().Which.Should().Be(destinationKey);
        _hcdtNormaliserMock.Verify(n => n.NormaliseAsync(
            It.IsAny<Stream>(),
            It.IsAny<Stream>(),
            It.IsAny<Action<XsvHcdtOptions>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task NormaliseStage_SkipsExistingFiles_ForIdempotency()
    {
        // Arrange
        var rawFileKey = "raw/sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.csv";
        var expectedDestKey = "sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.parquet";

        // Simulate the file already existing in the normalised/ folder
        _blobStorageMock.Setup(b => b.ExistsAsync(expectedDestKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var inputPayload = new RawFileSet(_dataSetDef) { Files = [rawFileKey] };

        // Act
        var results = await RunStageAsync(inputPayload);

        // Assert - Payload still contains the file
        var outputPayload = results.Single();
        outputPayload.Files.Should().ContainSingle().Which.Should().Be(expectedDestKey);

        // Assert
        _blobStorageMock.Verify(b => b.OpenReadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _blobStorageMock.Verify(b => b.OpenWriteAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, string>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task NormaliseStage_TrimsWhitespaceAndHandlesEmptyFields()
    {
        // Arrange
        var rawFileKey = "raw/test/file.csv";

        // Whitespace padding and empty fields "||"
        const string inputPsv = " CPH | DISEASE_TYPE | CHANGETYPE \n" +
                                "  12/345/6789  ||  I  \n";

        _blobStorageMock.Setup(b => b.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(false);
        _blobStorageMock.Setup(b => b.OpenReadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream(Encoding.UTF8.GetBytes(inputPsv)));

        var outputStream = new NonClosingMemoryStream();
        _blobStorageMock.Setup(b => b.OpenWriteAsync(It.IsAny<string>(), It.IsAny<string>(), null, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(outputStream);

        // Act
        await RunStageAsync(new RawFileSet(_dataSetDef) { Files = [rawFileKey] });

        // Assert
        outputStream.Position = 0;
        await using var reader = await ParquetReader.CreateAsync(outputStream);
        using var rowGroup = reader.OpenRowGroupReader(0);
        var fields = reader.Schema.GetDataFields();

        // Check CPH was trimmed
        var cphColumn = new string[1];
        await rowGroup.ReadAsync(fields[0], cphColumn);
        cphColumn[0].Should().Be("12/345/6789");

        // Check DISEASE_TYPE (which was empty ||) was captured as null, not an empty string
        var diseaseColumn = new string[1];
        await rowGroup.ReadAsync(fields[1], diseaseColumn);
        diseaseColumn[0].Should().BeNull();

        // Check CHANGETYPE was trimmed
        var changeTypeColumn = new string[1];
        await rowGroup.ReadAsync(fields[2], changeTypeColumn);
        changeTypeColumn[0].Should().Be("I");
    }

    [Fact]
    public async Task NormaliseStage_WritesLargePsvInBoundedRowGroups()
    {
        const int sourceRowCount = 50_001;
        const string rawFileKey = "raw/sam_cph_holdings/LITP_SAMCPHHOLDING_20260102.csv";
        const string relativeRawKey = "sam_cph_holdings/LITP_SAMCPHHOLDING_20260102.csv";
        const string destinationKey = "sam_cph_holdings/LITP_SAMCPHHOLDING_20260102.parquet";

        var psv = new StringBuilder("CPH|CHANGETYPE\n");
        for (var row = 0; row < sourceRowCount; row++)
        {
            psv.Append("CPH").Append(row).Append("|I\n");
        }

        _blobStorageMock.Setup(b => b.ExistsAsync(destinationKey, It.IsAny<CancellationToken>())).ReturnsAsync(false);
        _blobStorageMock.Setup(b => b.OpenReadAsync(relativeRawKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream(Encoding.UTF8.GetBytes(psv.ToString())));

        var outputStream = new NonClosingMemoryStream();
        _blobStorageMock.Setup(b => b.OpenWriteAsync(destinationKey, It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, string>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(outputStream);

        await RunStageAsync(new RawFileSet(_dataSetDef) { Files = [rawFileKey] });

        outputStream.Position = 0;
        await using var reader = await ParquetReader.CreateAsync(outputStream);
        reader.RowGroupCount.Should().Be(2);

        long parquetRowCount = 0;
        for (var groupIndex = 0; groupIndex < reader.RowGroupCount; groupIndex++)
        {
            using var rowGroup = reader.OpenRowGroupReader(groupIndex);
            parquetRowCount += rowGroup.RowCount;
        }

        parquetRowCount.Should().Be(sourceRowCount);
    }

    #region Helpers

    /// <summary>
    /// Helper to convert the single item to an IAsyncEnumerable and consume the pipeline stage.
    /// </summary>
    private async Task<List<NormalisedFileSet>> RunStageAsync(RawFileSet input)
    {
        var asyncInput = AsyncEnumerableHelper(input);
        var outputStream = _sut.RunAsync(asyncInput, _pipelineContext, CancellationToken.None);

        var results = new List<NormalisedFileSet>();
        await foreach (var item in outputStream)
        {
            results.Add(item);
        }

        return results;
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    private async IAsyncEnumerable<RawFileSet> AsyncEnumerableHelper(RawFileSet item)
    {
        yield return item;
    }



    /// <summary>
    /// MemoryStream wrapper that ignores Dispose/Close calls so we can read the written bytes in our assertions.
    /// </summary>
    private class NonClosingMemoryStream : MemoryStream
    {
        public override void Close() { }
        protected override void Dispose(bool disposing) { }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    #endregion

}
