using System.Text;
using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Parquet;
using XsvHcdtHelper;
using Xunit;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Normalise. Input: RawFileSet. Output: NormalisedFileSet.</summary>
[Trait("Category", "Component")]
public class NormaliseStageTests
{
    // --- 1. ORIGINAL TESTS (Testing pipeline flow) ---

    private static Task<List<NormalisedFileSet>> RunAsync(params RawFileSet[] inputs)
    {
        var dummyStorageProvider = new Mock<IEtlPipelineStorageProvider>();
        // Return a dummy storage service no matter what folder is requested
        dummyStorageProvider.Setup(p => p.ForFolder(It.IsAny<string>()))
            .Returns(new Mock<IBlobStorageService>().Object);

        var dummyNormaliser = new Mock<IXsvHcdtNormaliser>().Object;
        var dummyLogger = NullLogger<NormaliseStage>.Instance;

        return StageRunner.RunAsync(new NormaliseStage(dummyStorageProvider.Object, dummyNormaliser, dummyLogger), inputs);
    }

    [Fact]
    public async Task Produces_one_normalised_file_set_per_input()
    {
        var output = await RunAsync(
            new RawFileSet(StageRunner.Definition("SAM_CPH")),
            new RawFileSet(StageRunner.Definition("CTS_KEEPER")));

        output.Should().HaveCount(2);
    }

    [Fact]
    public async Task Produces_nothing_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
    }


    // --- 2. NEW BEHAVIOURAL TESTS (Testing PSV -> Parquet conversion) ---

    [Fact]
    public async Task NormaliseStage_ConvertsPsvToParquet_Successfully()
    {
        // Arrange
        var storageProviderMock = new Mock<IEtlPipelineStorageProvider>();
        var rawStorageMock = new Mock<IBlobStorageService>();
        var normalisedStorageMock = new Mock<IBlobStorageService>();

        // Wire up the new storage provider!
        storageProviderMock.Setup(p => p.ForFolder("raw")).Returns(rawStorageMock.Object);
        storageProviderMock.Setup(p => p.ForFolder("normalised")).Returns(normalisedStorageMock.Object);

        var normaliserMock = new Mock<IXsvHcdtNormaliser>();
        var stage = new NormaliseStage(storageProviderMock.Object, normaliserMock.Object, NullLogger<NormaliseStage>.Instance);

        var runId = Guid.NewGuid();
        var dataSetDef = StageRunner.Definition("sam_cph_holdings");
        var rawFileKey = "raw/sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.csv";
        var relativeRawKey = "sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.csv"; // Without "raw/"

        const string inputPsv = "CPH|DISEASE_TYPE|CHANGETYPE\n" +
                                "12/345/6789|TB|I\n" +
                                "98/765/4321|BSE|U\n";

        // Setup Source Stream (Now coming from rawStorageMock)
        normalisedStorageMock.Setup(b => b.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        rawStorageMock.Setup(b => b.OpenReadAsync(relativeRawKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream(Encoding.UTF8.GetBytes(inputPsv)));

        // Setup Destination Stream (Now coming from normalisedStorageMock)
        var outputStream = new NonClosingMemoryStream();
        normalisedStorageMock.Setup(b => b.OpenWriteAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, string>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(outputStream);

        var inputPayload = new RawFileSet(dataSetDef) { RunId = runId, Files = [rawFileKey] };

        // Act
        var results = await StageRunner.RunAsync(stage, [inputPayload]);

        // Assert - Payload updated correctly
        var outputPayload = results.Single();
        outputPayload.Files.Should().ContainSingle().Which.Should().EndWith("LITP_SAMCPHHOLDING_20260101.parquet");

        // Assert - Parquet Content is valid and column-aligned
        outputStream.Position = 0;
        await using var reader = await ParquetReader.CreateAsync(outputStream);
        using var rowGroup = reader.OpenRowGroupReader(0);

        var fields = reader.Schema.GetDataFields();
        fields.Should().HaveCount(3);
        fields[0].Name.Should().Be("CPH");

        var cphColumn = new string[rowGroup.RowCount];
        await rowGroup.ReadAsync(fields[0], cphColumn);
        cphColumn[0].Should().Be("12/345/6789");
        cphColumn[1].Should().Be("98/765/4321");
    }

    [Fact]
    public async Task NormaliseStage_SkipsExistingFiles_ForIdempotency()
    {
        // Arrange
        var storageProviderMock = new Mock<IEtlPipelineStorageProvider>();
        var rawStorageMock = new Mock<IBlobStorageService>();
        var normalisedStorageMock = new Mock<IBlobStorageService>();

        storageProviderMock.Setup(p => p.ForFolder("raw")).Returns(rawStorageMock.Object);
        storageProviderMock.Setup(p => p.ForFolder("normalised")).Returns(normalisedStorageMock.Object);

        var normaliserMock = new Mock<IXsvHcdtNormaliser>();
        var stage = new NormaliseStage(storageProviderMock.Object, normaliserMock.Object, NullLogger<NormaliseStage>.Instance);

        var runId = Guid.NewGuid();
        var dataSetDef = StageRunner.Definition("sam_cph_holdings");
        var rawFileKey = "raw/sam_cph_holdings/LITP_SAMCPHHOLDING_20260101.csv";

        // Simulate the file already existing in the normalised folder
        normalisedStorageMock.Setup(b => b.ExistsAsync(It.Is<string>(s => s.EndsWith(".parquet")), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var inputPayload = new RawFileSet(dataSetDef) { RunId = runId, Files = [rawFileKey] };

        // Act
        var results = await StageRunner.RunAsync(stage, [inputPayload]);

        // Assert - Payload still contains the file (so downstream stages know it's there)
        results.Single().Files.Should().ContainSingle().Which.Should().EndWith("LITP_SAMCPHHOLDING_20260101.parquet");

        // Assert - But we NEVER opened the streams or processed data
        rawStorageMock.Verify(b => b.OpenReadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        normalisedStorageMock.Verify(b => b.OpenWriteAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, string>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task NormaliseStage_TrimsWhitespaceAndHandlesEmptyFields()
    {
        // Arrange
        var storageProviderMock = new Mock<IEtlPipelineStorageProvider>();
        var rawStorageMock = new Mock<IBlobStorageService>();
        var normalisedStorageMock = new Mock<IBlobStorageService>();

        storageProviderMock.Setup(p => p.ForFolder("raw")).Returns(rawStorageMock.Object);
        storageProviderMock.Setup(p => p.ForFolder("normalised")).Returns(normalisedStorageMock.Object);

        var normaliserMock = new Mock<IXsvHcdtNormaliser>();
        var stage = new NormaliseStage(storageProviderMock.Object, normaliserMock.Object, NullLogger<NormaliseStage>.Instance);

        var runId = Guid.NewGuid();
        var dataSetDef = StageRunner.Definition("sam_cph_holdings");
        var rawFileKey = "raw/test/file.csv";
        var relativeRawKey = "test/file.csv";

        const string inputPsv = " CPH | DISEASE_TYPE | CHANGETYPE \n" +
                                "  12/345/6789  ||  I  \n";

        normalisedStorageMock.Setup(b => b.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        rawStorageMock.Setup(b => b.OpenReadAsync(relativeRawKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream(Encoding.UTF8.GetBytes(inputPsv)));

        var outputStream = new NonClosingMemoryStream();
        normalisedStorageMock.Setup(b => b.OpenWriteAsync(It.IsAny<string>(), It.IsAny<string>(), null, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(outputStream);

        var inputPayload = new RawFileSet(dataSetDef) { RunId = runId, Files = [rawFileKey] };

        // Act
        await StageRunner.RunAsync(stage, [inputPayload]);

        // Assert
        outputStream.Position = 0;
        await using var reader = await ParquetReader.CreateAsync(outputStream);
        using var rowGroup = reader.OpenRowGroupReader(0);
        var fields = reader.Schema.GetDataFields();

        var cphColumn = new string[1];
        await rowGroup.ReadAsync(fields[0], cphColumn);
        cphColumn[0].Should().Be("12/345/6789");

        // Note: We check for BeNull() because you explicitly map `string.IsNullOrEmpty(val) ? null : val` in the class!
        var diseaseColumn = new string[1];
        await rowGroup.ReadAsync(fields[1], diseaseColumn);
        diseaseColumn[0].Should().BeNull();

        var changeTypeColumn = new string[1];
        await rowGroup.ReadAsync(fields[2], changeTypeColumn);
        changeTypeColumn[0].Should().Be("I");
    }

    #region Helpers

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