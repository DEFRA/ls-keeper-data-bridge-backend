using System.Text;
using FluentAssertions;
using KeeperData.Core.Crypto;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Decrypt. Input: DiscoveredFileSet. Output: RawFileSet.
/// Streams each source file through decryption into raw/, skipping anything already there.</summary>
public class DecryptStageTests
{
    private const string Salt = "test-salt";

    private readonly Mock<IBlobStorageServiceFactory> _blobFactory = new();
    private readonly Mock<IBlobStorageServiceReadOnly> _sourceBlobs = new();
    private readonly Mock<IBlobStorageService> _rawBlobs = new();
    private readonly Mock<IEtlPipelineStorageProvider> _etlStorageProvider = new();
    private readonly Mock<IAesCryptoTransform> _crypto = new();
    private readonly Mock<IPasswordSaltService> _passwordSalt = new();

    private readonly HashSet<string> _existingRawKeys = [];
    private readonly Dictionary<string, MemoryStream> _rawWrites = [];

    public DecryptStageTests()
    {
        _blobFactory.Setup(f => f.GetSource(It.IsAny<string>())).Returns(_sourceBlobs.Object);
        _etlStorageProvider.Setup(s => s.ForFolder(EtlPipelineFolders.Raw)).Returns(_rawBlobs.Object);

        _sourceBlobs.Setup(s => s.OpenReadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string key, CancellationToken _) => new MemoryStream(Encoding.UTF8.GetBytes($"encrypted:{key}")));

        _sourceBlobs.Setup(s => s.GetMetadataAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string key, CancellationToken _) => new StorageObjectMetadata
            {
                Container = "external",
                Key = key,
                ContentLength = 128,
                StorageUri = new Uri($"s3://external/{key}"),
                UserMetadata = new Dictionary<string, string>()
            });

        _rawBlobs.Setup(s => s.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string key, CancellationToken _) => _existingRawKeys.Contains(key));

        _rawBlobs.Setup(s => s.OpenWriteAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, string>>(),
                It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string key, string _, IReadOnlyDictionary<string, string>? _, int _, CancellationToken _) =>
            {
                var buffer = new MemoryStream();
                _rawWrites[key] = buffer;
                return buffer;
            });

        _passwordSalt.Setup(p => p.Get(It.IsAny<string>()))
            .Returns((string fileName) => new PasswordSalt(fileName, Salt));

        // Stand-in for real decryption: write a recognisable payload to the output stream.
        _crypto.Setup(c => c.DecryptStreamAsync(
                It.IsAny<Stream>(), It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<long?>(), It.IsAny<ProgressCallback?>(), It.IsAny<CancellationToken>()))
            .Returns((Stream _, Stream output, string password, string _, long? _, ProgressCallback? _, CancellationToken ct)
                => output.WriteAsync(Encoding.UTF8.GetBytes($"decrypted:{password}"), ct).AsTask());
    }

    private DecryptStage Sut() => new(
        _blobFactory.Object,
        _etlStorageProvider.Object,
        _crypto.Object,
        _passwordSalt.Object,
        NullLogger<DecryptStage>.Instance);

    private Task<List<RawFileSet>> RunAsync(params DiscoveredFileSet[] inputs) =>
        StageRunner.RunAsync(Sut(), inputs);

    [Fact]
    public async Task Produces_one_raw_file_set_per_input()
    {
        var output = await RunAsync(
            StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv"),
            StageRunner.DiscoveredSet("CTS_KEEPER", "CTS_KEEPER_1.csv"));

        output.Should().HaveCount(2);
    }

    [Fact]
    public async Task Produces_nothing_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
    }

    [Fact]
    public async Task Writes_each_discovered_file_into_the_raw_folder()
    {
        await RunAsync(StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv", "SAM_CPH_2.csv"));

        _etlStorageProvider.Verify(s => s.ForFolder(EtlPipelineFolders.Raw), Times.AtLeastOnce);
        _rawWrites.Keys.Should().BeEquivalentTo(["SAM_CPH_1.csv", "SAM_CPH_2.csv"]);
    }

    [Fact]
    public async Task Reports_the_raw_keys_and_run_id_on_the_output()
    {
        var context = StageRunner.Context();

        var output = await StageRunner.RunAsync(
            Sut(),
            new[] { StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv", "SAM_CPH_2.csv") },
            context);

        output.Single().Files.Should().BeEquivalentTo(["SAM_CPH_1.csv", "SAM_CPH_2.csv"]);
        output.Single().RunId.Should().Be(context.RunId);
    }

    [Fact]
    public async Task Skips_a_file_that_already_exists_in_raw_without_overwriting_it()
    {
        _existingRawKeys.Add("SAM_CPH_1.csv");

        await RunAsync(StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv", "SAM_CPH_2.csv"));

        _rawWrites.Keys.Should().BeEquivalentTo(["SAM_CPH_2.csv"]);
        _rawBlobs.Verify(s => s.OpenWriteAsync("SAM_CPH_1.csv", It.IsAny<string>(),
            It.IsAny<IReadOnlyDictionary<string, string>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Still_reports_a_skipped_file_as_present_in_raw()
    {
        _existingRawKeys.Add("SAM_CPH_1.csv");

        var output = await RunAsync(StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv"));

        output.Single().Files.Should().BeEquivalentTo(["SAM_CPH_1.csv"]);
    }

    [Fact]
    public async Task Derives_the_password_from_the_object_key_and_uses_the_configured_salt()
    {
        await RunAsync(StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv"));

        _passwordSalt.Verify(p => p.Get("SAM_CPH_1.csv"), Times.Once);
        _crypto.Verify(c => c.DecryptStreamAsync(
            It.IsAny<Stream>(), It.IsAny<Stream>(), "SAM_CPH_1.csv", Salt,
            It.IsAny<long?>(), It.IsAny<ProgressCallback?>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Streams_the_source_file_rather_than_downloading_it_into_memory()
    {
        await RunAsync(StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv"));

        _sourceBlobs.Verify(s => s.OpenReadAsync("SAM_CPH_1.csv", It.IsAny<CancellationToken>()), Times.Once);
        _sourceBlobs.Verify(s => s.DownloadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Writes_the_decrypted_content_to_the_raw_object()
    {
        await RunAsync(StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv"));

        Encoding.UTF8.GetString(_rawWrites["SAM_CPH_1.csv"].ToArray())
            .Should().Be("decrypted:SAM_CPH_1.csv");
    }

    [Fact]
    public async Task Reads_the_source_for_the_source_type_on_the_run_context()
    {
        await StageRunner.RunAsync(
            Sut(),
            new[] { StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv") },
            StageRunner.Context(sourceType: "internal"));

        _blobFactory.Verify(f => f.GetSource("internal"), Times.Once);
    }
}
