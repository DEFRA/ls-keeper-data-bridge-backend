using System.Text;
using System.Text.Json;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Temporary stage so testers can see what discovery found: logs each dataset with its files,
/// and writes a JSON manifest to the internal bucket. Remove once the decrypt stage lands.</summary>
public sealed class ReportDiscoveredFilesStage(
    IBlobStorageServiceFactory blobStorageFactory,
    ILogger<ReportDiscoveredFilesStage> logger) : MapStage<DiscoveredFileSet, DiscoveredFileSet>
{
    public override string Name => "report-discovered-files";

    protected override async Task<DiscoveredFileSet> MapAsync(
        DiscoveredFileSet input, IPipelineContext context, CancellationToken cancellationToken)
    {
        var etlContext = (EtlPipelineContext)context;

        logger.LogInformation(
            "Discovered {FileCount} file(s) for dataset {Dataset} (runId={RunId}): {Files}",
            input.Files.Count,
            input.Definition.Name,
            etlContext.RunId,
            string.Join(", ", input.Files.Select(f => f.StorageObject.Key)));

        var manifest = JsonSerializer.Serialize(
            new
            {
                runId = etlContext.RunId,
                dataset = input.Definition.Name,
                sourceType = etlContext.SourceType,
                discoveredAt = DateTimeOffset.UtcNow,
                files = input.Files.Select(f => new
                {
                    key = f.StorageObject.Key,
                    container = f.StorageObject.Container,
                    size = f.StorageObject.Size,
                    timestamp = f.Timestamp
                })
            },
            new JsonSerializerOptions { WriteIndented = true });

        var objectKey = $"discovery/{etlContext.RunId}/{input.Definition.Name}.json";

        await blobStorageFactory
            .GetSourceInternal()
            .UploadAsync(objectKey, Encoding.UTF8.GetBytes(manifest), "application/json", null, cancellationToken);

        logger.LogInformation("Wrote discovery manifest {ObjectKey} (runId={RunId})", objectKey, etlContext.RunId);

        return input;
    }
}
