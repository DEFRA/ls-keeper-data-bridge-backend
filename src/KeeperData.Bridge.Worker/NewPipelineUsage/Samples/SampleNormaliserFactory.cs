using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Bridge.Worker.NewPipelineUsage.Samples;

/// <summary>Returns a normaliser per dataset. The real factory would pick PSV vs legacy H/C/D/T;
/// the demo always returns the same sample normaliser.</summary>
public sealed class SampleNormaliserFactory : INormaliserFactory
{
    private readonly SamplePsvToParquetNormaliser _normaliser = new();

    public INormaliser For(DataSetDefinition dataset) => _normaliser;
}

/// <summary>Sample normaliser: copies the source through so the demo flows. The real
/// implementation streams rows (PsvReader) into a ParquetWriter.</summary>
public sealed class SamplePsvToParquetNormaliser : INormaliser
{
    public Task NormaliseAsync(Stream source, Stream parquetOutput, DataSetDefinition dataset, CancellationToken cancellationToken)
        => source.CopyToAsync(parquetOutput, cancellationToken);
}
