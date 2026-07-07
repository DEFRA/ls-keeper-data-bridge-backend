using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>Streams one source file into one Parquet file.</summary>
public interface INormaliser
{
    Task NormaliseAsync(Stream source, Stream parquetOutput, DataSetDefinition dataset, CancellationToken cancellationToken);
}
