using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Payloads;
#pragma warning disable CS9113 // Parameter is unread.

namespace KeeperData.Core.Ingestion.Stages;

/// <summary>Converts each raw file (PSV / legacy H-C-D-T) to Parquet in
/// normalised/. Plain Parquet - NO DuckDB here. Materialises: normalised/.</summary>
public sealed class NormaliseStage(INormaliserFactory normalisers) : MapStage<RawFileSet, NormalisedFileSet>
{
    public override string Name => "normalise";

    protected override Task<NormalisedFileSet> MapAsync(RawFileSet input, IPipelineContext context, CancellationToken cancellationToken)
        // STREAMS per file: open raw -> INormaliser (PsvReader.Stream -> ParquetWriter) -> WriteAtomic to normalised/
        => throw new NotImplementedException();
}
