using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Optimise;
using KeeperData.Core.EtlPipeline.Parquet;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Schema;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Converts each normalised file into its optimised form in optimised/ - columns the
/// dataset does not project are dropped, columns the dataset types are converted, and rows its
/// filter rejects never arrive. Materialises: optimised/.
///
/// A file the dataset configures nothing for is passed through untouched: the payload points the
/// snapshot at the normalised original rather than paying a copy for the same bytes. A configured
/// file is rewritten once; like every artefact in the pipeline an existing optimised/ object is
/// never touched again, so the definition's settings are baked into the artefact at first write.
///
/// Type detection samples the first row group only. A column that changes type deeper in the file
/// fails the import rather than degrading quietly: <see cref="SourceFileConversionException"/> names
/// the file, column and record so the misclassified value can be found.</summary>
public sealed class OptimiseStage(
    IEtlPipelineStorageProvider storageProvider,
    ILogger<OptimiseStage> logger) : MapStage<NormalisedFileSet, OptimisedFileSet>
{
    public override string Name => "optimise";

    protected override async Task<OptimisedFileSet> MapAsync(NormalisedFileSet input, IPipelineContext context, CancellationToken cancellationToken)
    {
        var etlContext = (EtlPipelineContext)context;
        var definition = input.Definition;

        OptimisePlan.ValidateDefinition(definition);

        var normalised = storageProvider.ForFolder(EtlPipelineFolders.Normalised);
        var optimised = storageProvider.ForFolder(EtlPipelineFolders.Optimised);

        var keys = await KeysAsync(input, normalised, cancellationToken);
        var files = new List<OptimisedFile>(keys.Count);

        foreach (var key in keys)
            files.Add(await OptimiseFileAsync(definition, key, normalised, optimised, cancellationToken));

        return new OptimisedFileSet(definition)
        {
            RunId = etlContext.RunId,
            Files = files
        };
    }

    /// <summary>The file's optimised identity: where the snapshot finds it.</summary>
    private async Task<OptimisedFile> OptimiseFileAsync(
        DataSetDefinition definition,
        string key,
        IBlobStorageService normalised,
        IBlobStorageService optimised,
        CancellationToken cancellationToken)
    {
        if (PassThrough(definition))
        {
            return new OptimisedFile(EtlPipelineFolders.Normalised, key);
        }

        if (await optimised.ExistsAsync(key, cancellationToken))
        {
            logger.LogInformation("Skipping optimisation, file already exists: {DestKey}", key);
            return new OptimisedFile(EtlPipelineFolders.Optimised, key);
        }

        await EtlArtefactWrite.RunAsync(optimised, key, async () =>
        {
            await using var sourceStream = await normalised.OpenReadAsync(key, cancellationToken);
            await using var seekable = await ParquetStreams.AsSeekableAsync(sourceStream, cancellationToken);
            await using var reader = await ParquetReader.CreateAsync(seekable, cancellationToken: cancellationToken);

            var fields = reader.Schema.GetDataFields();
            var detected = await DetectAsync(reader, fields, definition, cancellationToken);
            var plan = OptimisePlan.Resolve(definition, fields, detected);

            WarnOnUnknownColumns(definition, fields);

            await using var destination = await optimised.OpenWriteAsync(
                key, SnapshotFileNaming.ParquetContentType, cancellationToken: cancellationToken);

            if (plan.IsIdentity && definition.RowFilter is null)
            {
                // Nothing to change, but the artefact still materialises so later runs skip the
                // detection pass: a byte copy is cheaper than a re-encode.
                seekable.Position = 0;
                await seekable.CopyToAsync(destination, cancellationToken);
                return;
            }

            await new ParquetOptimiser(definition, key, logger).RunAsync(reader, plan, destination, cancellationToken);
        }, logger);

        return new OptimisedFile(EtlPipelineFolders.Optimised, key);
    }

    /// <summary>The dataset that configures nothing the stage does never touches storage: its files
    /// are referenced where they already are.</summary>
    private static bool PassThrough(DataSetDefinition definition)
        => !definition.AutoDetectColumnTypes
            && definition.ColumnTypes is not { Count: > 0 }
            && definition.IncludedColumns is not { Length: > 0 }
            && definition.ExcludedColumns is not { Length: > 0 }
            && definition.RowFilter is null;

    /// <summary>Auto-detection samples the first row group. Null when the dataset opts out - every
    /// undeclared column then keeps its string type.</summary>
    private static async Task<IReadOnlyDictionary<string, ColumnDataType>?> DetectAsync(
        ParquetReader reader, DataField[] fields, DataSetDefinition definition, CancellationToken cancellationToken)
    {
        if (!definition.AutoDetectColumnTypes)
        {
            return null;
        }

        var detected = new Dictionary<string, ColumnDataType>(StringComparer.OrdinalIgnoreCase);

        if (reader.RowGroupCount == 0)
        {
            return detected;
        }

        using var rowGroup = reader.OpenRowGroupReader(0);
        var sampleRows = (int)Math.Min(rowGroup.RowCount, definition.TypeDetectionSampleRows);

        foreach (var field in fields)
        {
            var values = await ParquetColumns.ReadAsStringsAsync(rowGroup, field, cancellationToken);
            detected[field.Name] = ColumnTypeDetector.Detect(values.Take(sampleRows));
        }

        return detected;
    }

    /// <summary>An include or exclude naming a column the file does not carry is ignored, but worth
    /// a warning: a renamed source column silently survives a projection meant to drop it.</summary>
    private void WarnOnUnknownColumns(DataSetDefinition definition, DataField[] fields)
    {
        var present = new HashSet<string>(fields.Select(field => field.Name), StringComparer.OrdinalIgnoreCase);

        foreach (var name in (IEnumerable<string>?)definition.IncludedColumns ?? definition.ExcludedColumns)
        {
            if (!present.Contains(name))
            {
                logger.LogWarning(
                    "Dataset {DataSet} projects column {Column}, which the file does not carry; the entry is ignored",
                    definition.Name, name);
            }
        }
    }

    /// <summary>The keys the payload carries, falling back to listing the dataset's folder while the
    /// normalise stage does not yet populate them.</summary>
    private static async Task<IReadOnlyList<string>> KeysAsync(
        NormalisedFileSet input, IBlobStorageService normalised, CancellationToken cancellationToken)
    {
        if (input.Files.Count > 0)
        {
            return input.Files;
        }

        var objects = await normalised.ListAsync(SnapshotFileNaming.DataSetPrefix(input.Definition), cancellationToken);

        return [.. objects.Select(o => o.Key)];
    }
}
