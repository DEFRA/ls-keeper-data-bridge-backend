using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using XsvHcdtHelper;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Converts each raw file (PSV / legacy H-C-D-T) to Parquet in normalised/. No DuckDB here.</summary>
public sealed class NormaliseStage(
    IEtlPipelineStorageProvider storageProvider,
    IXsvHcdtNormaliser hcdtNormaliser,
    ILogger<NormaliseStage> logger) : MapStage<RawFileSet, NormalisedFileSet>
{
    public override string Name => "normalise";

    protected override async Task<NormalisedFileSet> MapAsync(RawFileSet input, IPipelineContext context, CancellationToken cancellationToken)
    {
        var etlContext = (EtlPipelineContext)context;
        var normalisedFiles = new List<string>();

        var rawStorage = storageProvider.ForFolder(EtlPipelineFolders.Raw);
        var normalisedStorage = storageProvider.ForFolder(EtlPipelineFolders.Normalised);

        // Check if this dataset uses the H/C/D/T format
        bool isHcdtFormat = input.Definition.Format == FileFormat.Hcdt;

        foreach (var rawFileKey in input.Files)
        {
            // Determine relative keys based on storage provider folder structure
            var relativeRawKey = rawFileKey.StartsWith("raw/") ? rawFileKey.Substring(4) : rawFileKey;
            var fileName = Path.GetFileNameWithoutExtension(relativeRawKey);
            // Storage returned by ForFolder(Normalised) is already rooted at normalised/.
            // Keep every dataset's files together so downstream snapshot discovery can use the
            // dataset prefix and re-runs resolve to the same target.
            var relativeDestKey = $"{input.Definition.Name}/{fileName}.parquet";

            if (await normalisedStorage.ExistsAsync(relativeDestKey, cancellationToken))
            {
                logger.LogInformation("Skipping normalisation, file already exists: {DestKey}", relativeDestKey);
                normalisedFiles.Add(relativeDestKey);
                continue;
            }

            logger.LogInformation("Normalising {RawFileKey} to {DestKey}. Format: {Format}",
                relativeRawKey, relativeDestKey, isHcdtFormat ? "H/C/D/T" : "Simple PSV");

            await using var sourceStream = await rawStorage.OpenReadAsync(relativeRawKey, cancellationToken);
            await using var destStream = await normalisedStorage.OpenWriteAsync(
                relativeDestKey,
                SnapshotFileNaming.ParquetContentType,
                cancellationToken: cancellationToken);

            if (isHcdtFormat)
            {
                // Use the NuGet package for H/C/D/T files
                await NormaliseHcdtAsync(sourceStream, destStream, cancellationToken);
            }
            else
            {
                // Use manual chunking for Simple PSV files
                await ConvertSimplePsvToParquetAsync(sourceStream, destStream, cancellationToken);
            }

            normalisedFiles.Add(relativeDestKey);
        }

        return new NormalisedFileSet(input.Definition)
        {
            RunId = etlContext.RunId,
            Files = normalisedFiles
        };
    }

    private async Task NormaliseHcdtAsync(Stream source, Stream dest, CancellationToken ct)
    {
        var report = await hcdtNormaliser.NormaliseAsync(source, dest, options =>
        {
            options.OutputFormat = OutputFormat.Parquet;
            options.InputDelimiter = FieldDelimiter.Auto;
            options.StrictFieldCount = false;
        }, ct);

        logger.LogInformation("H/C/D/T normalisation complete. Declared: {Declared}, Actual: {Actual}",
            report.DeclaredRecordCount, report.ActualDataRecords);
    }

    private async Task ConvertSimplePsvToParquetAsync(Stream source, Stream dest, CancellationToken ct)
    {
        using var reader = new StreamReader(source);

        // Match legacy CsvHelper config
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = "|",
            TrimOptions = TrimOptions.Trim,
            BadDataFound = null
        });

        await csv.ReadAsync();
        csv.ReadHeader();
        var headers = csv.HeaderRecord!;

        var dataFields = headers.Select(h => new DataField<string?>(h)).ToArray();
        var schema = new ParquetSchema(dataFields);

        await using var parquetWriter = await ParquetWriter.CreateAsync(schema, dest, cancellationToken: ct);

        var rowBuffer = new List<string?[]>();
        const int maxRowsPerGroup = 50_000; // Memory boundary

        while (await csv.ReadAsync())
        {
            var row = new string?[headers.Length];
            for (int i = 0; i < headers.Length; i++)
            {
                var val = csv.GetField(i);
                row[i] = string.IsNullOrEmpty(val) ? null : val;
            }
            rowBuffer.Add(row);

            if (rowBuffer.Count >= maxRowsPerGroup)
            {
                await WriteRowGroupAsync(parquetWriter, dataFields, rowBuffer);
                rowBuffer.Clear();
            }
        }

        // Flush any remaining records
        if (rowBuffer.Count > 0)
        {
            await WriteRowGroupAsync(parquetWriter, dataFields, rowBuffer);
        }
    }

    private static async Task WriteRowGroupAsync(ParquetWriter writer, DataField<string?>[] fields, List<string?[]> buffer)
    {
        using var groupWriter = writer.CreateRowGroup();
        for (int col = 0; col < fields.Length; col++)
        {
            var columnData = buffer.Select(r => r[col]).ToArray();

            await groupWriter.WriteAsync(fields[col], columnData);
        }
    }
}
