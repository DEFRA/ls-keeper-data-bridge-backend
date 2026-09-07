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

        foreach (var rawFileKey in input.Files)
        {
            var destKey = await NormaliseFileAsync(
                input.Definition, rawFileKey, rawStorage, normalisedStorage, cancellationToken);

            normalisedFiles.Add(destKey);
        }

        return new NormalisedFileSet(input.Definition)
        {
            RunId = etlContext.RunId,
            Files = normalisedFiles
        };
    }

    /// <summary>Normalises one raw file to Parquet, or reuses the destination if it already exists.
    /// Returns the destination key either way.</summary>
    private async Task<string> NormaliseFileAsync(
        DataSetDefinition definition,
        string rawFileKey,
        IBlobStorageService rawStorage,
        IBlobStorageService normalisedStorage,
        CancellationToken cancellationToken)
    {
        // Determine relative keys based on storage provider folder structure
        var relativeRawKey = rawFileKey.StartsWith("raw/") ? rawFileKey.Substring(4) : rawFileKey;
        var relativeDestKey = DestinationKeyFor(definition, relativeRawKey);

        if (await normalisedStorage.ExistsAsync(relativeDestKey, cancellationToken))
        {
            logger.LogInformation("Skipping normalisation, file already exists: {DestKey}", relativeDestKey);
            return relativeDestKey;
        }

        var isHcdtFormat = definition.Format == FileFormat.Hcdt;

        logger.LogInformation("Normalising {RawFileKey} to {DestKey}. Format: {Format}",
            relativeRawKey, relativeDestKey, isHcdtFormat ? "H/C/D/T" : "Simple PSV");

        await EtlArtefactWrite.RunAsync(normalisedStorage, relativeDestKey, async () =>
        {
            await using var sourceStream = await rawStorage.OpenReadAsync(relativeRawKey, cancellationToken);
            await using var destStream = await normalisedStorage.OpenWriteAsync(
                relativeDestKey,
                SnapshotFileNaming.ParquetContentType,
                cancellationToken: cancellationToken);

            if (isHcdtFormat)
            {
                await NormaliseDeclaredHcdtAsync(sourceStream, destStream, relativeRawKey, cancellationToken);
            }
            else
            {
                await ConvertSimplePsvToParquetAsync(sourceStream, destStream, cancellationToken);
            }
        }, logger);

        return relativeDestKey;
    }

    /// <summary>Storage returned by ForFolder(Normalised) is already rooted at normalised/. Keep
    /// every dataset's files together so downstream snapshot discovery can use the dataset prefix
    /// and re-runs resolve to the same target.</summary>
    private static string DestinationKeyFor(DataSetDefinition definition, string relativeRawKey)
    {
        var fileName = Path.GetFileNameWithoutExtension(relativeRawKey);
        return $"{definition.Name}/{fileName}.parquet";
    }

    /// <summary>A dataset declared H/C/D/T is not always actually H/C/D/T in practice, so the source
    /// is buffered and peeked before committing to a parser: a genuine H/C/D/T file is handed to the
    /// NuGet normaliser, and one that turns out not to carry the leading H header falls back to
    /// simple PSV parsing instead of failing.</summary>
    private async Task NormaliseDeclaredHcdtAsync(
        Stream sourceStream, Stream destStream, string relativeRawKey, CancellationToken cancellationToken)
    {
        await using var buffered = new MemoryStream();
        await sourceStream.CopyToAsync(buffered, cancellationToken);
        buffered.Position = 0;

        if (await LooksLikeHcdtAsync(buffered, cancellationToken))
        {
            await NormaliseHcdtAsync(buffered, destStream, cancellationToken);
        }
        else
        {
            logger.LogWarning(
                "File did not appear to be H/C/D/T despite dataset format; falling back to PSV parsing for {Key}.",
                relativeRawKey);

            await ConvertSimplePsvToParquetAsync(buffered, destStream, cancellationToken);
        }
    }

    /// <summary>Peeks the first non-empty line for an H header (e.g. "H|" or "H,"), leaving the
    /// stream repositioned at the start for whichever parser is chosen next.</summary>
    private static async Task<bool> LooksLikeHcdtAsync(MemoryStream buffered, CancellationToken cancellationToken)
    {
        string? firstLine = null;

        using (var reader = new StreamReader(buffered, System.Text.Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1024, leaveOpen: true))
        {
            while (true)
            {
                firstLine = await reader.ReadLineAsync(cancellationToken);
                if (firstLine == null || !string.IsNullOrWhiteSpace(firstLine))
                    break;
            }
        }

        buffered.Position = 0;

        return firstLine != null && firstLine.TrimStart().StartsWith("H", StringComparison.OrdinalIgnoreCase);
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

        // Match legacy CsvHelper config. The delimiter is detected rather than assumed: most feeds
        // are pipe-delimited, but a dataset declared as H/C/D/T that turns out not to carry an "H"
        // header (CTS's bulk and delta files) is still a true comma-delimited CSV, and parsing it
        // with "|" leaves the whole line as a single column.
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            DetectDelimiter = true,
            DetectDelimiterValues = ["|", ","],
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
