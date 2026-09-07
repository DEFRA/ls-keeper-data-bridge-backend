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

    /// <summary>Enough of a file to tell an empty one from a file with content.</summary>
    private const int HeadPeekBytes = 1;

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

            if (destKey is not null)
            {
                normalisedFiles.Add(destKey);
            }
        }
        return new NormalisedFileSet(input.Definition)
        {
            RunId = etlContext.RunId,
            Files = normalisedFiles
        };
    }

    /// <summary>Normalises one raw file to Parquet, or reuses the destination if it already exists.
    /// Returns the destination key either way, and null for a file with no content to normalise.</summary>
    private async Task<string?> NormaliseFileAsync(
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

        await using var sourceStream = await rawStorage.OpenReadAsync(relativeRawKey, cancellationToken);

        var (head, headLength) = await PeekHeadAsync(sourceStream, HeadPeekBytes, cancellationToken);

        if (headLength == 0)
        {
            logger.LogWarning("Nothing to normalise: {RawFileKey} is empty, so no Parquet is written", relativeRawKey);
            return null;
        }

        if (isHcdtFormat)
        {
            EnsureHcdtHead(head, headLength, relativeRawKey);
        }

        await EtlArtefactWrite.RunAsync(normalisedStorage, relativeDestKey, async () =>
        {
            await using var source = new HeadPeekStream(head.AsMemory(0, headLength), sourceStream);
            await using var destStream = await normalisedStorage.OpenWriteAsync(
                relativeDestKey,
                SnapshotFileNaming.ParquetContentType,
                cancellationToken: cancellationToken);

            if (isHcdtFormat)
            {
                await NormaliseHcdtAsync(source, destStream, cancellationToken);
            }
            else
            {
                await ConvertSimplePsvToParquetAsync(source, destStream, cancellationToken);
            }
        }, logger);

        return relativeDestKey;
    }

    private static async Task<(byte[] head, int headLength)> PeekHeadAsync(Stream sourceStream, int headPeekBytes, CancellationToken cancellationToken)
    {
        var head = new byte[headPeekBytes];
        var headLength = await sourceStream.ReadAtLeastAsync(
            head, head.Length, throwOnEndOfStream: false, cancellationToken);
        return (head, headLength);
    }

    private static void EnsureHcdtHead(byte[] head, int headLength, string relativeRawKey)
    {
        // Ensure the file actually starts with an H record. We peek only a small head so skip any leading
        // whitespace and check the first non-whitespace character.
        var headSpan = head.AsSpan(0, headLength);
        var idx = 0;
        while (idx < headSpan.Length && char.IsWhiteSpace((char)headSpan[idx])) idx++;
        if (idx == headSpan.Length || char.ToUpperInvariant((char)headSpan[idx]) != 'H')
        {
            throw new XsvValidationException($"H/C/D/T file invalid or misdeclared: {relativeRawKey} does not start with an H record.");
        }
    }

    /// <summary>Storage returned by ForFolder(Normalised) is already rooted at normalised/. Keep
    /// every dataset's files together so downstream snapshot discovery can use the dataset prefix
    /// and re-runs resolve to the same target.</summary>
    private static string DestinationKeyFor(DataSetDefinition definition, string relativeRawKey)
    {
        var fileName = Path.GetFileNameWithoutExtension(relativeRawKey);
        return $"{definition.Name}/{fileName}.parquet";
    }

    private async Task NormaliseHcdtAsync(Stream source, Stream dest, CancellationToken ct)
    {
        var report = await hcdtNormaliser.NormaliseAsync(source, dest, options =>
        {
            options.OutputFormat = OutputFormat.Parquet;
            options.InputDelimiter = FieldDelimiter.Auto;
            options.StrictFieldCount = false;

            // A file's H record is stamped when the extract starts writing it and its T record when it
            // finishes, 45 seconds apart for a bulk cut, so requiring the two to match rejects the file.
            // The trailer's record count is validated separately and stays on.
            options.ValidateHeaderTrailerMatch = false;
        }, ct);

        logger.LogInformation("H/C/D/T normalisation complete. Declared: {Declared}, Actual: {Actual}",
            report.DeclaredRecordCount, report.ActualDataRecords);
    }

    private static async Task ConvertSimplePsvToParquetAsync(Stream source, Stream dest, CancellationToken ct)
    {
        using var reader = new StreamReader(source);

        // Match legacy CsvHelper config. The delimiter is detected rather than assumed, so a
        // comma-delimited cut of an otherwise pipe-delimited feed still reads. Detection settles on
        // the delimiter that gives a consistent field count, so a pipe file whose values contain
        // commas still reads as pipes.
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            DetectDelimiter = true,
            DetectDelimiterValues = ["|", ","],
            Delimiter = "|",
            TrimOptions = TrimOptions.Trim,
            BadDataFound = null
        });

        // A file with no header line at all reaches here only as blank lines: there is no schema to
        // write, and ReadHeader would throw.
        if (!await csv.ReadAsync())
        {
            return;
        }

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