using System.Globalization;
using System.Text;
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

    /// <summary>Enough of a file to hold its first record, whichever framing it turns out to carry.</summary>
    private const int HeadPeekBytes = 8 * 1024;

    private static ReadOnlySpan<byte> Utf8Bom => [0xEF, 0xBB, 0xBF];

    private static ReadOnlySpan<char> HcdtDelimiters => "|,";

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

        // The head is read before the destination is opened: an empty file has no schema to write, and
        // an object that exists but holds no Parquet would be skipped as done by every later run.
        var head = new byte[HeadPeekBytes];
        var headLength = await sourceStream.ReadAtLeastAsync(
            head, head.Length, throwOnEndOfStream: false, cancellationToken);

        if (headLength == 0)
        {
            logger.LogWarning("Nothing to normalise: {RawFileKey} is empty, so no Parquet is written", relativeRawKey);
            return null;
        }

        if (isHcdtFormat && !LooksLikeHcdt(head.AsSpan(0, headLength)))
        {
            throw new InvalidDataException(
                $"{relativeRawKey} is declared as {nameof(FileFormat.Hcdt)} but its first record is not an H header. " +
                $"A delimited file carrying no H/C/D/T framing must be declared as {nameof(FileFormat.SimplePsv)}.");
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

    /// <summary>Storage returned by ForFolder(Normalised) is already rooted at normalised/. Keep
    /// every dataset's files together so downstream snapshot discovery can use the dataset prefix
    /// and re-runs resolve to the same target.</summary>
    private static string DestinationKeyFor(DataSetDefinition definition, string relativeRawKey)
    {
        var fileName = Path.GetFileNameWithoutExtension(relativeRawKey);
        return $"{definition.Name}/{fileName}.parquet";
    }

    /// <summary>Reads the first non-empty line of the peeked head and requires an H record: the marker
    /// followed by a delimiter, so a header column such as HOLDING_ID does not read as framing. A head
    /// holding nothing but blank lines is left to the normaliser, which accepts a zero-record file.</summary>
    private static bool LooksLikeHcdt(ReadOnlySpan<byte> head)
    {
        if (head.StartsWith(Utf8Bom))
        {
            head = head[Utf8Bom.Length..];
        }

        // A head short of a line break still decides it: only the start of the first line is read.
        foreach (var line in Encoding.UTF8.GetString(head).Split('\n'))
        {
            var candidate = line.Trim();
            if (candidate.Length == 0)
            {
                continue;
            }

            return candidate.Length > 1
                && (candidate[0] is 'H' or 'h')
                && HcdtDelimiters.Contains(candidate[1]);
        }

        return true;
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

        if (report.DeclaredRecordCount != report.ActualDataRecords)
        {
            logger.LogWarning(
                "H/C/D/T trailer count disagrees with the records read. Declared: {Declared}, Actual: {Actual}",
                report.DeclaredRecordCount, report.ActualDataRecords);
        }
    }

    private async Task ConvertSimplePsvToParquetAsync(Stream source, Stream dest, CancellationToken ct)
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
