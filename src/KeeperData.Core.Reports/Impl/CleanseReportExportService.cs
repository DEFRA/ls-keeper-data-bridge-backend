using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO.Compression;
using CsvHelper;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Reports.Impl;

/// <summary>
/// Service for exporting cleanse reports to CSV and uploading to S3.
/// Streams data from MongoDB to minimize memory usage.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Export service with S3 and streaming dependencies - covered by integration tests.")]
public class CleanseReportExportService : ICleanseReportExportService
{
    private const string CsvFileName = "cleanse-report.csv";
    private const int StreamBatchSize = 1000;

    private readonly ICleanseReportRepository _reportRepository;
    private readonly IBlobStorageServiceFactory _blobStorageServiceFactory;
    private readonly ILogger<CleanseReportExportService> _logger;

    public CleanseReportExportService(
        ICleanseReportRepository reportRepository,
        IBlobStorageServiceFactory blobStorageServiceFactory,
        ILogger<CleanseReportExportService> logger)
    {
        _reportRepository = reportRepository;
        _blobStorageServiceFactory = blobStorageServiceFactory;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<CleanseReportExportResult> ExportAndUploadAsync(string operationId, CancellationToken ct = default)
    {
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
        var zipFileName = $"cleanse-report_{timestamp}.zip";

        string? tempCsvPath = null;
        string? tempZipPath = null;

        try
        {
            _logger.LogInformation(
                "Starting cleanse report export for operation {OperationId}, timestamp {Timestamp}",
                operationId, timestamp);

            // Step 1: Stream issues from MongoDB directly to CSV file
            tempCsvPath = Path.GetTempFileName();
            var recordCount = await StreamIssuesToCsvAsync(tempCsvPath, ct);
            _logger.LogInformation("Streamed {RecordCount} issues to CSV file at {TempPath}", recordCount, tempCsvPath);

            // Step 2: Create zip file
            tempZipPath = Path.GetTempFileName();
            CreateZipFile(tempCsvPath, tempZipPath, CsvFileName);
            _logger.LogInformation("Created zip file at {ZipPath}", tempZipPath);

            // Step 3: Upload to S3 (using the cleanse reports blob service which has the correct prefix)
            var blobService = _blobStorageServiceFactory.GetCleanseReportsBlobService();
            var zipContent = await File.ReadAllBytesAsync(tempZipPath, ct);
            await blobService.UploadAsync(zipFileName, zipContent, "application/zip", cancellationToken: ct);
            _logger.LogInformation("Uploaded report to S3 with key {ObjectKey}", zipFileName);

            // Step 4: Generate presigned URL (using the zip file name as the key - blob service handles the prefix)
            var presignedUrl = blobService.GeneratePresignedUrl(zipFileName);
            _logger.LogInformation(
                "Generated presigned URL for cleanse report (operation {OperationId}): {ReportUrl}",
                operationId, presignedUrl);

            return new CleanseReportExportResult
            {
                Success = true,
                ReportUrl = presignedUrl,
                ObjectKey = zipFileName
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to export cleanse report for operation {OperationId}", operationId);
            return new CleanseReportExportResult
            {
                Success = false,
                Error = ex.Message
            };
        }
        finally
        {
            // Clean up temp files
            CleanupTempFile(tempCsvPath);
            CleanupTempFile(tempZipPath);
        }
    }

    /// <summary>
    /// Streams issues from MongoDB directly to CSV file using CsvHelper.
    /// Memory footprint is O(batch_size) instead of O(total_records).
    /// </summary>
    private async Task<int> StreamIssuesToCsvAsync(string filePath, CancellationToken ct)
    {
        var recordCount = 0;

        await using var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536, useAsync: true);
        await using var writer = new StreamWriter(fileStream);
        await using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

        csv.Context.RegisterClassMap<CleanseReportCsvMap>();

        // Write header
        csv.WriteHeader<CleanseReportCsvRecord>();
        await csv.NextRecordAsync();

        // Stream records from MongoDB and write directly to CSV
        await foreach (var issue in _reportRepository.StreamActiveIssuesAsync(StreamBatchSize, ct))
        {
            var record = CleanseReportCsvRecord.FromDomain(issue);
            csv.WriteRecord(record);
            await csv.NextRecordAsync();
            recordCount++;

            // Periodic flush to avoid buffering too much in StreamWriter
            if (recordCount % 10000 == 0)
            {
                await csv.FlushAsync();
                _logger.LogDebug("Streamed {RecordCount} records to CSV...", recordCount);
            }
        }

        await csv.FlushAsync();
        return recordCount;
    }

    private static void CreateZipFile(string sourceFilePath, string zipFilePath, string entryName)
    {
        // Delete existing file if it exists (since we're using GetTempFileName which creates the file)
        if (File.Exists(zipFilePath))
            File.Delete(zipFilePath);

        using var zipArchive = ZipFile.Open(zipFilePath, ZipArchiveMode.Create);
        zipArchive.CreateEntryFromFile(sourceFilePath, entryName, CompressionLevel.Optimal);
    }

    private void CleanupTempFile(string? filePath)
    {
        if (string.IsNullOrEmpty(filePath))
            return;

        try
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to clean up temp file {FilePath}", filePath);
        }
    }
}
