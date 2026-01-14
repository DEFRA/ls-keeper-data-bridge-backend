using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using KeeperData.Core.Telemetry;
using Microsoft.Extensions.Logging;
using System.Collections.Immutable;
using System.Diagnostics;

namespace KeeperData.Core.ETL.Impl;

public class AcquisitionPipeline(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IExternalCatalogueServiceFactory ExternalCatalogueServiceFactory,
    IAesCryptoTransform aesCryptoTransform,
    IPasswordSaltService passwordSalt,
    IImportReportingService reportingService,
    IApplicationMetrics metrics,
    ILogger<AcquisitionPipeline> logger) : IAcquisitionPipeline
{
    private const string MimeTypeTextCsv = "text/csv";

    public async Task StartAsync(ImportReport report, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        logger.LogInformation("Starting import pipeline for ImportId: {ImportId}, SourceType: {SourceType}", report.ImportId, report.SourceType);

        metrics.RecordRequest("acquisition_pipeline", "started");
        metrics.RecordCount("acquisition_started", 1, ("source_type", report.SourceType));

        try
        {
            var storageServices = InitializeStorageServices(report.ImportId, report.SourceType);

            var (fileSets, totalFiles) = await DiscoverFilesAsync(report.ImportId, storageServices.ExternalCatalogueService, ct);

            // Update acquisition phase to Started
            report.AcquisitionPhase!.Status = PhaseStatus.Started;
            report.AcquisitionPhase!.FilesDiscovered = totalFiles;
            report.AcquisitionPhase!.StartedAtUtc = DateTime.UtcNow;
            await reportingService.UpsertImportReportAsync(report, ct);

            var (processedCount, skippedCount) = await ProcessAllFilesAsync(report, fileSets, totalFiles,
                storageServices.SourceBlobs, storageServices.DestinationBlobs, ct);

            report.AcquisitionPhase!.Status = PhaseStatus.Completed;
            report.AcquisitionPhase!.FilesProcessed = processedCount;
            report.AcquisitionPhase!.FilesSkipped = skippedCount;
            report.AcquisitionPhase!.CompletedAtUtc = DateTime.UtcNow;
            await reportingService.UpsertImportReportAsync(report, ct);

            LogPipelineCompletion(report.ImportId, stopwatch);

            stopwatch.Stop();

            metrics.RecordRequest("acquisition_pipeline", "completed");
            metrics.RecordDuration("acquisition_pipeline", stopwatch.ElapsedMilliseconds);
            metrics.RecordCount("acquisition_completions", 1,
                ("source_type", report.SourceType),
                ("status", "success"));

            metrics.RecordCount("files_discovered", totalFiles,
                ("source_type", report.SourceType),
                ("file_sets", fileSets.Count.ToString()));
            metrics.RecordCount("files_processed_acquisition", processedCount,
                ("source_type", report.SourceType));
            metrics.RecordCount("files_skipped_acquisition", skippedCount,
                ("source_type", report.SourceType));

            if (totalFiles > 0)
            {
                var processingRatio = (double)processedCount / totalFiles;
                metrics.RecordValue("acquisition_processing_ratio", processingRatio,
                    ("source_type", report.SourceType));
            }
        }
        catch (Exception ex)
        {
            report.AcquisitionPhase!.Status = PhaseStatus.Failed;
            report.AcquisitionPhase!.CompletedAtUtc = DateTime.UtcNow;
            await reportingService.UpsertImportReportAsync(report, ct);

            LogPipelineFailure(report.ImportId, stopwatch, ex);

            stopwatch.Stop();

            metrics.RecordRequest("acquisition_pipeline", "failed");
            metrics.RecordDuration("acquisition_pipeline", stopwatch.ElapsedMilliseconds);
            metrics.RecordCount("acquisition_completions", 1,
                ("source_type", report.SourceType),
                ("status", "failed"));
            metrics.RecordCount("acquisition_errors", 1,
                ("source_type", report.SourceType),
                ("error_type", ex.GetType().Name));

            throw;
        }
    }

    private (IBlobStorageServiceReadOnly SourceBlobs, ExternalCatalogueService ExternalCatalogueService, IBlobStorageService DestinationBlobs)
        InitializeStorageServices(Guid importId, string sourceType)
    {
        var sourceBlobs = blobStorageServiceFactory.GetSource(sourceType);
        var catalogueService = ExternalCatalogueServiceFactory.Create(sourceBlobs);
        var destinationBlobs = blobStorageServiceFactory.Get();

        logger.LogDebug("Initialized blob storage services for ImportId: {ImportId}", importId);

        return (sourceBlobs, catalogueService, destinationBlobs);
    }

    private async Task<(ImmutableList<FileSet> FileSets, int TotalFiles)> DiscoverFilesAsync(Guid importId, IExternalCatalogueService catalogueService, CancellationToken ct)
    {
        var discoveryStopwatch = Stopwatch.StartNew();
        logger.LogInformation("Step 1: Discovering files for ImportId: {ImportId}", importId);

        var fileSets = await catalogueService.GetFileSetsAsync(EtlConstants.DefaultLookbackDays, ct);
        var totalFiles = fileSets.Sum(fs => fs.Files.Length);

        logger.LogInformation("Discovered {FileSetCount} file set(s) containing {TotalFileCount} file(s) for ImportId: {ImportId}",
            fileSets.Count,
            totalFiles,
            importId);

        discoveryStopwatch.Stop();

        metrics.RecordDuration("file_discovery", discoveryStopwatch.ElapsedMilliseconds);
        metrics.RecordCount("file_sets_discovered", fileSets.Count);
        metrics.RecordValue("avg_files_per_set", fileSets.Count > 0 ? (double)totalFiles / fileSets.Count : 0);

        return (fileSets, totalFiles);
    }

    private async Task<(int ProcessedCount, int SkippedFileCount)> ProcessAllFilesAsync(ImportReport report, ImmutableList<FileSet> fileSets,
        int totalFiles, IBlobStorageServiceReadOnly sourceBlobs, IBlobStorageService destinationBlobs, CancellationToken ct)
    {
        logger.LogInformation("Step 2: Processing and decrypting files for ImportId: {ImportId}", report.ImportId);

        var processedFileCount = 0;
        var skippedFileCount = 0;

        foreach (var fileSet in fileSets)
        {
            logger.LogDebug("Processing file set for definition: {DefinitionName} with {FileCount} file(s) for ImportId: {ImportId}",
                fileSet.Definition.Name, fileSet.Files.Length, report.ImportId);

            foreach (var file in fileSet.Files)
            {
                processedFileCount++;

                var result = await ProcessSingleFileAsync(report.ImportId, fileSet, file, processedFileCount, totalFiles, sourceBlobs, destinationBlobs, ct);

                if (result == ProcessSingleFileResult.Skipped)
                {
                    skippedFileCount++;
                }
            }
        }

        logger.LogInformation("Step 2 completed: Processed {ProcessedFileCount} file(s) for ImportId: {ImportId}",
            processedFileCount,
            report.ImportId);

        return (processedFileCount, skippedFileCount);
    }

    private enum ProcessSingleFileResult
    {
        Skipped,
        Processed,
    }

    private async Task<ProcessSingleFileResult> ProcessSingleFileAsync(Guid importId, FileSet fileSet, EtlFile file, int currentFileNumber, int totalFiles,
        IBlobStorageServiceReadOnly sourceBlobs, IBlobStorageService destinationBlobs, CancellationToken ct)
    {
        var fileStopwatch = Stopwatch.StartNew();

        logger.LogInformation("Processing file {CurrentFile}/{TotalFiles}: {FileKey} for ImportId: {ImportId}", currentFileNumber,
            totalFiles, file.StorageObject.Key, importId);

        try
        {
            var fileContext = await PrepareFileContextAsync(file, sourceBlobs, ct);

            var transferDecision = await DetermineFileTransferRequirementAsync(
                file.StorageObject.Key,
                fileContext.EncryptedMetadata.ContentLength,
                fileContext.SourceETag,
                destinationBlobs,
                importId,
                ct);

            // Early return for skipped files WITHOUT recording acquisition
            if (transferDecision.ShouldSkip)
            {
                fileStopwatch.Stop();
                logger.LogInformation("Skipped file {FileKey} - already acquired in previous import (Duration: {Duration}ms) for ImportId: {ImportId}",
                    file.StorageObject.Key,
                    fileStopwatch.ElapsedMilliseconds,
                    importId);

                return ProcessSingleFileResult.Skipped;
            }

            // File needs to be acquired
            var acquisitionResult = await AcquireFileAsync(fileContext, transferDecision, destinationBlobs, ct);

            fileStopwatch.Stop();

            // Only record acquisition for files that were actually transferred
            await RecordSuccessfulAcquisitionAsync(importId, fileSet, file, acquisitionResult, fileStopwatch.ElapsedMilliseconds, ct);

            return ProcessSingleFileResult.Processed;
        }
        catch (Exception ex)
        {
            fileStopwatch.Stop();
            logger.LogError(ex, "Failed to process file: {FileKey} after {Duration}ms for ImportId: {ImportId}",
                file.StorageObject.Key,
                fileStopwatch.ElapsedMilliseconds,
                importId);

            await RecordFailedAcquisitionAsync(importId, fileSet, file, fileStopwatch.ElapsedMilliseconds, ex, ct);

            throw;
        }
    }

    private async Task<FileContext> PrepareFileContextAsync(EtlFile file, IBlobStorageServiceReadOnly sourceBlobs, CancellationToken ct)
    {
        var encryptedStream = await sourceBlobs.OpenReadAsync(file.StorageObject.Key, ct);
        var encryptedMetadata = await sourceBlobs.GetMetadataAsync(file.StorageObject.Key, ct);
        var credentials = passwordSalt.Get(file.StorageObject.Key);

        // Capture source ETag for file comparison
        var sourceETag = encryptedMetadata.ETag ?? string.Empty;

        logger.LogDebug("Loaded file context: {FileKey}, ContentLength: {ContentLength} bytes, SourceETag: {SourceETag}",
            file.StorageObject.Key, encryptedMetadata.ContentLength, sourceETag);

        return new FileContext(file.StorageObject.Key, encryptedStream, encryptedMetadata, credentials.Password, credentials.Salt, sourceETag);
    }

    private async Task<FileTransferDecision> DetermineFileTransferRequirementAsync(
        string fileKey,
        long sourceEncryptedLength,
        string sourceETag,
        IBlobStorageService destinationBlobs,
        Guid importId,
        CancellationToken ct)
    {
        var targetExists = await destinationBlobs.ExistsAsync(fileKey, ct);

        if (!targetExists)
        {
            logger.LogDebug("File transfer required for {FileKey} - target does not exist", fileKey);
            return FileTransferDecision.TransferRequired();
        }

        var targetMetadata = await destinationBlobs.GetMetadataAsync(fileKey, ct);

        // Use S3-compliant metadata keys (lowercase with x-amz-meta- prefix)
        if (!targetMetadata.UserMetadata.TryGetValue(EtlConstants.MetadataKeySourceEncryptedLength, out var storedSourceLength) ||
            !targetMetadata.UserMetadata.TryGetValue(EtlConstants.MetadataKeySourceETag, out var storedSourceETag))
        {
            logger.LogInformation("Re-acquiring {FileKey} - missing source metadata (will add metadata). Available keys: {Keys}",
                fileKey, string.Join(", ", targetMetadata.UserMetadata.Keys));
            return FileTransferDecision.TransferRequired();
        }

        // Compare source length
        if (!long.TryParse(storedSourceLength, out var storedLength) || storedLength != sourceEncryptedLength)
        {
            logger.LogInformation("Re-acquiring {FileKey} - source length changed from {OldLength} to {NewLength} bytes",
                fileKey, storedLength, sourceEncryptedLength);
            return FileTransferDecision.TransferRequired();
        }

        // Normalize and compare ETags
        var normalizedStoredETag = NormalizeETag(storedSourceETag);
        var normalizedSourceETag = NormalizeETag(sourceETag);

        if (!string.Equals(normalizedStoredETag, normalizedSourceETag, StringComparison.OrdinalIgnoreCase))
        {
            logger.LogInformation("Re-acquiring {FileKey} - source ETag changed from '{OldETag}' to '{NewETag}'",
                fileKey, normalizedStoredETag, normalizedSourceETag);
            return FileTransferDecision.TransferRequired();
        }

        // File is identical - skip transfer
        var targetETag = targetMetadata.ETag ?? string.Empty;

        logger.LogInformation("Skipping file transfer for {FileKey} - target exists with matching source (Length: {SourceLength} bytes, SourceETag: {SourceETag}, Target size: {TargetSize} bytes, TargetETag: {TargetETag}) for ImportId: {ImportId}",
            fileKey,
            sourceEncryptedLength,
            normalizedSourceETag,
            targetMetadata.ContentLength,
            targetETag,
            importId);

        return FileTransferDecision.SkipTransfer(
            targetMetadata.ContentLength,
            targetETag);
    }

    /// <summary>
    /// Normalizes an ETag for comparison by trimming quotes and whitespace.
    /// S3 ETags are case-insensitive and may be quoted.
    /// </summary>
    /// <param name="etag">The ETag to normalize</param>
    /// <returns>Normalized ETag string</returns>
    private static string NormalizeETag(string? etag)
    {
        return (etag ?? string.Empty).Trim('"').Trim();
    }

    private async Task<FileAcquisitionResult> AcquireFileAsync(FileContext fileContext, FileTransferDecision transferDecision,
        IBlobStorageService destinationBlobs, CancellationToken ct)
    {
        if (transferDecision.ShouldSkip)
        {
            // File transfer is skipped, but return the existing metadata
            // so it can still be recorded for ingestion to find
            logger.LogInformation("Skipping file transfer for {FileKey} - using existing target file (ETag: {ETag}, Size: {FileSize} bytes)",
                fileContext.FileKey,
                transferDecision.ExistingETag,
                transferDecision.ExistingFileSize);

            return new FileAcquisitionResult(transferDecision.ExistingETag, transferDecision.ExistingFileSize);
        }

        // Decrypt and upload (without computing MD5)
        var fileSize = await DecryptAndUploadAsync(
            fileContext.EncryptedStream,
            destinationBlobs,
            fileContext.FileKey,
            fileContext.Password,
            fileContext.Salt,
            fileContext.EncryptedMetadata.ContentLength,
            ct);

        // Store source metadata in target
        await StoreFileMetadataAsync(
            destinationBlobs,
            fileContext.FileKey,
            fileContext.EncryptedMetadata.ContentLength,
            fileContext.SourceETag,
            ct);

        // Get target file ETag after upload
        var targetMetadata = await destinationBlobs.GetMetadataAsync(fileContext.FileKey, ct);
        var targetETag = targetMetadata.ETag ?? string.Empty;

        logger.LogInformation("Successfully processed file: {FileKey} ({SizeMB:F2} MB, Target ETag: {ETag})",
            fileContext.FileKey,
            fileSize / (1024.0 * 1024.0),
            targetETag);

        return new FileAcquisitionResult(targetETag, fileSize);
    }

    private async Task StoreFileMetadataAsync(
        IBlobStorageService destinationBlobs,
        string fileKey,
        long sourceEncryptedLength,
        string sourceETag,
        CancellationToken ct)
    {
        // Use S3-compliant metadata keys (lowercase with x-amz-meta- prefix)
        var metadata = new Dictionary<string, string>
        {
            { EtlConstants.MetadataKeySourceEncryptedLength, sourceEncryptedLength.ToString() },
            { EtlConstants.MetadataKeySourceETag, sourceETag }
        };

        await destinationBlobs.SetMetadataAsync(fileKey, metadata, ct);
    }

    private async Task RecordSuccessfulAcquisitionAsync(Guid importId, FileSet fileSet, EtlFile file, FileAcquisitionResult acquisitionResult,
        long durationMs, CancellationToken ct)
    {
        await reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
        {
            FileName = Path.GetFileName(file.StorageObject.Key),
            FileKey = file.StorageObject.Key,
            DatasetName = fileSet.Definition.Name,
            ETag = acquisitionResult.ETag,
            FileSize = acquisitionResult.FileSize,
            SourceKey = file.StorageObject.Key,
            DecryptionDurationMs = durationMs,
            AcquiredAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Acquired
        }, ct);
    }

    private async Task RecordFailedAcquisitionAsync(
        Guid importId,
        FileSet fileSet,
        EtlFile file,
        long durationMs,
        Exception ex,
        CancellationToken ct)
    {
        try
        {
            await reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
            {
                FileName = Path.GetFileName(file.StorageObject.Key),
                FileKey = file.StorageObject.Key,
                DatasetName = fileSet.Definition.Name,
                ETag = string.Empty,
                FileSize = 0,
                SourceKey = file.StorageObject.Key,
                DecryptionDurationMs = durationMs,
                AcquiredAtUtc = DateTime.UtcNow,
                Status = FileProcessingStatus.Failed,
                Error = ex.Message
            }, ct);
        }
        catch (Exception reportEx)
        {
            logger.LogError(reportEx, "Failed to record acquisition failure for file: {FileKey}", file.StorageObject.Key);
        }
    }

    private void LogPipelineCompletion(Guid importId, Stopwatch stopwatch)
    {
        stopwatch.Stop();
        logger.LogInformation("Import pipeline completed successfully for ImportId: {ImportId}. Total duration: {Duration}ms ({DurationSeconds}s)",
            importId,
            stopwatch.ElapsedMilliseconds,
            stopwatch.Elapsed.TotalSeconds);
    }

    private void LogPipelineFailure(Guid importId, Stopwatch stopwatch, Exception ex)
    {
        stopwatch.Stop();
        logger.LogError(ex, "Import pipeline failed for ImportId: {ImportId} after {Duration}ms ({DurationSeconds}s)",
            importId,
            stopwatch.ElapsedMilliseconds,
            stopwatch.Elapsed.TotalSeconds);
    }

    /// <summary>
    /// Decrypts a stream and uploads it while tracking file size.
    /// This streaming approach avoids loading the entire file into memory.
    /// </summary>
    /// <returns>The file size in bytes</returns>
    private async Task<long> DecryptAndUploadAsync(
        Stream encryptedStream,
        IBlobStorageService targetStorage,
        string fileKey,
        string password,
        string salt,
        long encryptedContentLength,
        CancellationToken ct)
    {
        // Create upload stream
        await using var uploadStream = await targetStorage.OpenWriteAsync(fileKey, MimeTypeTextCsv, cancellationToken: ct);

        // Wrap with byte counter to track file size
        await using var byteCounter = new ByteCountingStream(uploadStream);

        // Decrypt directly into the counting+upload stream pipeline
        // Pipeline: Decrypted data → ByteCountingStream (size) → Upload Stream (S3)
        await aesCryptoTransform.DecryptStreamAsync(
            encryptedStream,
            byteCounter,
            password,
            salt,
            encryptedContentLength,
            null,
            ct);

        // Ensure all data is written
        await byteCounter.FlushAsync(ct);

        // Return the file size
        return byteCounter.BytesWritten;
    }

    // Helper records for internal state management
    private record FileContext(
        string FileKey,
        Stream EncryptedStream,
        StorageObjectMetadata EncryptedMetadata,
        string Password,
        string Salt,
        string SourceETag) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await EncryptedStream.DisposeAsync();
        }
    }

    private record FileTransferDecision
    {
        public bool ShouldSkip { get; init; }
        public long ExistingFileSize { get; init; }
        public string ExistingETag { get; init; } = string.Empty;

        public static FileTransferDecision TransferRequired() => new() { ShouldSkip = false };

        public static FileTransferDecision SkipTransfer(long fileSize, string etag) => new()
        {
            ShouldSkip = true,
            ExistingFileSize = fileSize,
            ExistingETag = etag
        };
    }

    private record FileAcquisitionResult(string ETag, long FileSize);
}