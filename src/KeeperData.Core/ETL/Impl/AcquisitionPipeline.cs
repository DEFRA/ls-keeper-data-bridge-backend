using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.Extensions.Logging;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Security.Cryptography;

namespace KeeperData.Core.ETL.Impl;

public class AcquisitionPipeline(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IExternalCatalogueServiceFactory ExternalCatalogueServiceFactory,
    IAesCryptoTransform aesCryptoTransform,
    IPasswordSaltService passwordSalt,
    IImportReportingService reportingService,
    ILogger<AcquisitionPipeline> logger) : IAcquisitionPipeline
{
    private const string MimeTypeTextCsv = "text/csv";

    public async Task StartAsync(ImportReport report, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        logger.LogInformation("Starting import pipeline for ImportId: {ImportId}, SourceType: {SourceType}", report.ImportId, report.SourceType);

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
        }
        catch (Exception ex)
        {
            report.AcquisitionPhase!.Status = PhaseStatus.Failed;
            report.AcquisitionPhase!.CompletedAtUtc = DateTime.UtcNow;
            await reportingService.UpsertImportReportAsync(report, ct);

            LogPipelineFailure(report.ImportId, stopwatch, ex);
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

    private async Task<(ImmutableList<FileSet> FileSets, int TotalFiles)> DiscoverFilesAsync(
        Guid importId,
        IExternalCatalogueService catalogueService,
        CancellationToken ct)
    {
        logger.LogInformation("Step 1: Discovering files for ImportId: {ImportId}", importId);

        var fileSets = await catalogueService.GetFileSetsAsync(100, ct);
        var totalFiles = fileSets.Sum(fs => fs.Files.Length);

        logger.LogInformation("Discovered {FileSetCount} file set(s) containing {TotalFileCount} file(s) for ImportId: {ImportId}",
            fileSets.Count,
            totalFiles,
            importId);

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

            var transferDecision = await DetermineFileTransferRequirementAsync(file.StorageObject.Key, fileContext.EncryptedMetadata.ContentLength, 
                destinationBlobs, importId, ct);

            var acquisitionResult = await AcquireFileAsync(fileContext, transferDecision, destinationBlobs, ct);

            fileStopwatch.Stop();

            await RecordSuccessfulAcquisitionAsync(importId, fileSet, file, acquisitionResult, fileStopwatch.ElapsedMilliseconds, ct);

            return transferDecision.ShouldSkip ? ProcessSingleFileResult.Skipped : ProcessSingleFileResult.Processed;
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

        logger.LogDebug("Loaded file context: {FileKey}, ContentLength: {ContentLength} bytes", file.StorageObject.Key, encryptedMetadata.ContentLength);

        return new FileContext(file.StorageObject.Key, encryptedStream, encryptedMetadata, credentials.Password, credentials.Salt);
    }

    private async Task<FileTransferDecision> DetermineFileTransferRequirementAsync(string fileKey, long sourceEncryptedLength, 
        IBlobStorageService destinationBlobs, Guid importId, CancellationToken ct)
    {
        var targetExists = await destinationBlobs.ExistsAsync(fileKey, ct);

        if (!targetExists)
        {
            return FileTransferDecision.TransferRequired();
        }

        var targetMetadata = await destinationBlobs.GetMetadataAsync(fileKey, ct);

        if (!targetMetadata.UserMetadata.TryGetValue("SourceEncryptedLength", out var storedSourceLength))
        {
            return FileTransferDecision.TransferRequired();
        }

        if (!long.TryParse(storedSourceLength, out var storedLength) || storedLength != sourceEncryptedLength)
        {
            return FileTransferDecision.TransferRequired();
        }

        // File exists with matching source length - skip transfer
        targetMetadata.UserMetadata.TryGetValue("MD5Hash", out var existingMd5Hash);

        logger.LogInformation("Skipping file transfer for {FileKey} - target exists with matching source length {SourceLength} bytes (decrypted size: {DecryptedSize} bytes) for ImportId: {ImportId}",
            fileKey,
            sourceEncryptedLength,
            targetMetadata.ContentLength,
            importId);

        return FileTransferDecision.SkipTransfer(
            targetMetadata.ContentLength,
            existingMd5Hash ?? string.Empty);
    }

    private async Task<FileAcquisitionResult> AcquireFileAsync(FileContext fileContext, FileTransferDecision transferDecision, 
        IBlobStorageService destinationBlobs, CancellationToken ct)
    {
        if (transferDecision.ShouldSkip)
        {
            return new FileAcquisitionResult(transferDecision.ExistingMd5Hash, transferDecision.ExistingFileSize);
        }

        var (md5Hash, fileSize) = await DecryptAndUploadWithMd5Async(
            fileContext.EncryptedStream,
            destinationBlobs,
            fileContext.FileKey,
            fileContext.Password,
            fileContext.Salt,
            fileContext.EncryptedMetadata.ContentLength,
            ct);

        await StoreFileMetadataAsync(
            destinationBlobs,
            fileContext.FileKey,
            fileContext.EncryptedMetadata.ContentLength,
            md5Hash,
            ct);

        logger.LogInformation("Successfully processed file: {FileKey} ({SizeMB:F2} MB, MD5: {Md5Hash})",
            fileContext.FileKey,
            fileSize / (1024.0 * 1024.0),
            md5Hash);

        return new FileAcquisitionResult(md5Hash, fileSize);
    }

    private async Task StoreFileMetadataAsync(
        IBlobStorageService destinationBlobs,
        string fileKey,
        long sourceEncryptedLength,
        string md5Hash,
        CancellationToken ct)
    {
        var metadata = new Dictionary<string, string>
        {
            { "SourceEncryptedLength", sourceEncryptedLength.ToString() },
            { "MD5Hash", md5Hash }
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
            Md5Hash = acquisitionResult.Md5Hash,
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
                Md5Hash = string.Empty,
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
    /// Decrypts a stream and uploads it while calculating MD5 hash in a single pass.
    /// This streaming approach avoids loading the entire file into memory.
    /// </summary>
    /// <returns>A tuple containing the MD5 hash and the file size in bytes</returns>
    private async Task<(string md5Hash, long fileSize)> DecryptAndUploadWithMd5Async(
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

        // Wrap with MD5 calculation
        using var md5 = MD5.Create();
        await using var cryptoStream = new CryptoStream(byteCounter, md5, CryptoStreamMode.Write, leaveOpen: true);

        // Decrypt directly into the MD5+counting+upload stream pipeline
        // Pipeline: Decrypted data → CryptoStream (MD5) → ByteCountingStream (size) → Upload Stream (S3)
        await aesCryptoTransform.DecryptStreamAsync(
            encryptedStream,
            cryptoStream,
            password,
            salt,
            encryptedContentLength,
            null,
            ct);

        // Ensure all data is written and MD5 is finalized
        await cryptoStream.FlushFinalBlockAsync(ct);
        await byteCounter.FlushAsync(ct);

        // Get the computed hash and file size
        var hashBytes = md5.Hash ?? throw new InvalidOperationException("MD5 hash computation failed");
        var md5Hash = Convert.ToHexString(hashBytes).ToLowerInvariant();
        var fileSize = byteCounter.BytesWritten;

        return (md5Hash, fileSize);
    }

    // Helper records for internal state management
    private record FileContext(
        string FileKey,
        Stream EncryptedStream,
        StorageObjectMetadata EncryptedMetadata,
        string Password,
        string Salt) : IAsyncDisposable
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
        public string ExistingMd5Hash { get; init; } = string.Empty;

        public static FileTransferDecision TransferRequired() => new() { ShouldSkip = false };

        public static FileTransferDecision SkipTransfer(long fileSize, string md5Hash) => new()
        {
            ShouldSkip = true,
            ExistingFileSize = fileSize,
            ExistingMd5Hash = md5Hash
        };
    }

    private record FileAcquisitionResult(string Md5Hash, long FileSize);
}