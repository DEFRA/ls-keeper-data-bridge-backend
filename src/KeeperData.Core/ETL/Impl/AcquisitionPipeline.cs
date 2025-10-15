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

    public async Task StartAsync(Guid importId, string sourceType, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        logger.LogInformation("Starting import pipeline for ImportId: {ImportId}, SourceType: {SourceType}", importId, sourceType);

        try
        {
            var storageServices = InitializeStorageServices(importId, sourceType);
            
            var fileSets = await DiscoverFilesAsync(importId, storageServices.ExternalCatalogueService, ct);
            
            await UpdateAcquisitionPhaseStartedAsync(importId, fileSets.TotalFiles, ct);
            
            var processingResults = await ProcessAllFilesAsync(
                importId, 
                fileSets.FileSets, 
                fileSets.TotalFiles,
                storageServices.SourceBlobs, 
                storageServices.DestinationBlobs, 
                ct);
            
            await UpdateAcquisitionPhaseCompletedAsync(
                importId, 
                fileSets.TotalFiles, 
                processingResults.ProcessedCount, 
                processingResults.FailedCount, 
                ct);

            LogPipelineCompletion(importId, stopwatch);
        }
        catch (Exception ex)
        {
            LogPipelineFailure(importId, stopwatch, ex);
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
        
        var fileSets = await catalogueService.GetFileSetsAsync(20, ct);
        var totalFiles = fileSets.Sum(fs => fs.Files.Length);
        
        logger.LogInformation("Discovered {FileSetCount} file set(s) containing {TotalFileCount} file(s) for ImportId: {ImportId}",
            fileSets.Count,
            totalFiles,
            importId);

        return (fileSets, totalFiles);
    }

    private async Task UpdateAcquisitionPhaseStartedAsync(Guid importId, int totalFiles, CancellationToken ct)
    {
        await reportingService.UpdateAcquisitionPhaseAsync(importId, new AcquisitionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesDiscovered = totalFiles,
            FilesProcessed = 0,
            FilesFailed = 0
        }, ct);
    }

    private async Task<(int ProcessedCount, int FailedCount)> ProcessAllFilesAsync(
        Guid importId,
        ImmutableList<FileSet> fileSets,
        int totalFiles,
        IBlobStorageServiceReadOnly sourceBlobs,
        IBlobStorageService destinationBlobs,
        CancellationToken ct)
    {
        logger.LogInformation("Step 2: Processing and decrypting files for ImportId: {ImportId}", importId);
        
        var processedFileCount = 0;
        var failedFileCount = 0;

        foreach (var fileSet in fileSets)
        {
            logger.LogDebug("Processing file set for definition: {DefinitionName} with {FileCount} file(s) for ImportId: {ImportId}",
                fileSet.Definition.Name,
                fileSet.Files.Length,
                importId);

            foreach (var file in fileSet.Files)
            {
                processedFileCount++;
                
                var result = await ProcessSingleFileAsync(
                    importId,
                    fileSet,
                    file,
                    processedFileCount,
                    totalFiles,
                    sourceBlobs,
                    destinationBlobs,
                    ct);

                if (!result)
                {
                    failedFileCount++;
                }
            }
        }

        logger.LogInformation("Step 2 completed: Processed {ProcessedFileCount} file(s) for ImportId: {ImportId}",
            processedFileCount,
            importId);

        return (processedFileCount, failedFileCount);
    }

    private async Task<bool> ProcessSingleFileAsync(
        Guid importId,
        FileSet fileSet,
        StorageObjectInfo file,
        int currentFileNumber,
        int totalFiles,
        IBlobStorageServiceReadOnly sourceBlobs,
        IBlobStorageService destinationBlobs,
        CancellationToken ct)
    {
        var fileStopwatch = Stopwatch.StartNew();

        logger.LogInformation("Processing file {CurrentFile}/{TotalFiles}: {FileKey} for ImportId: {ImportId}",
            currentFileNumber,
            totalFiles,
            file.Key,
            importId);

        try
        {
            var fileContext = await PrepareFileContextAsync(file, sourceBlobs, destinationBlobs, ct);
            
            var transferDecision = await DetermineFileTransferRequirementAsync(
                file.Key, 
                fileContext.EncryptedMetadata.ContentLength, 
                destinationBlobs, 
                importId, 
                ct);

            var acquisitionResult = await AcquireFileAsync(
                fileContext, 
                transferDecision, 
                destinationBlobs, 
                ct);

            fileStopwatch.Stop();

            await CheckForDuplicateProcessingAsync(file.Key, acquisitionResult.Md5Hash, importId, ct);

            await RecordSuccessfulAcquisitionAsync(
                importId, 
                fileSet, 
                file, 
                acquisitionResult, 
                fileStopwatch.ElapsedMilliseconds, 
                ct);

            return true;
        }
        catch (Exception ex)
        {
            fileStopwatch.Stop();
            logger.LogError(ex, "Failed to process file: {FileKey} after {Duration}ms for ImportId: {ImportId}",
                file.Key,
                fileStopwatch.ElapsedMilliseconds,
                importId);

            await RecordFailedAcquisitionAsync(importId, fileSet, file, fileStopwatch.ElapsedMilliseconds, ex, ct);
            
            throw;
        }
    }

    private async Task<FileContext> PrepareFileContextAsync(
        StorageObjectInfo file,
        IBlobStorageServiceReadOnly sourceBlobs,
        IBlobStorageService destinationBlobs,
        CancellationToken ct)
    {
        var encryptedStream = await sourceBlobs.OpenReadAsync(file.Key, ct);
        var encryptedMetadata = await sourceBlobs.GetMetadataAsync(file.Key, ct);
        var credentials = passwordSalt.Get(file.Key);

        logger.LogDebug("Loaded file context: {FileKey}, ContentLength: {ContentLength} bytes",
            file.Key,
            encryptedMetadata.ContentLength);

        return new FileContext(
            file.Key,
            encryptedStream,
            encryptedMetadata,
            credentials.Password,
            credentials.Salt);
    }

    private async Task<FileTransferDecision> DetermineFileTransferRequirementAsync(
        string fileKey,
        long sourceEncryptedLength,
        IBlobStorageService destinationBlobs,
        Guid importId,
        CancellationToken ct)
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

    private async Task<FileAcquisitionResult> AcquireFileAsync(
        FileContext fileContext,
        FileTransferDecision transferDecision,
        IBlobStorageService destinationBlobs,
        CancellationToken ct)
    {
        if (transferDecision.ShouldSkip)
        {
            return new FileAcquisitionResult(
                transferDecision.ExistingMd5Hash,
                transferDecision.ExistingFileSize);
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

    private async Task CheckForDuplicateProcessingAsync(
        string fileKey,
        string md5Hash,
        Guid importId,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(md5Hash))
        {
            return;
        }

        var isAlreadyProcessed = await reportingService.IsFileProcessedAsync(fileKey, md5Hash, ct);
        
        if (isAlreadyProcessed)
        {
            logger.LogWarning("File {FileKey} with MD5 {Md5Hash} was already processed in a previous import for ImportId: {ImportId}",
                fileKey,
                md5Hash,
                importId);
        }
    }

    private async Task RecordSuccessfulAcquisitionAsync(
        Guid importId,
        FileSet fileSet,
        StorageObjectInfo file,
        FileAcquisitionResult acquisitionResult,
        long durationMs,
        CancellationToken ct)
    {
        await reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
        {
            FileName = Path.GetFileName(file.Key),
            FileKey = file.Key,
            DatasetName = fileSet.Definition.Name,
            Md5Hash = acquisitionResult.Md5Hash,
            FileSize = acquisitionResult.FileSize,
            SourceKey = file.Key,
            DecryptionDurationMs = durationMs,
            AcquiredAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Acquired
        }, ct);
    }

    private async Task RecordFailedAcquisitionAsync(
        Guid importId,
        FileSet fileSet,
        StorageObjectInfo file,
        long durationMs,
        Exception ex,
        CancellationToken ct)
    {
        try
        {
            await reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
            {
                FileName = Path.GetFileName(file.Key),
                FileKey = file.Key,
                DatasetName = fileSet.Definition.Name,
                Md5Hash = string.Empty,
                FileSize = 0,
                SourceKey = file.Key,
                DecryptionDurationMs = durationMs,
                AcquiredAtUtc = DateTime.UtcNow,
                Status = FileProcessingStatus.Failed,
                Error = ex.Message
            }, ct);
        }
        catch (Exception reportEx)
        {
            logger.LogError(reportEx, "Failed to record acquisition failure for file: {FileKey}", file.Key);
        }
    }

    private async Task UpdateAcquisitionPhaseCompletedAsync(
        Guid importId,
        int totalFiles,
        int processedCount,
        int failedCount,
        CancellationToken ct)
    {
        await reportingService.UpdateAcquisitionPhaseAsync(importId, new AcquisitionPhaseUpdate
        {
            Status = failedCount > 0 ? PhaseStatus.Failed : PhaseStatus.Completed,
            FilesDiscovered = totalFiles,
            FilesProcessed = processedCount - failedCount,
            FilesFailed = failedCount,
            CompletedAtUtc = DateTime.UtcNow
        }, ct);
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