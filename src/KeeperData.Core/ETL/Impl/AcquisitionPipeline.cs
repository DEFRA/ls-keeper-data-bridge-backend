using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

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
            var sourceBlobs = blobStorageServiceFactory.GetSource(sourceType);
            var ExternalCatalogueService = ExternalCatalogueServiceFactory.Create(sourceBlobs);
            var blobs = blobStorageServiceFactory.Get();

            logger.LogDebug("Initialized blob storage services for ImportId: {ImportId}", importId);

            // step 1: discover files that may need processing
            logger.LogInformation("Step 1: Discovering files for ImportId: {ImportId}", importId);
            var fileSets = await ExternalCatalogueService.GetFileSetsAsync(20, ct);
            var totalFiles = fileSets.Sum(fs => fs.Files.Length);
            
            logger.LogInformation("Discovered {FileSetCount} file set(s) containing {TotalFileCount} file(s) for ImportId: {ImportId}",
                fileSets.Count,
                totalFiles,
                importId);

            // Update acquisition phase - started
            await reportingService.UpdateAcquisitionPhaseAsync(importId, new AcquisitionPhaseUpdate
            {
                Status = PhaseStatus.Started,
                FilesDiscovered = totalFiles,
                FilesProcessed = 0,
                FilesFailed = 0
            }, ct);

            // step 2: for each file, stream/decrypt into the target 
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
                    var fileStopwatch = Stopwatch.StartNew();
                    processedFileCount++;

                    logger.LogInformation("Processing file {CurrentFile}/{TotalFiles}: {FileKey} for ImportId: {ImportId}",
                        processedFileCount,
                        totalFiles,
                        file.Key,
                        importId);

                    try
                    {
                        await using var encryptedStream = await sourceBlobs.OpenReadAsync(file.Key, ct);
                        var encryptedMetadata = await sourceBlobs.GetMetadataAsync(file.Key, ct);
                        var cred = passwordSalt.Get(file.Key);

                        logger.LogDebug("Decrypting file: {FileKey}, ContentLength: {ContentLength} bytes for ImportId: {ImportId}",
                            file.Key,
                            encryptedMetadata.ContentLength,
                            importId);

                        // Decrypt to memory stream first to calculate MD5
                        using var decryptedStream = new MemoryStream();
                        await aesCryptoTransform.DecryptStreamAsync(encryptedStream,
                                                                    decryptedStream,
                                                                    cred.Password,
                                                                    cred.Salt,
                                                                    encryptedMetadata.ContentLength,
                                                                    null,
                                                                    ct);

                        // Calculate MD5 hash
                        decryptedStream.Position = 0;
                        var md5Hash = await Md5HashHelper.CalculateMd5Async(decryptedStream, ct);
                        
                        // Check if file was already processed
                        var isAlreadyProcessed = await reportingService.IsFileProcessedAsync(file.Key, md5Hash, ct);
                        
                        if (isAlreadyProcessed)
                        {
                            logger.LogInformation("File {FileKey} with MD5 {Md5Hash} was already processed, skipping for ImportId: {ImportId}",
                                file.Key,
                                md5Hash,
                                importId);
                            
                            fileStopwatch.Stop();
                            continue;
                        }

                        // Upload decrypted content
                        decryptedStream.Position = 0;
                        await using var uploadStream = await blobs.OpenWriteAsync(file.Key, MimeTypeTextCsv, cancellationToken: ct);
                        await decryptedStream.CopyToAsync(uploadStream, ct);

                        fileStopwatch.Stop();

                        // Record successful acquisition
                        await reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
                        {
                            FileName = Path.GetFileName(file.Key),
                            FileKey = file.Key,
                            DatasetName = fileSet.Definition.Name,
                            Md5Hash = md5Hash,
                            FileSize = decryptedStream.Length,
                            SourceKey = file.Key,
                            DecryptionDurationMs = fileStopwatch.ElapsedMilliseconds,
                            AcquiredAtUtc = DateTime.UtcNow,
                            Status = FileProcessingStatus.Acquired
                        }, ct);

                        logger.LogInformation("Successfully processed file: {FileKey} in {Duration}ms for ImportId: {ImportId}",
                            file.Key,
                            fileStopwatch.ElapsedMilliseconds,
                            importId);
                    }
                    catch (Exception ex)
                    {
                        fileStopwatch.Stop();
                        failedFileCount++;
                        
                        logger.LogError(ex, "Failed to process file: {FileKey} after {Duration}ms for ImportId: {ImportId}",
                            file.Key,
                            fileStopwatch.ElapsedMilliseconds,
                            importId);

                        // Record failed acquisition
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
                                DecryptionDurationMs = fileStopwatch.ElapsedMilliseconds,
                                AcquiredAtUtc = DateTime.UtcNow,
                                Status = FileProcessingStatus.Failed,
                                Error = ex.Message
                            }, ct);
                        }
                        catch (Exception reportEx)
                        {
                            logger.LogError(reportEx, "Failed to record acquisition failure for file: {FileKey}", file.Key);
                        }
                        
                        throw;
                    }
                }
            }

            logger.LogInformation("Step 2 completed: Processed {ProcessedFileCount} file(s) for ImportId: {ImportId}",
                processedFileCount,
                importId);

            // Update acquisition phase - completed
            await reportingService.UpdateAcquisitionPhaseAsync(importId, new AcquisitionPhaseUpdate
            {
                Status = failedFileCount > 0 ? PhaseStatus.Failed : PhaseStatus.Completed,
                FilesDiscovered = totalFiles,
                FilesProcessed = processedFileCount - failedFileCount,
                FilesFailed = failedFileCount,
                CompletedAtUtc = DateTime.UtcNow
            }, ct);

            // step 3: stream-read all the transferred files and stream into mongo
            logger.LogInformation("Step 3: Stream-read and import to database (TODO) for ImportId: {ImportId}", importId);
            // todo

            stopwatch.Stop();
            logger.LogInformation("Import pipeline completed successfully for ImportId: {ImportId}. Total duration: {Duration}ms ({DurationSeconds}s)",
                importId,
                stopwatch.ElapsedMilliseconds,
                stopwatch.Elapsed.TotalSeconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            logger.LogError(ex, "Import pipeline failed for ImportId: {ImportId} after {Duration}ms ({DurationSeconds}s)",
                importId,
                stopwatch.ElapsedMilliseconds,
                stopwatch.Elapsed.TotalSeconds);
            throw;
        }
    }
}