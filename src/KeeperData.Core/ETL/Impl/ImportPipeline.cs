using Amazon.Runtime.Internal.Util;
using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace KeeperData.Core.ETL.Impl;

public class ImportPipeline(IBlobStorageServiceFactory blobStorageServiceFactory,
    ISourceDataServiceFactory sourceDataServiceFactory,
    IAesCryptoTransform aesCryptoTransform,
    IPasswordSaltService passwordSalt,
    ILogger<ImportPipeline> logger) : IImportPipeline
{
    private const string MimeTypeTextCsv = "text/csv";

    public async Task StartAsync(Guid importId, string sourceType, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        logger.LogInformation("Starting import pipeline for ImportId: {ImportId}, SourceType: {SourceType}", importId, sourceType);

        try
        {
            var sourceBlobs = blobStorageServiceFactory.GetSource(sourceType);
            var sourceDataService = sourceDataServiceFactory.Create(sourceBlobs);
            var blobs = blobStorageServiceFactory.Get();

            logger.LogDebug("Initialized blob storage services for ImportId: {ImportId}", importId);

            // step 1: discover files that may need processing
            logger.LogInformation("Step 1: Discovering files for ImportId: {ImportId}", importId);
            var fileSets = await sourceDataService.GetFileSetsAsync(20 ,ct);
            logger.LogInformation("Discovered {FileSetCount} file set(s) containing {TotalFileCount} file(s) for ImportId: {ImportId}",
                fileSets.Count,
                fileSets.Sum(fs => fs.Files.Length),
                importId);

            // step 2: for each file, stream/decrypt into the target 
            logger.LogInformation("Step 2: Processing and decrypting files for ImportId: {ImportId}", importId);
            var processedFileCount = 0;
            var totalFiles = fileSets.Sum(fs => fs.Files.Length);

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
                        await using var decryptedUploadStream = await blobs.OpenWriteAsync(file.Key, MimeTypeTextCsv, cancellationToken: ct);
                        var encryptedMetadata = await sourceBlobs.GetMetadataAsync(file.Key, ct);
                        var cred = passwordSalt.Get(file.Key);

                        logger.LogDebug("Decrypting file: {FileKey}, ContentLength: {ContentLength} bytes for ImportId: {ImportId}",
                            file.Key,
                            encryptedMetadata.ContentLength,
                            importId);

                        await aesCryptoTransform.DecryptStreamAsync(encryptedStream,
                                                                    decryptedUploadStream,
                                                                    cred.Password,
                                                                    cred.Salt,
                                                                    encryptedMetadata.ContentLength,
                                                                    null,
                                                                    ct);

                        fileStopwatch.Stop();
                        logger.LogInformation("Successfully processed file: {FileKey} in {Duration}ms for ImportId: {ImportId}",
                            file.Key,
                            fileStopwatch.ElapsedMilliseconds,
                            importId);
                    }
                    catch (Exception ex)
                    {
                        fileStopwatch.Stop();
                        logger.LogError(ex, "Failed to process file: {FileKey} after {Duration}ms for ImportId: {ImportId}",
                            file.Key,
                            fileStopwatch.ElapsedMilliseconds,
                            importId);
                        throw;
                    }
                }
            }

            logger.LogInformation("Step 2 completed: Processed {ProcessedFileCount} file(s) for ImportId: {ImportId}",
                processedFileCount,
                importId);

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