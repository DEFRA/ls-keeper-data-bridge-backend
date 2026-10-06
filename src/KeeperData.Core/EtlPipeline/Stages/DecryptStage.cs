using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Concurrency;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;
using System.Security.Cryptography;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Decrypts a dataset's files into raw/. Materialises: raw/. Idempotent: a file already
/// present in raw/ is skipped and never overwritten.
///
/// Each file is streamed source -> decrypt -> raw, so no file is held in memory. The raw object key
/// mirrors the source object key, so raw/ has the same layout as the source folder.
///
/// Files are decrypted concurrently against the I/O budget: the work is mostly spent waiting on the
/// source storage, so more of them fit than there are cores to run them on.</summary>
public sealed class DecryptStage(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IEtlPipelineStorageProvider etlPipelineStorageProvider,
    IAesCryptoTransform aesCryptoTransform,
    IPasswordSaltService passwordSaltService,
    EtlConcurrency concurrency,
    ILogger<DecryptStage> logger) : ParallelMapStage<DiscoveredFileSet, RawFileSet>(concurrency)
{
    private const string MimeTypeTextCsv = "text/csv";

    public override string Name => "decrypt";

    protected override async Task<RawFileSet> MapAsync(
        DiscoveredFileSet input,
        IPipelineContext context,
        CancellationToken cancellationToken)
    {
        var etlContext = (EtlPipelineContext)context;
        
        var sourceBlobsStorageService = blobStorageServiceFactory.GetSource(etlContext.SourceType);
        var rawBlobsStorageService = etlPipelineStorageProvider.ForFolder(EtlPipelineFolders.Raw);

        var rawKeys = await Concurrency.ForEachAsync(
            input.Files,
            EtlWorkload.Io,
            (file, token) => DecryptOneAsync(
                file.StorageObject.Key,
                input.Definition,
                etlContext,
                sourceBlobsStorageService,
                rawBlobsStorageService,
                token),
            cancellationToken);

        return new RawFileSet(input.Definition)
        {
            RunId = etlContext.RunId,
            Files = rawKeys
        };
    }

    /// <summary>Decrypts one file into raw/, or leaves the one already there alone. Returns the raw
    /// key either way.</summary>
    private async Task<string> DecryptOneAsync(
        string objectKey,
        DataSetDefinition definition,
        EtlPipelineContext etlContext,
        IBlobStorageServiceReadOnly sourceBlobsStorageService,
        IBlobStorageService rawBlobsStorageService,
        CancellationToken cancellationToken)
    {
        if (await rawBlobsStorageService.ExistsAsync(objectKey, cancellationToken))
        {
            logger.LogInformation(
                "Skipping {ObjectKey} for dataset {DatasetName} - already present in {Folder} for RunId: {RunId}",
                objectKey, definition.Name, EtlPipelineFolders.Raw, etlContext.RunId);

            return objectKey;
        }

        var decryptedLength = await EtlArtefactWrite.RunAsync(
            rawBlobsStorageService,
            objectKey,
            () => DecryptToRawAsync(
                objectKey, definition, sourceBlobsStorageService, rawBlobsStorageService, cancellationToken),
            logger);

        logger.LogInformation(
            "Decrypted {ObjectKey} for dataset {DatasetName} into {Folder} ({SizeMB:F2} MB) for RunId: {RunId}",
            objectKey, definition.Name, EtlPipelineFolders.Raw,
            decryptedLength / (1024.0 * 1024.0), etlContext.RunId);

        return objectKey;
    }

    /// <summary>Streams one encrypted source object through decryption and into raw/.
    /// Nothing is buffered: the decrypted bytes go straight to the upload stream.</summary>
    private async Task<long> DecryptToRawAsync(
        string objectKey,
        DataSetDefinition definition,
        IBlobStorageServiceReadOnly sourceBlobs,
        IBlobStorageService rawBlobs,
        CancellationToken cancellationToken)
    {
        var credentials = passwordSaltService.Get(objectKey, definition.PasswordDerivation);
        var sourceMetadata = await sourceBlobs.GetMetadataAsync(objectKey, cancellationToken);

        await using var encryptedStream = await sourceBlobs.OpenReadAsync(objectKey, cancellationToken);
        await using var uploadStream = await rawBlobs.OpenWriteAsync(objectKey, MimeTypeTextCsv, cancellationToken: cancellationToken);
        await using var byteCounter = new ByteCountingStream(uploadStream);

        try
        {
            await aesCryptoTransform.DecryptStreamAsync(
                encryptedStream,
                byteCounter,
                credentials.Password,
                credentials.Salt,
                sourceMetadata.ContentLength,
                null,
                cancellationToken);
        }
        catch (CryptographicException exception)
        {
            // A padding error is what a wrong key looks like, and the key is derived from the
            // filename and the salt alone. Say so, rather than reporting the padding.
            throw new SourceFileDecryptionException(objectKey, definition.Name, exception);
        }

        await byteCounter.FlushAsync(cancellationToken);

        return byteCounter.BytesWritten;
    }
}
