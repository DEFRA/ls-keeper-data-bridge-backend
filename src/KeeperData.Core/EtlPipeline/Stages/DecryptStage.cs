using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Impl;
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
/// mirrors the source object key, so raw/ has the same layout as the source folder.</summary>
public sealed class DecryptStage(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IEtlPipelineStorageProvider etlPipelineStorageProvider,
    IAesCryptoTransform aesCryptoTransform,
    IPasswordSaltService passwordSaltService,
    ILogger<DecryptStage> logger) : MapStage<DiscoveredFileSet, RawFileSet>
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

        var rawKeys = new List<string>(input.Files.Count);

        foreach (var file in input.Files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var objectKey = file.StorageObject.Key;

            if (await rawBlobsStorageService.ExistsAsync(objectKey, cancellationToken))
            {
                logger.LogInformation(
                    "Skipping {ObjectKey} for dataset {DatasetName} - already present in {Folder} for RunId: {RunId}",
                    objectKey, input.Definition.Name, EtlPipelineFolders.Raw, etlContext.RunId);

                rawKeys.Add(objectKey);
                continue;
            }

            var decryptedLength = await EtlArtefactWrite.RunAsync(
                rawBlobsStorageService,
                objectKey,
                () => DecryptToRawAsync(
                    objectKey, input.Definition, sourceBlobsStorageService, rawBlobsStorageService, cancellationToken),
                logger);

            logger.LogInformation(
                "Decrypted {ObjectKey} for dataset {DatasetName} into {Folder} ({SizeMB:F2} MB) for RunId: {RunId}",
                objectKey, input.Definition.Name, EtlPipelineFolders.Raw,
                decryptedLength / (1024.0 * 1024.0), etlContext.RunId);

            rawKeys.Add(objectKey);
        }

        return new RawFileSet(input.Definition)
        {
            RunId = etlContext.RunId,
            Files = rawKeys
        };
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
