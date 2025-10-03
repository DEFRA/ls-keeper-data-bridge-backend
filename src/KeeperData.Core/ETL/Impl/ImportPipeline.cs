using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Impl;

public class ImportPipeline(IBlobStorageServiceFactory blobStorageServiceFactory,
    ISourceDataServiceFactory sourceDataServiceFactory,
    IAesCryptoTransform aesCryptoTransform,
    IPasswordSaltService passwordSalt) : IImportPipeline
{
    private const string MimeTypeTextCsv = "text/csv";

    public async Task StartAsync(string sourceType, CancellationToken ct)
    {
        var sourceBlobs = blobStorageServiceFactory.GetSource(sourceType);
        var sourceDataService = sourceDataServiceFactory.Create(sourceBlobs);
        var blobs = blobStorageServiceFactory.Get();

        // step 1: discover files that may need processing
        var fileSets = await sourceDataService.GetFileSetsAsync(ct);

        // step 2: for each file, stream/decrypt into the target 
        foreach (var fileSet in fileSets)
        {
            foreach (var file in fileSet.Files)
            {
                await using var encryptedStream = await sourceBlobs.OpenReadAsync(file.Key, ct);
                await using var decryptedUploadStream = await blobs.OpenWriteAsync(file.Key, MimeTypeTextCsv, cancellationToken: ct);
                var encryptedMetadata = await sourceBlobs.GetMetadataAsync(file.Key, ct);
                var cred = passwordSalt.Get(file.Key);

                await aesCryptoTransform.DecryptStreamAsync(encryptedStream,
                                                            decryptedUploadStream,
                                                            cred.Password,
                                                            cred.Salt,
                                                            encryptedMetadata.ContentLength,
                                                            null,
                                                            ct);
            }
        }

        // step 3: stream-read all the transferred files and stream into mongo
        // todo

    }

}