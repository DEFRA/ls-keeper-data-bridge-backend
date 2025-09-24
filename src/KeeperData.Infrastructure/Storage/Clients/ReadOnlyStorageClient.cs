using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using AutoMapper;

namespace KeeperData.Infrastructure.Storage.Clients;

public class ReadOnlyStorageClient : IReadOnlyStorageClient
{
    
    private const int DownloadBufferSize = 1024;
    protected IAmazonS3 _s3Client;
    private readonly IMapper _mapper;
    private string sourceBucket = "";
    
    public ReadOnlyStorageClient()
    {
        
    }
    
    public ReadOnlyStorageClient(IAmazonS3 s3Client, string clientName, IMapper mapper)
    {
        _s3Client = s3Client;
        _mapper = mapper;
    }

    public string ClientName { get; } = "";

    public async Task<IReadOnlyList<StorageObjectInfo>> ListAsync(string container, string? prefix = null, CancellationToken cancellationToken = default)
    {
        var response = await _s3Client.ListObjectsAsync(container, prefix, cancellationToken);
        
        if ( response.S3Objects.Count == 0)
        {
            return Array.Empty<StorageObjectInfo>();
        }
        
        return _mapper.Map<List<StorageObjectInfo>>(response.S3Objects);
    }

    public async Task<StorageListPage> ListPageAsync(string container, string? prefix = null, int pageSize = 1000, string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        var request = new ListObjectsV2Request()
        {
            BucketName = container,
            Prefix = prefix,
            
        };
        
        var response = await _s3Client.ListObjectsV2Async(request, cancellationToken);

        return new StorageListPage()
        {
            Items = _mapper.Map<List<StorageObjectInfo>>(response.S3Objects),
            ContinuationToken = response.ContinuationToken,
            IsTruncated = response.IsTruncated,
        };
    }

    public async Task<StorageObjectMetadata> GetMetadataAsync(
        string container,
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        var response = await _s3Client.GetObjectMetadataAsync(container, objectKey, cancellationToken);
        
        return _mapper.Map<StorageObjectMetadata>(response);
    }
    
    public async Task<byte[]> DownloadAsync(
        string container,
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        var objectStream = await OpenReadStreamAsync(container, objectKey, cancellationToken);
        
        var byteOutput = new byte[objectStream.Length];
	
        int readBytes = 0;
        int chunkSize = DownloadBufferSize;
	
        while(readBytes < objectStream.Length)
        {
            long remaining = objectStream.Length - readBytes;
            int thisChunk = (int)long.Min(chunkSize, remaining);
		
            var readResult = await objectStream.ReadAsync(byteOutput, readBytes, thisChunk, cancellationToken);
		
            readBytes += readResult;
        }
        
        return byteOutput;
    }
    
    public async Task<Stream> OpenReadAsync(string container, string objectKey, CancellationToken cancellationToken = default)
    {
        return await OpenReadStreamAsync(container, objectKey, cancellationToken);
    }

    public async Task<bool> ExistsAsync(string container, string objectKey, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _s3Client.GetObjectMetadataAsync(container, objectKey, cancellationToken);
            return response.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }
        catch (AmazonS3Exception s3Ex)
        {
            if (s3Ex.StatusCode == HttpStatusCode.NotFound) return false;
            throw;
        }
    }

    private async Task<Stream> OpenReadStreamAsync(string container, string objectKey, CancellationToken cancellationToken = default)
    {
        var getObjectRequest = new Amazon.S3.Model.GetObjectRequest()
        {
            BucketName = sourceBucket,
            Key = objectKey
        };
        
        var response = await _s3Client.GetObjectAsync(getObjectRequest, cancellationToken);
        
        return response.ResponseStream;
    }
}