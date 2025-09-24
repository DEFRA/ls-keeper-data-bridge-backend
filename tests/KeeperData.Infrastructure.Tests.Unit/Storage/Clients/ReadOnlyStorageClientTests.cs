using System.Net;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using AutoMapper;
using FluentAssertions;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.Clients;

public class ReadOnlyStorageClientTests
{
    private const string ValidKey = "validKey";
    private const string MissingKey = "missingKey";
    
    [Fact]
    public async Task ListAsync_WithData_ShouldReturnMappedList()
    {
        // Arrange
        var sutSetup = CreateSetup("TestClient", 2);
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.ListAsync("testContainer", "testPrefix", CancellationToken.None);
        
        // Assert
        result.Count.Should().Be(2);
    }
    
    [Fact]
    public async Task ListAsync_WithNoData_ShouldReturnEmptyList()
    {
        // Arrange
        var sutSetup = CreateSetup("TestClient");
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.ListAsync("testContainer", "testPrefix", CancellationToken.None);
        
        // Assert
        result.Count.Should().Be(0);
    }
    
    [Fact]
    public async Task ListPageAsync_WithData_ShouldReturnMappedListPage()
    {
        // Arrange
        var sutSetup = CreateSetup("TestClient", 2);
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.ListPageAsync("testContainer", "testPrefix", 100, "", CancellationToken.None);
        
        // Assert
        result.Items.Count.Should().Be(2);
    }
    
    [Fact]
    public async Task ListPageAsync_WithNoData_ShouldReturnEmptyListPage()
    {
        // Arrange
        var sutSetup = CreateSetup("TestClient");
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.ListPageAsync("testContainer", "testPrefix", 100, "", CancellationToken.None);
        
        // Assert
        result.Items.Count.Should().Be(0);
    }

    [Fact]
    public async Task GetMetadataAsync_WithData_ShouldReturnMappedMetadata()
    {
        // Arrange
        var clientNamePretendingToBeEtag = "testClient";
        var sutSetup = CreateSetup(clientNamePretendingToBeEtag);
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.GetMetadataAsync("testContainer", ValidKey, CancellationToken.None);
        
        // Assert
        result.ETag.Should().Be(clientNamePretendingToBeEtag);
    }

    [Fact]
    public async Task DownloadAsync_WithData_ShouldReturnBytes()
    {
        // Arrange
        var clientNamePretendingToBeEtag = "testClient";
        var sutSetup = CreateSetup(clientNamePretendingToBeEtag);
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.DownloadAsync("testContainer", ValidKey, CancellationToken.None);
        
        // Assert
        result.Length.Should().BeGreaterThan(0);
    }
    
    [Fact]
    public async Task OpenReadAsync_WithData_ShouldReturnStream()
    {
        // Arrange
        var clientNamePretendingToBeEtag = "testClient";
        var sutSetup = CreateSetup(clientNamePretendingToBeEtag);
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.OpenReadAsync("testContainer", ValidKey, CancellationToken.None);
        
        // Assert
        result.Length.Should().BeGreaterThan(0);
    }
    
    [Fact]
    public async Task Exists_WithValidKey_ShouldReturnTrue()
    {
        // Arrange
        var clientNamePretendingToBeEtag = "testClient";
        var sutSetup = CreateSetup(clientNamePretendingToBeEtag);
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.ExistsAsync("testContainer", ValidKey, CancellationToken.None);
        
        // Assert
        result.Should().Be(true);
    }
    
    [Fact]
    public async Task Exists_WithMissingKey_ShouldReturnTrue()
    {
        // Arrange
        var clientNamePretendingToBeEtag = "testClient";
        var sutSetup = CreateSetup(clientNamePretendingToBeEtag);
        var sut = new ReadOnlyStorageClient(sutSetup.ClientMock.Object, sutSetup.ClientName, sutSetup.Mapper);
        
        // Act
        var result = await sut.ExistsAsync("testContainer", MissingKey, CancellationToken.None);
        
        // Assert
        result.Should().Be(false);
    }
    
    private (Mock<IAmazonS3> ClientMock, string ClientName, IMapper Mapper) CreateSetup(string clientName, int numberOfListResults = 0)
    {
        var listResults = new System.Collections.Generic.List<S3Object>();
        for (int i = 0; i < numberOfListResults; i++)
        {
            listResults.Add(new S3Object());
        }

        var testString = "this is some text to be turned into bytes";
        var bytes = Encoding.ASCII.GetBytes(testString);
        var stream = new MemoryStream(bytes);
        
        var clientMock = new Mock<IAmazonS3>();
        clientMock.Setup(c => c.ListObjectsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsResponse
            {
                ContentLength = numberOfListResults,
                HttpStatusCode = (HttpStatusCode)200,
                S3Objects = listResults,
            });
        
        clientMock.Setup(c => c.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response
            {
                ContentLength = numberOfListResults,
                HttpStatusCode = (HttpStatusCode)200,
                S3Objects = listResults,
            });
        
        clientMock.Setup(c => c.GetObjectMetadataAsync(It.IsAny<string>(), ValidKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectMetadataResponse()
            {
                HttpStatusCode = (HttpStatusCode)200,
                ETag = clientName
            });
        
        clientMock.Setup(c => c.GetObjectMetadataAsync(It.IsAny<string>(), MissingKey, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("Error")
            {
                StatusCode = (HttpStatusCode)404,
            });
        
        clientMock.Setup(c => c.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectResponse()
            {
                HttpStatusCode = (HttpStatusCode)200,
                ResponseStream = stream
            });

        var config = new MapperConfiguration(cfg =>
            {
                cfg.CreateMap<GetObjectMetadataResponse, StorageObjectMetadata>();
                cfg.CreateMap<S3Object, StorageObjectInfo>();
            },
            new LoggerFactory());
        var mapper = config.CreateMapper();
        
        return (clientMock, clientName, mapper);
    }
}