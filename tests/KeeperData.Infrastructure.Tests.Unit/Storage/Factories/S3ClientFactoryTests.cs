using Amazon.S3;
using FluentAssertions;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories.Implementations;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.Factories;

public class S3ClientFactoryTests
{
    private readonly S3ClientFactory _clientFactory;
    private readonly AmazonS3Config _defaultAmazonS3Config = new() { RegionEndpoint = Amazon.RegionEndpoint.EUWest2 };

    private const string DefaultBucketName = "test-bucket";

    public S3ClientFactoryTests() => _clientFactory = new S3ClientFactory();

    [Fact]
    public void GivenClientIsNotRegistered_WhenCallingGetClient_ShouldThrow()
    {
        Action act = () => _clientFactory.GetClient<TestStorageClient>();

        act.Should().Throw<KeyNotFoundException>();
    }

    [Fact]
    public void GivenClientIsRegistered_WhenCallingGetClient_ShouldReturnRegisteredClient()
    {
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);

        var result = _clientFactory.GetClient<TestStorageClient>();

        result.Should().NotBeNull().And.BeAssignableTo<IAmazonS3>();
    }

    [Fact]
    public void GivenClientIsNotRegistered_WhenCallingGetClientByName_ShouldThrow()
    {
        Action act = () => _clientFactory.GetClient(typeof(TestStorageClient).Name);

        act.Should().Throw<KeyNotFoundException>();
    }

    [Fact]
    public void GivenClientIsRegistered_WhenCallingGetClientByName_ShouldReturnRegisteredClient()
    {
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);

        var result = _clientFactory.GetClient(typeof(TestStorageClient).Name);

        result.Should().NotBeNull().And.BeAssignableTo<IAmazonS3>();
    }

    [Fact]
    public void GivenClientIsNotRegistered_WhenCallingGetClientBucketName_ShouldThrow()
    {
        Action act = () => _clientFactory.GetClientBucketName<TestStorageClient>();

        act.Should().Throw<KeyNotFoundException>();
    }

    [Fact]
    public void GivenClientIsRegistered_WhenCallingGetClientBucketName_ShouldReturnRegisteredClient()
    {
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);

        var result = _clientFactory.GetClientBucketName<TestStorageClient>();

        result.Should().NotBeNull().And.Be(DefaultBucketName);
    }

    [Fact]
    public void GivenClientIsNotRegistered_WhenCallingGetClientBucketNameByName_ShouldThrow()
    {
        Action act = () => _clientFactory.GetClientBucketName(typeof(TestStorageClient).Name);

        act.Should().Throw<KeyNotFoundException>();
    }

    [Fact]
    public void GivenClientIsRegistered_WhenCallingGetClientBucketNameByName_ShouldReturnRegisteredClient()
    {
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);

        var result = _clientFactory.GetClientBucketName(typeof(TestStorageClient).Name);

        result.Should().NotBeNull().And.Be(DefaultBucketName);
    }

    [Fact]
    public void GivenNoClientsAreRegistered_WhenCallingGetRegisteredClientNames_ShouldReturnEmptyList()
    {
        var result = _clientFactory.GetRegisteredClientNames();

        result.Should().NotBeNull();
        result.Count().Should().Be(0);
    }

    [Fact]
    public void GivenClientsAreRegistered_WhenCallingGetRegisteredClientNames_ShouldReturnPopulatedList()
    {
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);
        _clientFactory.AddClient<ExternalStorageClient>(DefaultBucketName, _defaultAmazonS3Config);

        var result = _clientFactory.GetRegisteredClientNames();

        result.Should().NotBeNull();
        result.Count().Should().Be(2);
    }

    [Theory]
    [InlineData("TestStorageClient", true)]
    [InlineData("ExternalStorageClient", false)]
    public void GivenInitialisedClients_WhenCallingHasStorageClient_ShouldReturnExpected(string testClientName, bool expectedResult)
    {
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);

        var result = _clientFactory.HasStorageClient(testClientName);

        result.Should().Be(expectedResult);
    }

    [Fact]
    public void WhenCallingAddClient_ShouldRegisterClient()
    {
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);

        _clientFactory.HasStorageClient("TestStorageClient").Should().Be(true);
        _clientFactory.GetRegisteredClientNames().Count().Should().Be(1);
        _clientFactory.GetRegisteredClientNames().Should().Contain("TestStorageClient");
    }

    [Fact]
    public void GivenClientAlreadyRegistered_WhenCallingAddClient_ShouldNotDuplicate()
    {
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);
        _clientFactory.AddClient<TestStorageClient>(DefaultBucketName, _defaultAmazonS3Config);

        _clientFactory.HasStorageClient("TestStorageClient").Should().Be(true);
        _clientFactory.GetRegisteredClientNames().Count().Should().Be(1);
        _clientFactory.GetRegisteredClientNames().Should().Contain("TestStorageClient");
    }

    [Fact]
    public void WhenCallingAddClientWithCredentials_ShouldRegisterClient()
    {
        Environment.SetEnvironmentVariable("TEST_ACCESS_KEY", "access");
        Environment.SetEnvironmentVariable("TEST_SECRET_KEY", "secret");

        _clientFactory.AddClientWithCredentials<TestStorageClient>(DefaultBucketName, "TEST_ACCESS_KEY", "TEST_SECRET_KEY", _defaultAmazonS3Config);

        _clientFactory.HasStorageClient("TestStorageClient").Should().Be(true);
        _clientFactory.GetRegisteredClientNames().Count().Should().Be(1);
        _clientFactory.GetRegisteredClientNames().Should().Contain("TestStorageClient");
    }

    [Fact]
    public void GivenClientAlreadyRegistered_WhenCallingAddClientWithCredentials_ShouldNotDuplicate()
    {
        Environment.SetEnvironmentVariable("TEST_ACCESS_KEY", "access");
        Environment.SetEnvironmentVariable("TEST_SECRET_KEY", "secret");

        _clientFactory.AddClientWithCredentials<TestStorageClient>(DefaultBucketName, "TEST_ACCESS_KEY", "TEST_SECRET_KEY", _defaultAmazonS3Config);
        _clientFactory.AddClientWithCredentials<TestStorageClient>(DefaultBucketName, "TEST_ACCESS_KEY", "TEST_SECRET_KEY", _defaultAmazonS3Config);

        _clientFactory.HasStorageClient("TestStorageClient").Should().Be(true);
        _clientFactory.GetRegisteredClientNames().Count().Should().Be(1);
        _clientFactory.GetRegisteredClientNames().Should().Contain("TestStorageClient");
    }

    [Fact]
    public void GivenCredentialsAreNotSet_WhenCallingAddClientWithCredentials_ShouldThrow()
    {
        Action act = () => _clientFactory.AddClientWithCredentials<TestStorageClient>(DefaultBucketName, "TEST_ACCESS_KEY", "TEST_SECRET_KEY", _defaultAmazonS3Config);

        act.Should().Throw<InvalidOperationException>();

        _clientFactory.GetRegisteredClientNames().Should().BeEmpty();
    }
}

public class TestStorageClient : IStorageClient
{
    public string ClientName => "TestStorageClient";
}