namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Collection definition for Phase II tests that need LocalStack S3 with a DuckDB stub uploaded.
/// </summary>
[CollectionDefinition("LocalStackAndDuckDb")]
public class DuckDbStubCollection : ICollectionFixture<LocalStackFixture>, ICollectionFixture<DuckDbStubFixture>
{
}
