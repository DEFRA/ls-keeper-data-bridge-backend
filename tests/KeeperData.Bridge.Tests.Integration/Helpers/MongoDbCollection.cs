using Xunit;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Collection definition to ensure MongoDB tests run serially and share the container.
/// </summary>
[CollectionDefinition("MongoDB")]
public class MongoDbCollection : ICollectionFixture<MongoDbFixture>
{
}
