using Xunit;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Collection definition to ensure LocalStack tests run serially.
/// </summary>
[CollectionDefinition("LocalStack")]
public class LocalStackCollection : ICollectionFixture<LocalStackFixture>
{
}