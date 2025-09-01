using FluentAssertions;

namespace KeeperData.Bridge.Tests.Integration;

public class PlaceholderTest
{
    [Fact]
    public void PlaceholderTestShould()
    {
        var result = true;
        result.Should().BeTrue();
    }
}
