using FluentAssertions;

namespace KeeperData.Bridge.Tests.Component;

public class PlaceholderTest
{
    [Fact]
    public void PlaceholderTestShould()
    {
        var result = true;
        result.Should().BeTrue();
    }
}
