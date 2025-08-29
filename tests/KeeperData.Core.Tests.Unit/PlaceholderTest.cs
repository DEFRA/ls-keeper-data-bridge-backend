using FluentAssertions;

namespace KeeperData.Core.Tests.Unit;

public class PlaceholderTest
{
    [Fact]
    public void PlaceholderTestShould()
    {
        var result = true;
        result.Should().BeTrue();
    }
}
