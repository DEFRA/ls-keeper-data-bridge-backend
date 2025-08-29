using FluentAssertions;

namespace KeeperData.Infrastructure.Tests.Unit;

public class PlaceholderTest
{
    [Fact]
    public void PlaceholderTestShould()
    {
        var result = true;
        result.Should().BeTrue();
    }
}
