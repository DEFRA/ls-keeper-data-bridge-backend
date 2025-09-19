using FluentAssertions;
using KeeperData.Core.Messaging.Extensions;

namespace KeeperData.Core.Tests.Unit.Messaging.Extensions;

public class MessageAttributesTests
{
    [Theory]
    [InlineData("PlaceholderMessage", "Placeholder")]
    [InlineData("UserCreatedMessage", "UserCreated")]
    [InlineData("Message", "")]
    [InlineData("NoSuffix", "NoSuffix")]
    [InlineData(null, "")]
    public void ReplaceSuffix_ShouldStripMessageSuffix(string? input, string expected)
    {
        var result = input.ReplaceSuffix();
        result.Should().Be(expected);
    }
}