using FluentAssertions;
using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Core.Tests.Unit.Storage.KeyRotation;

public class KeyIdMaskTests
{
    [Theory]
    [InlineData("AKIA1234567890ABCDEF", "AKI...DEF")]
    [InlineData("ABCDEFG", "ABC...EFG")]
    public void Mask_WithLongKeyId_ShowsFirstThreeAndLastThree(string keyId, string expected)
    {
        // Act
        var result = KeyIdMask.Mask(keyId);

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("ABCDEF", "******")]
    [InlineData("ABC", "***")]
    [InlineData("A", "*")]
    public void Mask_WithShortKeyId_FullyMasks(string keyId, string expected)
    {
        // Act
        var result = KeyIdMask.Mask(keyId);

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Mask_WithMissingKeyId_ReturnsEmpty(string? keyId)
    {
        // Act
        var result = KeyIdMask.Mask(keyId);

        // Assert
        result.Should().BeEmpty();
    }
}
