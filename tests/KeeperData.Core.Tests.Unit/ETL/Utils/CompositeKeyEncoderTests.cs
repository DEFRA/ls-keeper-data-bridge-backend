using FluentAssertions;
using KeeperData.Core.ETL.Utils;

namespace KeeperData.Core.Tests.Unit.ETL.Utils;

public class RecordIdGeneratorTests
{
    private readonly RecordIdGenerator _generator = new();

    [Theory]
    [InlineData("CPH001")]
    [InlineData("ABC")]
    [InlineData("123")]
    public void GenerateId_WithSingleSimpleKeyPart_ShouldReturnConsistentHash(string input)
    {
        // Act
        var result1 = _generator.GenerateId(input);
        var result2 = _generator.GenerateId(input);

        // Assert
        result1.Should().Be(result2, "Same input should produce same hash");
        result1.Should().HaveLength(43, "SHA256 Base64URL hash should be 43 characters");
        result1.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$", "Hash should be URL-safe");
    }

    [Fact]
    public void GenerateId_WithMultipleSimpleKeyParts_ShouldReturnConsistentHash()
    {
        // Arrange
        var parts = new[] { "NORTH", "F001" };

        // Act
        var result1 = _generator.GenerateId(parts);
        var result2 = _generator.GenerateId(parts);

        // Assert
        result1.Should().Be(result2, "Same input should produce same hash");
        result1.Should().HaveLength(43);
        result1.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$");
    }

    [Theory]
    [InlineData("CPH/001", "Special slash")]
    [InlineData("CPH&001", "Ampersand")]
    [InlineData("CPH?001", "Question mark")]
    [InlineData("CPH#001", "Hash")]
    [InlineData("CPH%001", "Percent")]
    [InlineData("CPH+001", "Plus")]
    [InlineData("CPH=001", "Equals")]
    [InlineData("CPH 001", "Space")]
    [InlineData("CPH@001", "At symbol")]
    public void GenerateId_WithUrlUnsafeCharacters_ShouldCreateUrlSafeHash(string input, string description)
    {
        // Act
        var hash = _generator.GenerateId(input);

        // Assert
        hash.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$", because: $"Hash should be URL-safe for {description}");
        hash.Should().NotContain("/");
        hash.Should().NotContain("?", because: "question mark is not URL-safe");
        hash.Should().NotContain("#", because: "hash is not URL-safe");
        hash.Should().NotContain("&", because: "ampersand is not URL-safe");
        hash.Should().NotContain(" ", because: "space is not URL-safe");
        hash.Should().NotContain("+", because: "plus is converted to - in Base64URL");
        hash.Should().NotContain("=", because: "padding is removed in Base64URL");
        hash.Should().HaveLength(43);
    }

    [Fact]
    public void GenerateId_WithUnicodeCharacters_ShouldProduceConsistentHash()
    {
        // Arrange
        var parts = new[] { "Café", "日本", "مرحبا", "Здравствуйте" };

        // Act
        var hash1 = _generator.GenerateId(parts);
        var hash2 = _generator.GenerateId(parts);

        // Assert
        hash1.Should().Be(hash2, "Unicode strings should hash consistently");
        hash1.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$");
    }

    [Fact]
    public void GenerateId_WithCompositeKeyDelimiterInValue_ShouldHandleCorrectly()
    {
        // Arrange - values containing the delimiter should still hash correctly
        var parts = new[] { "Key@@With@@Delimiters", "Another@@Part" };

        // Act
        var hash = _generator.GenerateId(parts);

        // Assert
        hash.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$");
        hash.Should().HaveLength(43);
    }

    [Fact]
    public void GenerateId_WithEmptyString_ShouldThrowArgumentException()
    {
        // Act
        var act = () => _generator.GenerateId("");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*null or empty*");
    }

    [Fact]
    public void GenerateId_WithNullKeyPart_ShouldThrowArgumentException()
    {
        // Act
        var act = () => _generator.GenerateId(new[] { "Valid", null!, "AlsoValid" });

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*null or empty*");
    }

    [Fact]
    public void GenerateId_WithNullArray_ShouldThrowArgumentNullException()
    {
        // Act
        var act = () => _generator.GenerateId((string[])null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void GenerateId_WithEmptyArray_ShouldThrowArgumentException()
    {
        // Act
        var act = () => _generator.GenerateId(Array.Empty<string>());

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*At least one key part*");
    }

    [Fact]
    public void GenerateId_WithEnumerable_ShouldWorkCorrectly()
    {
        // Arrange
        IEnumerable<string> parts = new List<string> { "Part1", "Part2", "Part3" };

        // Act
        var hash = _generator.GenerateId(parts);

        // Assert
        hash.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$");
        hash.Should().HaveLength(43);
    }

    [Theory]
    [InlineData("NORTH", "F001")]
    [InlineData("A", "B", "C", "D", "E")]
    [InlineData("CPH/001", "FARM&NAME", "SPECIAL?CHARS")]
    [InlineData("Key with spaces", "Another key")]
    public void GenerateId_WithVariousInputs_ShouldBeConsistent(params string[] parts)
    {
        // Act
        var hash1 = _generator.GenerateId(parts);
        var hash2 = _generator.GenerateId(parts);

        // Assert
        hash1.Should().Be(hash2, "Same input should always produce same hash");
    }

    [Fact]
    public void GenerateId_WithDifferentInputs_ShouldProduceDifferentHashes()
    {
        // Arrange
        var parts1 = new[] { "NORTH", "F001" };
        var parts2 = new[] { "SOUTH", "F001" };
        var parts3 = new[] { "NORTH", "F002" };

        // Act
        var hash1 = _generator.GenerateId(parts1);
        var hash2 = _generator.GenerateId(parts2);
        var hash3 = _generator.GenerateId(parts3);

        // Assert
        hash1.Should().NotBe(hash2);
        hash1.Should().NotBe(hash3);
        hash2.Should().NotBe(hash3);
    }

    [Fact]
    public void GenerateId_ResultShouldBeUrlSafe()
    {
        // Arrange
        var problematicParts = new[]
        {
            "https://example.com/path?query=value",
            "name@domain.com",
            "50%",
            "a+b=c",
            "hello world",
            "#hashtag"
        };

        // Act
        var hash = _generator.GenerateId(problematicParts);

        // Assert - verify hash is URL-safe
        hash.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$", "Hash should only contain URL-safe characters");
        hash.Should().NotContain("?");
        hash.Should().NotContain("&");
        hash.Should().NotContain(" ");
        hash.Should().NotContain("#");
        hash.Should().NotContain("%");
        hash.Should().NotContain("+");
        hash.Should().NotContain("=");
        hash.Should().NotContain("/");
        hash.Should().HaveLength(43);
    }

    [Fact]
    public void GenerateId_WithVeryLongStrings_ShouldProduceFixedLengthHash()
    {
        // Arrange
        var longString = new string('A', 10000);
        var parts = new[] { longString, "Short", longString };

        // Act
        var hash = _generator.GenerateId(parts);

        // Assert
        hash.Should().HaveLength(43, "Hash should always be 43 characters regardless of input length");
        hash.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$");
    }

    [Fact]
    public void GenerateId_WithSpecialXmlCharacters_ShouldHandleCorrectly()
    {
        // Arrange
        var parts = new[] { "<tag>", "&amp;", "\"quoted\"", "'single'" };

        // Act
        var hash = _generator.GenerateId(parts);

        // Assert
        hash.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$");
        hash.Should().HaveLength(43);
    }

    [Fact]
    public void GenerateId_MultipleGenerationsOfSameValue_ShouldProduceSameResult()
    {
        // Arrange
        var parts = new[] { "NORTH", "F001" };

        // Act
        var hash1 = _generator.GenerateId(parts);
        var hash2 = _generator.GenerateId(parts);
        var hash3 = _generator.GenerateId(parts);

        // Assert
        hash1.Should().Be(hash2);
        hash2.Should().Be(hash3);
    }

    [Fact]
    public void GenerateId_WithRealWorldExample_ShouldBeUrlSafeAndConsistent()
    {
        // Arrange - simulate real-world composite key from StandardDataSetDefinitionsBuilder
        var region = "SOUTH-WEST/DEVON";
        var farmId = "UK/FARM#12345";
        var subId = "UNIT A+B";

        // Act
        var hash1 = _generator.GenerateId(region, farmId, subId);
        var hash2 = _generator.GenerateId(region, farmId, subId);

        // Assert
        // Should be URL-safe (can be used in routes)
        hash1.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$");
        hash1.Should().NotContain("/");
        hash1.Should().NotContain("?", because: "question mark is not URL-safe");
        hash1.Should().NotContain("#", because: "hash is not URL-safe");
        hash1.Should().NotContain("+", because: "plus is converted to - in Base64URL");
        hash1.Should().NotContain(" ", because: "space is not URL-safe");
        hash1.Should().HaveLength(43);

        // Should be consistent
        hash1.Should().Be(hash2);
    }

    [Fact]
    public void GenerateId_WithDifferentOrdering_ShouldProduceDifferentHashes()
    {
        // Arrange
        var parts1 = new[] { "NORTH", "F001" };
        var parts2 = new[] { "F001", "NORTH" }; // Reversed order

        // Act
        var hash1 = _generator.GenerateId(parts1);
        var hash2 = _generator.GenerateId(parts2);

        // Assert
        hash1.Should().NotBe(hash2, "Different order should produce different hash");
    }

    [Fact]
    public void GenerateId_HashShouldBeDeterministic()
    {
        // Arrange - test that hashing is deterministic across multiple calls
        var testCases = new[]
        {
            new[] { "CPH001" },
            new[] { "NORTH", "F001" },
            new[] { "REGION", "FARM", "SUB" },
            new[] { "Test/With/Slashes", "And#Special@Chars" }
        };

        foreach (var parts in testCases)
        {
            // Act
            var results = Enumerable.Range(0, 10)
                .Select(_ => _generator.GenerateId(parts))
                .ToList();

            // Assert - all hashes should be identical
            results.Distinct().Should().HaveCount(1, "All hashes for same input should be identical");
            results.Should().AllSatisfy(h => h.Should().Be(results[0]));
        }
    }

    [Fact]
    public void GenerateId_WithCaseSensitiveInputs_ShouldProduceDifferentHashes()
    {
        // Arrange
        var lowercase = new[] { "north", "f001" };
        var uppercase = new[] { "NORTH", "F001" };

        // Act
        var hash1 = _generator.GenerateId(lowercase);
        var hash2 = _generator.GenerateId(uppercase);

        // Assert
        hash1.Should().NotBe(hash2, "Hashing should be case-sensitive");
    }

    [Fact]
    public void GenerateId_MultipleInstances_ShouldProduceSameHash()
    {
        // Arrange
        var generator1 = new RecordIdGenerator();
        var generator2 = new RecordIdGenerator();
        var parts = new[] { "NORTH", "F001" };

        // Act
        var hash1 = generator1.GenerateId(parts);
        var hash2 = generator2.GenerateId(parts);

        // Assert
        hash1.Should().Be(hash2, "Different instances should produce same hash for same input");
    }
}