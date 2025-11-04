using FluentAssertions;
using KeeperData.Core.Reporting.Services;

namespace KeeperData.Core.Tests.Unit.Reporting.Services;

/// <summary>
/// Unit tests for LineageIdGenerator to verify ID format and chronological ordering.
/// </summary>
public class LineageIdGeneratorTests
{
    private readonly LineageIdGenerator _generator = new();

    [Fact]
    public void GenerateLineageDocumentId_ShouldCreateValidCompositeKey()
    {
        // Arrange
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH001";

        // Act
        var result = _generator.GenerateLineageDocumentId(collectionName, recordId);

        // Assert
        result.Should().Be("sam_cph_holdings__CPH001");
    }

    [Fact]
    public void GenerateLineageDocumentId_WithNullCollectionName_ShouldThrowArgumentException()
    {
        // Act
        var act = () => _generator.GenerateLineageDocumentId(null!, "CPH001");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*collectionName*");
    }

    [Fact]
    public void GenerateLineageDocumentId_WithEmptyRecordId_ShouldThrowArgumentException()
    {
        // Act
        var act = () => _generator.GenerateLineageDocumentId("sam_cph_holdings", "");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*recordId*");
    }

    [Fact]
    public void GenerateLineageDocumentId_WithDelimiterInCollectionName_ShouldThrowArgumentException()
    {
        // Act
        var act = () => _generator.GenerateLineageDocumentId("invalid__collection", "CPH001");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*delimiter*");
    }

    [Fact]
    public void GenerateLineageEventId_ShouldCreateValidChronologicalKey()
    {
        // Arrange
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH001";
        var eventDate = new DateTime(2024, 12, 15, 14, 30, 45, 123, DateTimeKind.Utc)
            .AddTicks(4560); // Add microseconds

        // Act
        var result = _generator.GenerateLineageEventId(collectionName, recordId, eventDate);

        // Assert
        result.Should().StartWith("sam_cph_holdings__CPH001__20241215143045123456__");
        result.Should().MatchRegex(@"^sam_cph_holdings__CPH001__\d{20}__\d{6}$");
        
        // Extract components
        var parts = result.Split("__");
        parts.Should().HaveCount(4);
        parts[0].Should().Be("sam_cph_holdings");
        parts[1].Should().Be("CPH001");
        parts[2].Should().Be("20241215143045123456"); // Timestamp
        parts[3].Should().MatchRegex(@"^\d{6}$"); // 6-digit random
    }

    [Fact]
    public void GenerateLineageEventId_MultipleCallsWithSameTimestamp_ShouldProduceDifferentIds()
    {
        // Arrange
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH001";
        var eventDate = DateTime.UtcNow;

        // Act - Generate 10 IDs with same timestamp
        var ids = Enumerable.Range(0, 10)
            .Select(_ => _generator.GenerateLineageEventId(collectionName, recordId, eventDate))
            .ToList();

        // Assert - All IDs should be unique due to random component
        ids.Should().OnlyHaveUniqueItems();
        ids.Should().AllSatisfy(id => id.Should().StartWith($"sam_cph_holdings__CPH001__"));
    }

    [Fact]
    public void GenerateLineageEventId_WithDifferentTimestamps_ShouldSortChronologically()
    {
        // Arrange
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH001";
        
        var timestamp1 = new DateTime(2024, 12, 15, 10, 0, 0, DateTimeKind.Utc);
        var timestamp2 = new DateTime(2024, 12, 15, 10, 0, 1, DateTimeKind.Utc);
        var timestamp3 = new DateTime(2024, 12, 15, 10, 1, 0, DateTimeKind.Utc);
        var timestamp4 = new DateTime(2024, 12, 15, 11, 0, 0, DateTimeKind.Utc);
        var timestamp5 = new DateTime(2024, 12, 16, 10, 0, 0, DateTimeKind.Utc);

        // Act
        var id1 = _generator.GenerateLineageEventId(collectionName, recordId, timestamp1);
        var id2 = _generator.GenerateLineageEventId(collectionName, recordId, timestamp2);
        var id3 = _generator.GenerateLineageEventId(collectionName, recordId, timestamp3);
        var id4 = _generator.GenerateLineageEventId(collectionName, recordId, timestamp4);
        var id5 = _generator.GenerateLineageEventId(collectionName, recordId, timestamp5);

        var ids = new[] { id1, id2, id3, id4, id5 };

        // Assert - When sorted lexicographically, they should be in chronological order
        var sortedIds = ids.OrderBy(x => x).ToArray();
        sortedIds.Should().Equal(ids, "IDs should naturally sort chronologically");
        
        // Verify ordering explicitly
        string.Compare(id1, id2, StringComparison.Ordinal).Should().BeLessThan(0);
        string.Compare(id2, id3, StringComparison.Ordinal).Should().BeLessThan(0);
        string.Compare(id3, id4, StringComparison.Ordinal).Should().BeLessThan(0);
        string.Compare(id4, id5, StringComparison.Ordinal).Should().BeLessThan(0);
    }

    [Fact]
    public void GenerateLineageEventId_With100Events_ShouldMaintainChronologicalOrder()
    {
        // Arrange
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH001";
        var baseTime = DateTime.UtcNow;

        // Act - Generate 100 events with incrementing timestamps
        var events = Enumerable.Range(0, 100)
            .Select(i => new
            {
                Timestamp = baseTime.AddMilliseconds(i),
                Id = _generator.GenerateLineageEventId(
                    collectionName, 
                    recordId, 
                    baseTime.AddMilliseconds(i))
            })
            .ToList();

        // Assert - Verify lexicographic sorting matches chronological order
        var sortedByTimestamp = events.OrderBy(e => e.Timestamp).ToList();
        var sortedById = events.OrderBy(e => e.Id, StringComparer.Ordinal).ToList();

        for (int i = 0; i < 100; i++)
        {
            sortedById[i].Timestamp.Should().Be(sortedByTimestamp[i].Timestamp,
                $"ID at position {i} should correspond to timestamp at position {i}");
        }
    }

    [Fact]
    public void GenerateLineageEventId_RandomComponent_ShouldBe6DigitsZeroPadded()
    {
        // Arrange
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH001";
        var eventDate = DateTime.UtcNow;

        // Act - Generate multiple IDs
        var ids = Enumerable.Range(0, 50)
            .Select(_ => _generator.GenerateLineageEventId(collectionName, recordId, eventDate))
            .ToList();

        // Assert - All random components should be 6 digits
        foreach (var id in ids)
        {
            var randomPart = id.Split("__")[3];
            randomPart.Should().MatchRegex(@"^\d{6}$", 
                "random component should be exactly 6 digits");
            randomPart.Length.Should().Be(6);
        }
    }

    [Fact]
    public void GenerateLineageEventId_TimestampFormat_ShouldBe20CharactersFixedWidth()
    {
        // Arrange
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH001";
        
        // Test with various dates to ensure consistent width
        var dates = new[]
        {
            new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2024, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc),
            new DateTime(2024, 6, 15, 12, 30, 45, 123, DateTimeKind.Utc)
        };

        // Act & Assert
        foreach (var date in dates)
        {
            var id = _generator.GenerateLineageEventId(collectionName, recordId, date);
            var timestampPart = id.Split("__")[2];
            
            timestampPart.Length.Should().Be(20, 
                $"timestamp should always be 20 characters for date {date}");
            timestampPart.Should().MatchRegex(@"^\d{20}$");
        }
    }

    [Fact]
    public void GenerateLineageEventId_SameRecordDifferentCollections_ShouldGenerateDifferentKeys()
    {
        // Arrange
        var recordId = "CPH001";
        var collection1 = "sam_cph_holdings";
        var collection2 = "sam_cph_addresses";
        var eventDate = DateTime.UtcNow;

        // Act
        var id1 = _generator.GenerateLineageEventId(collection1, recordId, eventDate);
        var id2 = _generator.GenerateLineageEventId(collection2, recordId, eventDate);

        // Assert
        id1.Should().NotBe(id2);
        id1.Should().StartWith("sam_cph_holdings__CPH001__");
        id2.Should().StartWith("sam_cph_addresses__CPH001__");
    }
}
