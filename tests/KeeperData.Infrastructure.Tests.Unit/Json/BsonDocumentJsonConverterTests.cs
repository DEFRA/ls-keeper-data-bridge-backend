using System.Text;
using System.Text.Json;
using FluentAssertions;
using KeeperData.Infrastructure.Json;
using MongoDB.Bson;

namespace KeeperData.Infrastructure.Tests.Unit.Json;

public class BsonDocumentJsonConverterTests
{
    private readonly BsonDocumentJsonConverter _sut = new();
    private readonly JsonSerializerOptions _options = new();

    [Fact]
    public void Read_ThrowsNotImplementedException()
    {
        // Arrange
        var json = "{}"u8;
        var reader = new Utf8JsonReader(json);

        // Act & Assert
        try
        {
            _sut.Read(ref reader, typeof(BsonDocument), _options);
            Assert.Fail("Expected NotImplementedException");
        }
        catch (NotImplementedException ex)
        {
            ex.Message.Should().Contain("Deserialization").And.Contain("not supported");
        }
    }

    [Fact]
    public void Write_WithNullValue_WritesNull()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        // Act
        _sut.Write(writer, null!, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("null");
    }

    [Fact]
    public void Write_WithEmptyDocument_WritesEmptyObject()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument();

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("{}");
    }

    [Fact]
    public void Write_WithStringValue_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "name", "John" } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"name":"John"}""");
    }

    [Fact]
    public void Write_WithInt32Value_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "age", 42 } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"age":42}""");
    }

    [Fact]
    public void Write_WithInt64Value_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "bigNumber", BsonValue.Create(9223372036854775807L) } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"bigNumber":9223372036854775807}""");
    }

    [Fact]
    public void Write_WithDoubleValue_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "price", 19.99 } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("19.99");
    }

    [Fact]
    public void Write_WithDecimal128Value_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "amount", new BsonDecimal128(123.45m) } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("123.45");
    }

    [Fact]
    public void Write_WithBooleanValue_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "active", true }, { "deleted", false } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"active":true,"deleted":false}""");
    }

    [Fact]
    public void Write_WithNullValue_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "value", BsonNull.Value } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"value":null}""");
    }

    [Fact]
    public void Write_WithUndefinedValue_SerializesAsNull()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "value", BsonUndefined.Value } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"value":null}""");
    }

    [Fact]
    public void Write_WithDateTimeValue_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var dateTime = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var doc = new BsonDocument { { "createdAt", new BsonDateTime(dateTime) } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("2024-06-15");
    }

    [Fact]
    public void Write_WithObjectIdValue_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var objectId = ObjectId.GenerateNewId();
        var doc = new BsonDocument { { "_id", objectId } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain(objectId.ToString());
    }

    [Fact]
    public void Write_WithArrayValue_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "tags", new BsonArray { "one", "two", "three" } } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"tags":["one","two","three"]}""");
    }

    [Fact]
    public void Write_WithNestedDocument_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument
        {
            { "address", new BsonDocument { { "city", "London" }, { "postcode", "SW1A 1AA" } } }
        };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"address":{"city":"London","postcode":"SW1A 1AA"}}""");
    }

    [Fact]
    public void Write_WithBinaryData_SerializesAsBase64()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var binaryData = new byte[] { 1, 2, 3, 4, 5 };
        var doc = new BsonDocument { { "data", new BsonBinaryData(binaryData) } };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        var expectedBase64 = Convert.ToBase64String(binaryData);
        json.Should().Contain(expectedBase64);
    }

    [Fact]
    public void Write_WithPropertyNamingPolicy_AppliesPolicy()
    {
        // Arrange
        using var stream = new MemoryStream();
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument { { "FirstName", "John" } };

        // Act
        _sut.Write(writer, doc, options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Be("""{"firstName":"John"}""");
    }

    [Fact]
    public void Write_WithComplexDocument_SerializesCorrectly()
    {
        // Arrange
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var doc = new BsonDocument
        {
            { "name", "Test" },
            { "count", 42 },
            { "active", true },
            { "tags", new BsonArray { "a", "b" } },
            { "metadata", new BsonDocument { { "key", "value" } } }
        };

        // Act
        _sut.Write(writer, doc, _options);
        writer.Flush();

        // Assert
        var json = Encoding.UTF8.GetString(stream.ToArray());
        json.Should().Contain("\"name\":\"Test\"");
        json.Should().Contain("\"count\":42");
        json.Should().Contain("\"active\":true");
        json.Should().Contain("\"tags\":[\"a\",\"b\"]");
        json.Should().Contain("\"metadata\":{\"key\":\"value\"}");
    }
}
