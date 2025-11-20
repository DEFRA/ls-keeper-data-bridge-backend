using FluentAssertions;
using KeeperData.Core.Querying.Impl;
using MongoDB.Bson;

namespace KeeperData.Core.Tests.Unit.Querying;

public class DocumentProjectorTests
{
    private readonly DocumentProjector _sut;

    public DocumentProjectorTests()
    {
        _sut = new DocumentProjector();
    }

    [Fact]
    public void ProjectDocuments_WithNullFieldList_ReturnsAllFields()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "_id", "123" },
                { "Field1", "Value1" },
                { "Field2", "Value2" },
                { "Field3", "Value3" }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, null);

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().HaveCount(4); // All 4 fields
        result[0].Should().ContainKey("_id");
        result[0].Should().ContainKey("Field1");
        result[0].Should().ContainKey("Field2");
        result[0].Should().ContainKey("Field3");
    }

    [Fact]
    public void ProjectDocuments_WithEmptyFieldList_ReturnsAllFields()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "_id", "123" },
                { "Field1", "Value1" },
                { "Field2", "Value2" }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string>());

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().HaveCount(3); // All 3 fields
    }

    [Fact]
    public void ProjectDocuments_WithSingleField_ReturnsOnlyThatField()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "_id", "123" },
                { "Field1", "Value1" },
                { "Field2", "Value2" },
                { "Field3", "Value3" }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "Field1" });

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().HaveCount(1);
        result[0].Should().ContainKey("Field1");
        result[0]["Field1"].Should().Be("Value1");
    }

    [Fact]
    public void ProjectDocuments_WithMultipleFields_ReturnsOnlyThoseFields()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "_id", "123" },
                { "Field1", "Value1" },
                { "Field2", "Value2" },
                { "Field3", "Value3" },
                { "Field4", "Value4" }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "Field1", "Field3" });

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().HaveCount(2);
        result[0].Should().ContainKey("Field1");
        result[0].Should().ContainKey("Field3");
        result[0].Should().NotContainKey("Field2");
        result[0].Should().NotContainKey("Field4");
    }

    [Fact]
    public void ProjectDocuments_CaseInsensitive_MatchesFields()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "FieldName", "Value1" },
                { "OtherField", "Value2" }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "fieldname" }); // lowercase

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().HaveCount(1);
        result[0].Should().ContainKey("FieldName");
    }

    [Fact]
    public void ProjectDocuments_WithNonExistentField_ExcludesIt()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "Field1", "Value1" },
                { "Field2", "Value2" }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "Field1", "NonExistent" });

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().HaveCount(1); // Only Field1
        result[0].Should().ContainKey("Field1");
        result[0].Should().NotContainKey("NonExistent");
    }

    [Fact]
    public void ProjectDocuments_WithMultipleDocuments_ProjectsEachCorrectly()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "Field1", "Value1A" },
                { "Field2", "Value2A" },
                { "Field3", "Value3A" }
            },
            new BsonDocument
            {
                { "Field1", "Value1B" },
                { "Field2", "Value2B" },
                { "Field3", "Value3B" }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "Field1", "Field2" });

        // Assert
        result.Should().HaveCount(2);
        result[0].Should().HaveCount(2);
        result[1].Should().HaveCount(2);

        result[0]["Field1"].Should().Be("Value1A");
        result[0]["Field2"].Should().Be("Value2A");
        result[1]["Field1"].Should().Be("Value1B");
        result[1]["Field2"].Should().Be("Value2B");
    }

    [Fact]
    public void ProjectDocuments_WithVariousDataTypes_ConvertsCorrectly()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "StringField", "text" },
                { "IntField", 42 },
                { "DoubleField", 3.14 },
                { "BoolField", true },
                { "DateField", new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
                { "NullField", BsonNull.Value }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string>
        {
            "StringField", "IntField", "DoubleField", "BoolField", "DateField", "NullField"
        });

        // Assert
        result.Should().HaveCount(1);
        result[0]["StringField"].Should().Be("text");
        result[0]["IntField"].Should().Be(42);
        result[0]["DoubleField"].Should().Be(3.14);
        result[0]["BoolField"].Should().Be(true);
        result[0]["DateField"].Should().BeOfType<DateTime>();
        result[0]["NullField"].Should().BeNull();
    }

    [Fact]
    public void ProjectDocuments_WithNestedDocument_ConvertsCorrectly()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "TopLevel", "Value" },
                { "Nested", new BsonDocument
                    {
                        { "ChildField1", "ChildValue1" },
                        { "ChildField2", "ChildValue2" }
                    }
                }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "TopLevel", "Nested" });

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().HaveCount(2);
        result[0].Should().ContainKey("Nested");

        var nested = result[0]["Nested"] as Dictionary<string, object?>;
        nested.Should().NotBeNull();
        nested!.Should().ContainKey("ChildField1");
        nested.Should().ContainKey("ChildField2");
    }

    [Fact]
    public void ProjectDocuments_WithArray_ConvertsCorrectly()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "Field1", "Value1" },
                { "ArrayField", new BsonArray { "item1", "item2", "item3" } }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "Field1", "ArrayField" });

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().ContainKey("ArrayField");

        var array = result[0]["ArrayField"] as IEnumerable<object>;
        array.Should().NotBeNull();
        array.Should().HaveCount(3);
    }

    [Fact]
    public void ProjectDocuments_WithEmptyDocumentList_ReturnsEmptyList()
    {
        // Arrange
        var documents = new List<BsonDocument>();

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "Field1" });

        // Assert
        result.Should().BeEmpty();
    }

    [Fact]
    public void ProjectDocuments_WithObjectId_ConvertsToString()
    {
        // Arrange
        var objectId = ObjectId.GenerateNewId();
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "_id", objectId },
                { "Field1", "Value1" }
            }
        };

        // Act
        var result = _sut.ProjectDocuments(documents, new List<string> { "_id", "Field1" });

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().ContainKey("_id");
        result[0]["_id"].Should().BeOfType<string>();
        result[0]["_id"].Should().Be(objectId.ToString());
    }
}