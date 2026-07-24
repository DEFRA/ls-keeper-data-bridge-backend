using FluentAssertions;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.KeyRotation;

public class KeyRotationRecordSerializationTests
{
    private static KeyRotationRecord CreateRecord() => new()
    {
        Id = "rotation-1",
        BucketName = "cerespfm-prd-prd1-livestockfeeds",
        RotatedAtUtc = new DateTime(2026, 7, 16, 3, 0, 12, DateTimeKind.Utc),
        Source = KeyRotationSource.Automatic,
        Status = KeyRotationStatus.Active,
        FileKey = "Prd1_LI_CDP_Int_User_accessKeys.csv",
        FileHash = "9f2c1e",
        KeyIdMasked = "AKI...DEF",
        EncryptedAccessKeyId = new EncryptedSecret { KeyVersion = 1, Nonce = "bm9uY2U=", CipherText = "Y2lwaGVy", Tag = "dGFn" },
        EncryptedSecretAccessKey = new EncryptedSecret { KeyVersion = 1, Nonce = "bm9uY2Uy", CipherText = "Y2lwaGVyMg==", Tag = "dGFnMg==" },
        ValidatedAtUtc = new DateTime(2026, 7, 16, 3, 0, 13, DateTimeKind.Utc)
    };

    [Fact]
    public void Record_RoundTripsThroughBson()
    {
        // Arrange
        var record = CreateRecord();

        // Act
        var document = record.ToBsonDocument();
        var rehydrated = BsonSerializer.Deserialize<KeyRotationRecord>(document);

        // Assert
        rehydrated.Should().BeEquivalentTo(record);
    }

    [Fact]
    public void StatusAndSource_AreStoredAsStrings()
    {
        // Arrange - the partial unique index filters on the string value "Active",
        // so the enum representation must be a string, not an int.
        var record = CreateRecord();

        // Act
        var document = record.ToBsonDocument();
        var statusElement = document.Elements.Single(e => e.Name.Equals("status", StringComparison.OrdinalIgnoreCase));
        var sourceElement = document.Elements.Single(e => e.Name.Equals("source", StringComparison.OrdinalIgnoreCase));

        // Assert
        statusElement.Value.BsonType.Should().Be(BsonType.String);
        statusElement.Value.AsString.Should().Be("Active");
        sourceElement.Value.BsonType.Should().Be(BsonType.String);
        sourceElement.Value.AsString.Should().Be("Automatic");
    }

    [Fact]
    public void Record_UsesIdAsDocumentKey()
    {
        // Act
        var document = CreateRecord().ToBsonDocument();

        // Assert
        document["_id"].AsString.Should().Be("rotation-1");
    }
}
