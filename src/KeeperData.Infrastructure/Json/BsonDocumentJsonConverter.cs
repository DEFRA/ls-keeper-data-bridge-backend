using MongoDB.Bson;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KeeperData.Infrastructure.Json;

/// <summary>
/// Custom JSON converter for MongoDB BsonDocument to properly serialize to JSON.
/// Handles conversion of BSON types to .NET types for System.Text.Json serialization.
/// </summary>
public class BsonDocumentJsonConverter : JsonConverter<BsonDocument>
{
    public override BsonDocument? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException("Deserialization of BsonDocument is not supported");
    }

    public override void Write(Utf8JsonWriter writer, BsonDocument value, JsonSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStartObject();

        foreach (var element in value.Elements)
        {
            writer.WritePropertyName(options.PropertyNamingPolicy?.ConvertName(element.Name) ?? element.Name);
            WriteBsonValue(writer, element.Value, options);
        }

        writer.WriteEndObject();
    }

    private static void WriteBsonValue(Utf8JsonWriter writer, BsonValue value, JsonSerializerOptions options)
    {
        switch (value.BsonType)
        {
            case BsonType.Null:
            case BsonType.Undefined:
                writer.WriteNullValue();
                break;

            case BsonType.String:
                writer.WriteStringValue(value.AsString);
                break;

            case BsonType.Int32:
                writer.WriteNumberValue(value.AsInt32);
                break;

            case BsonType.Int64:
                writer.WriteNumberValue(value.AsInt64);
                break;

            case BsonType.Double:
                writer.WriteNumberValue(value.AsDouble);
                break;

            case BsonType.Decimal128:
                writer.WriteNumberValue(Decimal128.ToDecimal(value.AsDecimal128));
                break;

            case BsonType.Boolean:
                writer.WriteBooleanValue(value.AsBoolean);
                break;

            case BsonType.DateTime:
                writer.WriteStringValue(value.ToUniversalTime());
                break;

            case BsonType.ObjectId:
                writer.WriteStringValue(value.AsObjectId.ToString());
                break;

            case BsonType.Array:
                writer.WriteStartArray();
                foreach (var item in value.AsBsonArray)
                {
                    WriteBsonValue(writer, item, options);
                }
                writer.WriteEndArray();
                break;

            case BsonType.Document:
                writer.WriteStartObject();
                foreach (var element in value.AsBsonDocument.Elements)
                {
                    writer.WritePropertyName(options.PropertyNamingPolicy?.ConvertName(element.Name) ?? element.Name);
                    WriteBsonValue(writer, element.Value, options);
                }
                writer.WriteEndObject();
                break;

            case BsonType.Binary:
                // Serialize binary data as base64 string
                writer.WriteStringValue(Convert.ToBase64String(value.AsBsonBinaryData.Bytes));
                break;

            default:
                // For any other BSON type, serialize as string
                writer.WriteStringValue(value.ToString());
                break;
        }
    }
}