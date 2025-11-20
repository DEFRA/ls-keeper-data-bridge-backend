using MongoDB.Bson;

namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// Projects documents to include only specified fields.
/// Handles field selection and conversion to dictionaries.
/// </summary>
internal class DocumentProjector
{
    /// <summary>
    /// Projects a list of BSON documents to include only the specified fields.
    /// </summary>
    /// <param name="documents">The documents to project</param>
    /// <param name="fieldsToInclude">The fields to include, or null to include all fields</param>
    /// <returns>A list of dictionaries with only the selected fields</returns>
    public IReadOnlyList<Dictionary<string, object?>> ProjectDocuments(
        IEnumerable<BsonDocument> documents,
        IReadOnlyList<string>? fieldsToInclude)
    {
        if (fieldsToInclude == null || fieldsToInclude.Count == 0)
        {
            return ConvertAllFields(documents);
        }

        var fieldSet = new HashSet<string>(fieldsToInclude, StringComparer.OrdinalIgnoreCase);
        return ProjectSelectedFields(documents, fieldSet);
    }

    private IReadOnlyList<Dictionary<string, object?>> ConvertAllFields(IEnumerable<BsonDocument> documents)
    {
        var result = new List<Dictionary<string, object?>>();

        foreach (var doc in documents)
        {
            var dictionary = new Dictionary<string, object?>();

            foreach (var element in doc.Elements)
            {
                dictionary[element.Name] = ConvertBsonValue(element.Value);
            }

            result.Add(dictionary);
        }

        return result;
    }

    private IReadOnlyList<Dictionary<string, object?>> ProjectSelectedFields(
        IEnumerable<BsonDocument> documents,
        HashSet<string> fieldsToInclude)
    {
        var result = new List<Dictionary<string, object?>>();

        foreach (var doc in documents)
        {
            var dictionary = new Dictionary<string, object?>();

            foreach (var element in doc.Elements)
            {
                if (fieldsToInclude.Contains(element.Name))
                {
                    dictionary[element.Name] = ConvertBsonValue(element.Value);
                }
                else if (IsNestedFieldRequested(element.Name, fieldsToInclude, out var matchedFields))
                {
                    // Handle nested field selections
                    dictionary[element.Name] = ConvertBsonValue(element.Value);
                }
            }

            result.Add(dictionary);
        }

        return result;
    }

    private bool IsNestedFieldRequested(
        string fieldName,
        HashSet<string> fieldsToInclude,
        out List<string> matchedFields)
    {
        matchedFields = fieldsToInclude
            .Where(f => f.StartsWith(fieldName + ".", StringComparison.OrdinalIgnoreCase))
            .ToList();

        return matchedFields.Count > 0;
    }

    private object? ConvertBsonValue(BsonValue value)
    {
        return value.BsonType switch
        {
            BsonType.Null => null,
            BsonType.String => value.AsString,
            BsonType.Int32 => value.AsInt32,
            BsonType.Int64 => value.AsInt64,
            BsonType.Double => value.AsDouble,
            BsonType.Decimal128 => MongoDB.Bson.Decimal128.ToDecimal(value.AsDecimal128),
            BsonType.Boolean => value.AsBoolean,
            BsonType.DateTime => value.ToUniversalTime(),
            BsonType.ObjectId => value.AsObjectId.ToString(),
            BsonType.Array => value.AsBsonArray.Select(ConvertBsonValue).ToList(),
            BsonType.Document => ConvertBsonDocumentToDictionary(value.AsBsonDocument),
            _ => value.ToString()
        };
    }

    private Dictionary<string, object?> ConvertBsonDocumentToDictionary(BsonDocument document)
    {
        var dictionary = new Dictionary<string, object?>();
        foreach (var element in document.Elements)
        {
            dictionary[element.Name] = ConvertBsonValue(element.Value);
        }
        return dictionary;
    }
}