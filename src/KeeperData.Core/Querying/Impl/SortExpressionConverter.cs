using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Querying.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// Converts custom SortExpression to MongoDB SortDefinition
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB sort converter - covered by integration tests.")]
internal static class SortExpressionConverter
{
    public static SortDefinition<BsonDocument>? ToMongoSort(SortExpression? sort)
    {
        if (sort == null)
        {
            return null;
        }

        return sort switch
        {
            SingleFieldSort single => ConvertSingle(single),
            CompoundSort compound => ConvertCompound(compound),
            _ => throw new NotSupportedException($"Sort type '{sort.GetType().Name}' is not supported")
        };
    }

    private static SortDefinition<BsonDocument> ConvertSingle(SingleFieldSort sort)
    {
        return sort.Direction == Models.SortDirection.Ascending
            ? Builders<BsonDocument>.Sort.Ascending(sort.FieldName)
            : Builders<BsonDocument>.Sort.Descending(sort.FieldName);
    }

    private static SortDefinition<BsonDocument> ConvertCompound(CompoundSort sort)
    {
        var mongoSorts = sort.Sorts.Select(s => ToMongoSort(s)!).ToList();
        return Builders<BsonDocument>.Sort.Combine(mongoSorts);
    }
}
