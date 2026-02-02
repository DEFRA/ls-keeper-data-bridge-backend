using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Querying.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// Converts custom FilterExpression to MongoDB FilterDefinition
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB filter converter - covered by integration tests.")]
internal static class FilterExpressionConverter
{
    public static FilterDefinition<BsonDocument> ToMongoFilter(FilterExpression? filter)
    {
        if (filter == null)
        {
            return Builders<BsonDocument>.Filter.Empty;
        }

        return filter switch
        {
            ComparisonFilter comparison => ConvertComparison(comparison),
            InFilter inFilter => ConvertIn(inFilter),
            TextFilter text => ConvertText(text),
            RegexFilter regex => ConvertRegex(regex),
            ExistsFilter exists => ConvertExists(exists),
            LogicalFilter logical => ConvertLogical(logical),
            NotFilter not => ConvertNot(not),
            EmptyFilter => Builders<BsonDocument>.Filter.Empty,
            _ => throw new NotSupportedException($"Filter type '{filter.GetType().Name}' is not supported")
        };
    }

    private static FilterDefinition<BsonDocument> ConvertComparison(ComparisonFilter filter)
    {
        var value = ToBsonValue(filter.Value);

        return filter.Operator switch
        {
            ComparisonOperator.Equal => Builders<BsonDocument>.Filter.Eq(filter.FieldName, value),
            ComparisonOperator.NotEqual => Builders<BsonDocument>.Filter.Ne(filter.FieldName, value),
            ComparisonOperator.GreaterThan => Builders<BsonDocument>.Filter.Gt(filter.FieldName, value),
            ComparisonOperator.GreaterThanOrEqual => Builders<BsonDocument>.Filter.Gte(filter.FieldName, value),
            ComparisonOperator.LessThan => Builders<BsonDocument>.Filter.Lt(filter.FieldName, value),
            ComparisonOperator.LessThanOrEqual => Builders<BsonDocument>.Filter.Lte(filter.FieldName, value),
            _ => throw new NotSupportedException($"Comparison operator '{filter.Operator}' is not supported")
        };
    }

    private static FilterDefinition<BsonDocument> ConvertIn(InFilter filter)
    {
        var values = filter.Values.Select(ToBsonValue);
        return Builders<BsonDocument>.Filter.In(filter.FieldName, values);
    }

    private static FilterDefinition<BsonDocument> ConvertText(TextFilter filter)
    {
        var pattern = filter.Operator switch
        {
            TextOperator.Contains => System.Text.RegularExpressions.Regex.Escape(filter.Value),
            TextOperator.StartsWith => $"^{System.Text.RegularExpressions.Regex.Escape(filter.Value)}",
            TextOperator.EndsWith => $"{System.Text.RegularExpressions.Regex.Escape(filter.Value)}$",
            _ => throw new NotSupportedException($"Text operator '{filter.Operator}' is not supported")
        };

        var options = filter.CaseSensitive ? "" : "i";
        return Builders<BsonDocument>.Filter.Regex(filter.FieldName, new BsonRegularExpression(pattern, options));
    }

    private static FilterDefinition<BsonDocument> ConvertRegex(RegexFilter filter)
    {
        var options = filter.CaseSensitive ? "" : "i";
        return Builders<BsonDocument>.Filter.Regex(filter.FieldName, new BsonRegularExpression(filter.Pattern, options));
    }

    private static FilterDefinition<BsonDocument> ConvertExists(ExistsFilter filter)
    {
        return Builders<BsonDocument>.Filter.Exists(filter.FieldName, filter.ShouldExist);
    }

    private static FilterDefinition<BsonDocument> ConvertLogical(LogicalFilter filter)
    {
        var mongoFilters = filter.Filters.Select(ToMongoFilter).ToList();

        return filter.Operator switch
        {
            LogicalOperator.And => Builders<BsonDocument>.Filter.And(mongoFilters),
            LogicalOperator.Or => Builders<BsonDocument>.Filter.Or(mongoFilters),
            _ => throw new NotSupportedException($"Logical operator '{filter.Operator}' is not supported")
        };
    }

    private static FilterDefinition<BsonDocument> ConvertNot(NotFilter filter)
    {
        var mongoFilter = ToMongoFilter(filter.Filter);
        return Builders<BsonDocument>.Filter.Not(mongoFilter);
    }

    private static BsonValue ToBsonValue(object? value)
    {
        if (value == null)
        {
            return BsonNull.Value;
        }

        return value switch
        {
            string str => str,
            bool b => b,
            int i => i,
            long l => l,
            double d => d,
            decimal dec => dec,
            float f => f,
            DateTime dt => dt,
            DateTimeOffset dto => dto.UtcDateTime,
            _ => value.ToString() ?? string.Empty
        };
    }
}
