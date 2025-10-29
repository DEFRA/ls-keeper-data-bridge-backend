using MongoDB.Bson;
using MongoDB.Driver;
using System.Text.RegularExpressions;

namespace KeeperData.Core.Querying.Impl;

internal class ODataOrderByParser
{
    public SortDefinition<BsonDocument> Parse(string orderByExpression)
    {
        if (string.IsNullOrWhiteSpace(orderByExpression))
        {
            throw new ArgumentException("OrderBy expression cannot be empty", nameof(orderByExpression));
        }

        var sortClauses = orderByExpression
            .Split(',')
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToList();

        if (sortClauses.Count == 0)
        {
            throw new ArgumentException("OrderBy expression must contain at least one field", nameof(orderByExpression));
        }

        if (sortClauses.Count == 1)
        {
            return ParseSingleSort(sortClauses[0]);
        }

        var sortDefinitions = sortClauses.Select(ParseSingleSort).ToList();
        return Builders<BsonDocument>.Sort.Combine(sortDefinitions);
    }

    private SortDefinition<BsonDocument> ParseSingleSort(string sortClause)
    {
        var match = Regex.Match(sortClause, @"^(\w+)(?:\s+(asc|desc))?$", RegexOptions.IgnoreCase);

        if (!match.Success)
        {
            throw new ArgumentException($"Invalid orderBy clause: '{sortClause}'");
        }

        var fieldName = match.Groups[1].Value;
        var direction = match.Groups[2].Success
            ? match.Groups[2].Value.ToLowerInvariant()
            : "asc";

        return direction == "desc"
            ? Builders<BsonDocument>.Sort.Descending(fieldName)
            : Builders<BsonDocument>.Sort.Ascending(fieldName);
    }
}