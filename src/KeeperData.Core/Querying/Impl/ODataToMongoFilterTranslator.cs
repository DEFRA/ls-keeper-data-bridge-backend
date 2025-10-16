using Microsoft.OData;
using Microsoft.OData.Edm;
using Microsoft.OData.UriParser;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// Translates OData filter expressions to MongoDB filter definitions using Microsoft.OData.Core.
/// Supports: eq, ne, gt, ge, lt, le, contains, startswith, endswith, and, or
/// </summary>
internal class ODataToMongoFilterTranslator
{
    private readonly IEdmModel _model;
    private readonly IEdmEntityType _entityType;

    public ODataToMongoFilterTranslator()
    {
        // Create a minimal EDM model for dynamic entity
        var model = new EdmModel();
        var entityType = new EdmEntityType("Default", "DynamicEntity", null, false, true);
        model.AddElement(entityType);

        var container = new EdmEntityContainer("Default", "Container");
        container.AddEntitySet("DynamicEntities", entityType);
        model.AddElement(container);

        _model = model;
        _entityType = entityType;
    }

    public FilterDefinition<BsonDocument> Parse(string filterExpression)
    {
        if (string.IsNullOrWhiteSpace(filterExpression))
        {
            return Builders<BsonDocument>.Filter.Empty;
        }

        try
        {
            var parser = new ODataUriParser(_model, new Uri("http://temp"), new Uri($"http://temp/DynamicEntities?$filter={Uri.EscapeDataString(filterExpression)}"));
            var filterClause = parser.ParseFilter();

            if (filterClause == null)
            {
                return Builders<BsonDocument>.Filter.Empty;
            }

            return TranslateFilterNode(filterClause.Expression);
        }
        catch (ODataException ex)
        {
            throw new ArgumentException($"Invalid OData filter expression: {ex.Message}", nameof(filterExpression), ex);
        }
    }

    private FilterDefinition<BsonDocument> TranslateFilterNode(QueryNode node)
    {
        return node switch
        {
            BinaryOperatorNode binaryNode => TranslateBinaryOperator(binaryNode),
            UnaryOperatorNode unaryNode => TranslateUnaryOperator(unaryNode),
            SingleValueFunctionCallNode functionNode => TranslateFunctionCall(functionNode),
            _ => throw new NotSupportedException($"Query node type '{node.GetType().Name}' is not supported")
        };
    }

    private FilterDefinition<BsonDocument> TranslateBinaryOperator(BinaryOperatorNode node)
    {
        return node.OperatorKind switch
        {
            BinaryOperatorKind.Equal => TranslateComparison(node, (field, value) => Builders<BsonDocument>.Filter.Eq(field, value)),
            BinaryOperatorKind.NotEqual => TranslateComparison(node, (field, value) => Builders<BsonDocument>.Filter.Ne(field, value)),
            BinaryOperatorKind.GreaterThan => TranslateComparison(node, (field, value) => Builders<BsonDocument>.Filter.Gt(field, value)),
            BinaryOperatorKind.GreaterThanOrEqual => TranslateComparison(node, (field, value) => Builders<BsonDocument>.Filter.Gte(field, value)),
            BinaryOperatorKind.LessThan => TranslateComparison(node, (field, value) => Builders<BsonDocument>.Filter.Lt(field, value)),
            BinaryOperatorKind.LessThanOrEqual => TranslateComparison(node, (field, value) => Builders<BsonDocument>.Filter.Lte(field, value)),
            BinaryOperatorKind.And => Builders<BsonDocument>.Filter.And(
                TranslateFilterNode(node.Left),
                TranslateFilterNode(node.Right)),
            BinaryOperatorKind.Or => Builders<BsonDocument>.Filter.Or(
                TranslateFilterNode(node.Left),
                TranslateFilterNode(node.Right)),
            _ => throw new NotSupportedException($"Binary operator '{node.OperatorKind}' is not supported")
        };
    }

    private FilterDefinition<BsonDocument> TranslateComparison(
        BinaryOperatorNode node,
        Func<string, BsonValue, FilterDefinition<BsonDocument>> filterBuilder)
    {
        var fieldName = GetFieldName(node.Left);
        var value = GetValue(node.Right);
        return filterBuilder(fieldName, value);
    }

    private FilterDefinition<BsonDocument> TranslateUnaryOperator(UnaryOperatorNode node)
    {
        return node.OperatorKind switch
        {
            UnaryOperatorKind.Not => Builders<BsonDocument>.Filter.Not(TranslateFilterNode(node.Operand)),
            _ => throw new NotSupportedException($"Unary operator '{node.OperatorKind}' is not supported")
        };
    }

    private FilterDefinition<BsonDocument> TranslateFunctionCall(SingleValueFunctionCallNode node)
    {
        var functionName = node.Name.ToLowerInvariant();
        var parameters = node.Parameters.ToList();

        if (parameters.Count < 2)
        {
            throw new ArgumentException($"Function '{functionName}' requires at least 2 parameters");
        }

        var fieldName = GetFieldName(parameters[0]);
        var searchValue = GetValue(parameters[1]);
        var searchString = searchValue.AsString;

        return functionName switch
        {
            "contains" => Builders<BsonDocument>.Filter.Regex(
                fieldName,
                new BsonRegularExpression(System.Text.RegularExpressions.Regex.Escape(searchString), "i")),
            
            "startswith" => Builders<BsonDocument>.Filter.Regex(
                fieldName,
                new BsonRegularExpression($"^{System.Text.RegularExpressions.Regex.Escape(searchString)}", "i")),
            
            "endswith" => Builders<BsonDocument>.Filter.Regex(
                fieldName,
                new BsonRegularExpression($"{System.Text.RegularExpressions.Regex.Escape(searchString)}$", "i")),
            
            _ => throw new NotSupportedException($"Function '{functionName}' is not supported")
        };
    }

    private string GetFieldName(QueryNode node)
    {
        // Unwrap ConvertNode if present
        if (node is ConvertNode convertNode)
        {
            node = convertNode.Source;
        }

        return node switch
        {
            SingleValuePropertyAccessNode propertyNode => propertyNode.Property.Name,
            SingleValueOpenPropertyAccessNode openPropertyNode => openPropertyNode.Name,
            _ => throw new ArgumentException($"Expected a property access node, but got '{node.GetType().Name}'")
        };
    }

    private BsonValue GetValue(QueryNode node)
    {
        // Unwrap ConvertNode if present
        if (node is ConvertNode convertNode)
        {
            node = convertNode.Source;
        }

        if (node is not ConstantNode constantNode)
        {
            throw new ArgumentException($"Expected a constant value node, but got '{node.GetType().Name}'");
        }

        if (constantNode.Value == null)
        {
            return BsonNull.Value;
        }

        return constantNode.Value switch
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
            _ => constantNode.Value.ToString() ?? string.Empty
        };
    }
}
