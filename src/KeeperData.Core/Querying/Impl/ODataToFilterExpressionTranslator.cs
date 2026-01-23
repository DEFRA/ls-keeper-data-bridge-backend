using KeeperData.Core.Querying.Models;
using Microsoft.OData;
using Microsoft.OData.Edm;
using Microsoft.OData.UriParser;

namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// Translates OData filter expressions to custom FilterExpression using Microsoft.OData.Core.
/// Supports: eq, ne, gt, ge, lt, le, contains, startswith, endswith, and, or
/// </summary>
internal class ODataToFilterExpressionTranslator
{
    private readonly IEdmModel _model;
    private readonly IEdmEntityType _entityType;

    public ODataToFilterExpressionTranslator()
    {
        var model = new EdmModel();
        var entityType = new EdmEntityType("Default", "DynamicEntity", null, false, true);
        model.AddElement(entityType);

        var container = new EdmEntityContainer("Default", "Container");
        container.AddEntitySet("DynamicEntities", entityType);
        model.AddElement(container);

        _model = model;
        _entityType = entityType;
    }

    public FilterExpression Parse(string filterExpression)
    {
        if (string.IsNullOrWhiteSpace(filterExpression))
        {
            return FilterExpression.Empty();
        }

        try
        {
            var parser = new ODataUriParser(_model, new Uri("http://temp"), new Uri($"http://temp/DynamicEntities?$filter={Uri.EscapeDataString(filterExpression)}"));
            var filterClause = parser.ParseFilter();

            if (filterClause == null)
            {
                return FilterExpression.Empty();
            }

            return TranslateFilterNode(filterClause.Expression);
        }
        catch (ODataException ex)
        {
            throw new ArgumentException($"Invalid OData filter expression: {ex.Message}", nameof(filterExpression), ex);
        }
    }

    private FilterExpression TranslateFilterNode(QueryNode node)
    {
        return node switch
        {
            BinaryOperatorNode binaryNode => TranslateBinaryOperator(binaryNode),
            UnaryOperatorNode unaryNode => TranslateUnaryOperator(unaryNode),
            SingleValueFunctionCallNode functionNode => TranslateFunctionCall(functionNode),
            _ => throw new NotSupportedException($"Query node type '{node.GetType().Name}' is not supported")
        };
    }

    private FilterExpression TranslateBinaryOperator(BinaryOperatorNode node)
    {
        return node.OperatorKind switch
        {
            BinaryOperatorKind.Equal => TranslateComparison(node, (field, value) => FilterExpression.Equal(field, value)),
            BinaryOperatorKind.NotEqual => TranslateComparison(node, (field, value) => FilterExpression.NotEqual(field, value)),
            BinaryOperatorKind.GreaterThan => TranslateComparison(node, (field, value) => FilterExpression.GreaterThan(field, value!)),
            BinaryOperatorKind.GreaterThanOrEqual => TranslateComparison(node, (field, value) => FilterExpression.GreaterThanOrEqual(field, value!)),
            BinaryOperatorKind.LessThan => TranslateComparison(node, (field, value) => FilterExpression.LessThan(field, value!)),
            BinaryOperatorKind.LessThanOrEqual => TranslateComparison(node, (field, value) => FilterExpression.LessThanOrEqual(field, value!)),
            BinaryOperatorKind.And => FilterExpression.And(
                TranslateFilterNode(node.Left),
                TranslateFilterNode(node.Right)),
            BinaryOperatorKind.Or => FilterExpression.Or(
                TranslateFilterNode(node.Left),
                TranslateFilterNode(node.Right)),
            _ => throw new NotSupportedException($"Binary operator '{node.OperatorKind}' is not supported")
        };
    }

    private FilterExpression TranslateComparison(
        BinaryOperatorNode node,
        Func<string, object?, FilterExpression> filterBuilder)
    {
        var fieldName = GetFieldName(node.Left);
        var value = GetValue(node.Right);
        return filterBuilder(fieldName, value);
    }

    private FilterExpression TranslateUnaryOperator(UnaryOperatorNode node)
    {
        return node.OperatorKind switch
        {
            UnaryOperatorKind.Not => FilterExpression.Not(TranslateFilterNode(node.Operand)),
            _ => throw new NotSupportedException($"Unary operator '{node.OperatorKind}' is not supported")
        };
    }

    private FilterExpression TranslateFunctionCall(SingleValueFunctionCallNode node)
    {
        var functionName = node.Name.ToLowerInvariant();
        var parameters = node.Parameters.ToList();

        if (parameters.Count < 2)
        {
            throw new ArgumentException($"Function '{functionName}' requires at least 2 parameters");
        }

        var fieldName = GetFieldName(parameters[0]);
        var searchValue = GetValue(parameters[1]);
        var searchString = searchValue?.ToString() ?? string.Empty;

        return functionName switch
        {
            "contains" => FilterExpression.Contains(fieldName, searchString),
            "startswith" => FilterExpression.StartsWith(fieldName, searchString),
            "endswith" => FilterExpression.EndsWith(fieldName, searchString),
            _ => throw new NotSupportedException($"Function '{functionName}' is not supported")
        };
    }

    private string GetFieldName(QueryNode node)
    {
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

    private object? GetValue(QueryNode node)
    {
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
            return null;
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
            _ => constantNode.Value
        };
    }
}
