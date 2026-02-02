using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Represents the outcome of a rule execution.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Simple result record with factory methods - tested through rule tests.")]
public sealed record RuleResult
{
    /// <summary>
    /// Gets whether the rule detected an issue.
    /// </summary>
    public bool HasIssue { get; init; }

    /// <summary>
    /// Gets the issue code if an issue was detected.
    /// </summary>
    public string? IssueCode { get; init; }

    /// <summary>
    /// Gets additional contextual data about the issue.
    /// </summary>
    public IReadOnlyDictionary<string, object?>? ContextData { get; init; }

    /// <summary>
    /// Creates a result indicating no issue was found.
    /// </summary>
    public static RuleResult NoIssue() => new() { HasIssue = false };

    /// <summary>
    /// Creates a result indicating an issue was found.
    /// </summary>
    /// <param name="issueCode">The issue code.</param>
    /// <param name="contextData">Optional contextual data about the issue.</param>
    public static RuleResult Issue(string issueCode, IReadOnlyDictionary<string, object?>? contextData = null)
        => new() { HasIssue = true, IssueCode = issueCode, ContextData = contextData };
}
