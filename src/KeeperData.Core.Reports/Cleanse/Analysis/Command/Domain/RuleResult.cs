using KeeperData.Core.Reports.Domain;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

/// <summary>
/// Represents a rule-detected issue to be recorded.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Simple result record with factory methods - tested through rule tests.")]
public sealed record RuleResult
{
    /// <summary>
    /// Gets the rule descriptor identifying what was detected.
    /// </summary>
    public required RuleDescriptor Descriptor { get; init; }

    /// <summary>
    /// Gets optional issue data to persist on the report item.
    /// </summary>
    public IssueContextData? IssueContext { get; init; }

    /// <summary>
    /// Creates a result indicating an issue was found.
    /// </summary>
    public static RuleResult Issue(RuleDescriptor descriptor, IssueContextData? issueContext = null) => new() { Descriptor = descriptor, IssueContext = issueContext };

    public static RuleResult Issue(RuleDescriptor descriptor, Action<IssueContextData> contextProvider)
    {
        var issueContext = new IssueContextData();
        contextProvider(issueContext);
        return new() { Descriptor = descriptor, IssueContext = issueContext };
    }

    public static RuleResult Issue(RuleDescriptor descriptor, LidFullIdentifier lidFullIdentifier)
    {
        var issueContext = new IssueContextData
        {
            CtsLidFullIdentifier = lidFullIdentifier.Value,
            SamCph = lidFullIdentifier.Cph.Value
        };
        return new() { Descriptor = descriptor, IssueContext = issueContext };
    }

    public static RuleResult Issue(RuleDescriptor descriptor, Cph cph)
    {
        var issueContext = new IssueContextData
        {
            SamCph = cph.Value
        };
        return new() { Descriptor = descriptor, IssueContext = issueContext };
    }

    public static RuleResult Issue(RuleDescriptor descriptor, LidFullIdentifier lidFullIdentifier, Cph cph, Action<IssueContextData>? contextProvider = null)
    {
        var issueContext = new IssueContextData
        {
            SamCph = cph.Value,
            CtsLidFullIdentifier = lidFullIdentifier.Value
        };
        contextProvider?.Invoke(issueContext);
        return new() { Descriptor = descriptor, IssueContext = issueContext };
    }
}
