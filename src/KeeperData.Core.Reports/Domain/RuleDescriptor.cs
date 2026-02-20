using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Domain;

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record RuleDescriptor(string RuleId, string UserRuleNo, string UserErrorCode, string UserDescription, string Tag);