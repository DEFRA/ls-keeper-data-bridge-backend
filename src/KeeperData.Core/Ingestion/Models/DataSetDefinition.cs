namespace KeeperData.Core.Ingestion.Models;

//  DOMAIN MODEL - the values that flow BETWEEN stages.
//  After Discover, exactly ONE item per dataset flows down the pipe; the
//  "many files within a dataset" is streamed INSIDE each stage.

/// <summary>Static configuration for one dataset (mirrors the existing DataSetDefinition).</summary>
public sealed record DataSetDefinition(
    string Name,
    string CleanName,
    string FilePrefixFormat,
    IReadOnlyList<string> PrimaryKeyHeaderNames,
    string ChangeTypeHeaderName,
    IngestionMode IngestionMode,
    PasswordPolicyKind PasswordPolicy);
