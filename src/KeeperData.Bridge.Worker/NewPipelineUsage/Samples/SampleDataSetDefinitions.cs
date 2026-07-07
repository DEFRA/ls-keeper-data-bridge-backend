using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Bridge.Worker.NewPipelineUsage.Samples;

/// <summary>Illustrative dataset definitions for the demo. Shaped like the real feed:
/// LITP_* prefixes, "CHANGE_TYPE" delta marker, Scheme A (password = file name).</summary>
public sealed class SampleDataSetDefinitions : IDataSetDefinitions
{
    public IReadOnlyList<DataSetDefinition> All { get; } =
    [
        new DataSetDefinition(
            Name: "LITP_CPH_HOLDINGS",
            CleanName: "cph_holdings",
            FilePrefixFormat: "LITP_CPH_HOLDINGS_{0}",           // {0} = yyyyMMddHHmmss
            PrimaryKeyHeaderNames: ["cph"],
            ChangeTypeHeaderName: "CHANGE_TYPE",
            IngestionMode: IngestionMode.Deltas,
            PasswordPolicy: PasswordPolicyKind.Filename),

        new DataSetDefinition(
            Name: "LITP_AMLS2PORT",
            CleanName: "amls2_port",
            FilePrefixFormat: "LITP_AMLS2PORT_{0}",
            PrimaryKeyHeaderNames: ["cph"],
            ChangeTypeHeaderName: "CHANGE_TYPE",
            IngestionMode: IngestionMode.Deltas,
            PasswordPolicy: PasswordPolicyKind.Filename),

        new DataSetDefinition(
            Name: "LITP_PARTY_ROLES",
            CleanName: "party_roles",
            FilePrefixFormat: "LITP_PARTY_ROLES_{0}",
            PrimaryKeyHeaderNames: ["party_id", "role_code"],    // composite key
            ChangeTypeHeaderName: "CHANGE_TYPE",
            IngestionMode: IngestionMode.Snapshot,
            PasswordPolicy: PasswordPolicyKind.Filename),
    ];
}
