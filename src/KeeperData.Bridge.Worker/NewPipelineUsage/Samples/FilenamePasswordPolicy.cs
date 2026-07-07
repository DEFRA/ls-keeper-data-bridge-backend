using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Bridge.Worker.NewPipelineUsage.Samples;

/// <summary>Scheme A: the decryption password IS the file name (including extension).
/// This is the genuine policy for the current LITP_* datasets, not a stub.</summary>
public sealed class FilenamePasswordPolicy : IPasswordPolicy
{
    public string DerivePassword(DataSetDefinition dataset, string fileName) => fileName;
}
