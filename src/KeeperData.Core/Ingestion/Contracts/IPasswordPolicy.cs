using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>Derives the decryption password for a file per the dataset's policy
/// (Filename = password is the file name; CtsmReversed = the reverse-underscore scheme).</summary>
public interface IPasswordPolicy
{
    string DerivePassword(DataSetDefinition dataset, string fileName);
}
