using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.Crypto;

public record PasswordSalt(string Password, string Salt);

public interface IPasswordSaltService
{
    PasswordSalt Get(string fileName, PasswordDerivationPolicy policy = PasswordDerivationPolicy.FileNameVerbatim);
}
