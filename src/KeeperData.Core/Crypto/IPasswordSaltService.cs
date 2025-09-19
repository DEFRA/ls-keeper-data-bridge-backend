namespace KeeperData.Core.Crypto;

public record PasswordSalt(string Password, string Salt);

public interface IPasswordSaltService
{
    PasswordSalt Get(string fileName);

    string GenerateFileName();
}