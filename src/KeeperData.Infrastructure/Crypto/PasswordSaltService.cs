using KeeperData.Core.Crypto;
using Microsoft.Extensions.Configuration;

namespace KeeperData.Infrastructure.Crypto;

public partial class PasswordSaltService(IConfiguration configuration) : IPasswordSaltService
{
    private readonly IConfiguration _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

    public PasswordSalt Get(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
        {
            throw new ArgumentNullException(nameof(fileName));
        }

        var salt = _configuration["AesSalt"];
        if (string.IsNullOrWhiteSpace(salt))
        {
            throw new InvalidOperationException("AesSalt configuration value is missing or empty.");
        }

        return new PasswordSalt(fileName, salt);
    }

}