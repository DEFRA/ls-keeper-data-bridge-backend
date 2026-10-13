using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Impl;
using Microsoft.Extensions.Configuration;
using System.Text.RegularExpressions;

namespace KeeperData.Infrastructure.Crypto;

public partial class PasswordSaltService(IConfiguration configuration) : IPasswordSaltService
{
    private const int IsoDateLength = 10;

    private readonly IConfiguration _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

    public PasswordSalt Get(string fileName, PasswordDerivationPolicy policy = PasswordDerivationPolicy.FileNameVerbatim)
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

        // Object keys carry folders once the source prefix is the bucket root, but the source system
        // only ever encrypted against the name.
        var name = fileName[(fileName.LastIndexOf('/') + 1)..];

        var password = policy switch
        {
            PasswordDerivationPolicy.FileNameVerbatim => name,
            PasswordDerivationPolicy.CtsDerived => DeriveCtsPassword(name),
            _ => throw new NotSupportedException($"Unknown password derivation policy '{policy}'.")
        };

        return new PasswordSalt(password, salt);
    }

    /// <summary>The date from the trailing timestamp, then every preceding underscore-separated
    /// segment in reverse order. CTS names carry compound extensions (.xsvn.csv, .csv.enc), and no
    /// part of the extension belongs to the password, so everything from the first dot is dropped.</summary>
    private static string DeriveCtsPassword(string fileName)
    {
        var dot = fileName.IndexOf('.', StringComparison.Ordinal);
        var stem = dot < 0 ? fileName : fileName[..dot];

        var segments = stem.Split('_');

        if (segments.Length < 2)
        {
            throw new InvalidOperationException(
                $"Cannot derive a CTS password from '{fileName}': the name has no underscore-separated segments to build one from.");
        }

        var timestamp = segments[^1];

        if (!CtsTimestamp().IsMatch(timestamp))
        {
            throw new InvalidOperationException(
                $"Cannot derive a CTS password from '{fileName}': the final segment '{timestamp}' is not a yyyy-MM-dd-HHmmss timestamp.");
        }

        var leading = segments[..^1];
        Array.Reverse(leading);

        return $"{timestamp[..IsoDateLength]}_{string.Join('_', leading)}";
    }

    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}-\d{6}$")]
    private static partial Regex CtsTimestamp();
}
