using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Core.Reports;

/// <summary>
/// Extension methods for <see cref="QueryParameters"/>.
/// </summary>
public static class QueryParameterExtensions
{
    public static string GenerateCacheKey(this QueryParameters parameters)
    {
        var sb = new StringBuilder();
        sb.Append(parameters.CollectionName);
        sb.Append('|');
        sb.Append(parameters.Filter?.ToString() ?? string.Empty);
        sb.Append('|');
        sb.Append(parameters.Skip);
        sb.Append('|');
        sb.Append(parameters.Top);
        sb.Append('|');
        sb.Append(parameters.Sort?.ToString() ?? string.Empty);
        sb.Append('|');
        sb.Append(parameters.IncludeCount);

        if (parameters.FieldsToSelect is { Count: > 0 })
        {
            sb.Append('|');
            sb.Append(string.Join(',', parameters.FieldsToSelect));
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()));
        return Convert.ToHexString(hash);
    }
}
