using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.Text.RegularExpressions;

namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// The credentials extracted from an access-key rotation file.
/// </summary>
public sealed record AccessKeyCsvContent(string AccessKeyId, string SecretAccessKey);

/// <summary>
/// Parses the third party's access-key rotation CSV:
/// a header row followed by exactly one data row, where the access key id is the
/// first column and the secret access key is the last column.
/// </summary>
public static partial class AccessKeyCsvParser
{
    [GeneratedRegex("^[A-Z0-9]{16,128}$")]
    private static partial Regex AccessKeyIdPattern();

    /// <summary>
    /// Parses the CSV content. Throws <see cref="AccessKeyFileFormatException"/> on any
    /// structural or validation problem; exception messages never include file contents.
    /// </summary>
    public static AccessKeyCsvContent Parse(Stream content)
    {
        var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            TrimOptions = TrimOptions.Trim,
            BadDataFound = null,
            MissingFieldFound = null
        };

        using var reader = new StreamReader(content, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        using var csv = new CsvReader(reader, configuration);

        if (!csv.Read())
        {
            throw new AccessKeyFileFormatException("The access-key file is empty: no header row found.");
        }

        csv.ReadHeader();

        if (!csv.Read())
        {
            throw new AccessKeyFileFormatException("The access-key file has a header row but no data row.");
        }

        var record = csv.Parser.Record
            ?? throw new AccessKeyFileFormatException("The access-key file data row could not be read.");

        if (record.Length < 2)
        {
            throw new AccessKeyFileFormatException(
                $"The access-key file data row has {record.Length} column(s); at least 2 are required.");
        }

        var accessKeyId = record[0].Trim();
        var secretAccessKey = record[^1].Trim();

        if (csv.Read() && HasContent(csv.Parser.Record))
        {
            throw new AccessKeyFileFormatException(
                "The access-key file has more than one data row; exactly one was expected.");
        }

        if (string.IsNullOrWhiteSpace(accessKeyId))
        {
            throw new AccessKeyFileFormatException("The access key id column is empty.");
        }

        if (string.IsNullOrWhiteSpace(secretAccessKey))
        {
            throw new AccessKeyFileFormatException("The secret access key column is empty.");
        }

        if (!AccessKeyIdPattern().IsMatch(accessKeyId))
        {
            throw new AccessKeyFileFormatException(
                "The access key id failed format validation (expected 16-128 upper-case letters or digits).");
        }

        return new AccessKeyCsvContent(accessKeyId, secretAccessKey);
    }

    private static bool HasContent(string[]? record) =>
        record is not null && record.Any(field => !string.IsNullOrWhiteSpace(field));
}
