using System.Runtime.CompilerServices;
using System.Text;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>
/// Replays XsvHcdtHelper's record reader and strict RFC 4180 field check over a raw file to locate
/// the first record the normaliser rejects. The package's <see cref="XsvHcdtHelper.XsvValidationException"/>
/// carries neither the file nor the record position, so this exists to answer "which record" for
/// whoever is looking at a failed import.
///
/// The rules are the package's own, mirrored from XsvRfc4180RecordReader: the delimiter is resolved
/// from the first record's second character, records split on CR or LF outside quoted fields, and a
/// quoted field must be followed by the delimiter or the end of the record. It is a best-effort
/// diagnostic - if the package's rules drift, the reported position may be imprecise, but the
/// original validation exception still propagates unchanged.
/// </summary>
public static class Rfc4180RecordScanner
{
    /// <summary>The first record the strict field check rejects: its 1-based position counting every
    /// record in the file (H, C, D and T alike), the raw record text, and the check's reason.</summary>
    public sealed record Finding(int RecordNumber, string RawRecord, string Reason);

    /// <summary>Reads the file the way the package's reader does and returns the first record whose
    /// fields fail the strict check, or null when every record parses. The stream is left open.</summary>
    public static async Task<Finding?> FindFirstInvalidAsync(Stream input, CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(
            input, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);

        char? delimiter = null;
        var recordNumber = 0;

        await foreach (var record in ReadRecordsAsync(reader, cancellationToken))
        {
            recordNumber++;

            if (delimiter is null)
            {
                if (record.Length > 1 && record[1] is '|' or ',')
                {
                    delimiter = record[1];
                }
                else
                {
                    return new Finding(recordNumber, record, "Cannot auto-detect delimiter. Expected 'H|' or 'H,'.");
                }
            }

            var reason = CheckFields(record, delimiter.Value);
            if (reason is not null)
            {
                return new Finding(recordNumber, record, reason);
            }
        }

        return null;
    }

    /// <summary>The record with every non-structural character masked, for logging: the tag, quotes
    /// and delimiters show the failure's shape without carrying field content into the log.</summary>
    public static string MaskForLog(string record, int maxLength = 300)
    {
        var masked = new StringBuilder(Math.Min(record.Length, maxLength + 1));

        for (var index = 0; index < record.Length && index < maxLength; index++)
        {
            var character = record[index];
            masked.Append(index == 0 || character is '"' or '|' or ',' ? character : '#');
        }

        if (record.Length > maxLength)
        {
            masked.Append('…');
        }

        return masked.ToString();
    }

    /// <summary>Quote-aware record splitting, mirroring ReadRawRecordAsync: a newline inside a quoted
    /// field belongs to the record, and a quote inside a quoted field followed by anything but a
    /// second quote leaves quoted mode - so malformed quoting still splits the file into the same
    /// records the parser saw.</summary>
    private static async IAsyncEnumerable<string> ReadRecordsAsync(
        StreamReader reader,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var record = new StringBuilder();
        var inQuotedField = false;
        var quotePending = false;
        var atFieldStart = true;
        var discardLeadingLineFeed = false;
        var readAnyCharacter = false;
        int? unread = null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int code;
            if (unread is { } pending)
            {
                unread = null;
                code = pending;
            }
            else
            {
                code = reader.Read();
            }

            if (code == -1)
            {
                if (readAnyCharacter)
                {
                    yield return record.ToString();
                }

                yield break;
            }

            var character = (char)code;

            if (discardLeadingLineFeed)
            {
                discardLeadingLineFeed = false;
                if (character == '\n')
                {
                    continue;
                }
            }

            readAnyCharacter = true;

            if (inQuotedField && quotePending)
            {
                if (character == '"')
                {
                    quotePending = false;
                    record.Append(character);
                    continue;
                }

                inQuotedField = false;
                quotePending = false;
                unread = character;
                continue;
            }

            if (character == '\r' && !inQuotedField)
            {
                discardLeadingLineFeed = true;
                yield return record.ToString();
                record.Clear();
                atFieldStart = true;
                readAnyCharacter = false;
                continue;
            }

            if (character == '\n' && !inQuotedField)
            {
                yield return record.ToString();
                record.Clear();
                atFieldStart = true;
                readAnyCharacter = false;
                continue;
            }

            record.Append(character);

            if (inQuotedField)
            {
                quotePending = character == '"';
                continue;
            }

            if (character == '"' && atFieldStart)
            {
                inQuotedField = true;
                atFieldStart = false;
            }
            else
            {
                atFieldStart = character is ',' or '|';
            }
        }
    }

    /// <summary>The strict field check, mirroring ParseFields but tracking validity rather than
    /// building values: a closing quote must be followed by the delimiter or the end of the record,
    /// and a quoted field must close before the record ends.</summary>
    private static string? CheckFields(string record, char delimiter)
    {
        var inQuotedField = false;
        var atFieldStart = true;
        var afterClosingQuote = false;

        for (var index = 0; index < record.Length; index++)
        {
            var character = record[index];

            if (inQuotedField)
            {
                if (character == '"')
                {
                    if (index + 1 < record.Length && record[index + 1] == '"')
                    {
                        index++;
                    }
                    else
                    {
                        inQuotedField = false;
                        afterClosingQuote = true;
                    }
                }

                continue;
            }

            if (afterClosingQuote)
            {
                if (character != delimiter)
                {
                    return $"A quoted field must be followed by a delimiter or the end of the record. (Non-delimiter at offset {index}.)";
                }

                afterClosingQuote = false;
                atFieldStart = true;
                continue;
            }

            if (character == delimiter)
            {
                atFieldStart = true;
            }
            else if (character == '"' && atFieldStart)
            {
                inQuotedField = true;
                atFieldStart = false;
            }
            else
            {
                atFieldStart = false;
            }
        }

        return inQuotedField
            ? "A quoted field was not terminated before the end of the record."
            : null;
    }
}
