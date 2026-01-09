namespace KeeperData.Core.Reports;

/// <summary>
/// Extension methods for <see cref="string"/>.
/// </summary>
public static class StringExtensions
{
    /// <summary>
    /// Converts the string to an integer, or returns null if the conversion fails.
    /// </summary>
    /// <param name="value">The string to convert.</param>
    /// <returns>The integer value if conversion succeeds; otherwise, null.</returns>
    public static int? ToInteger(this string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return int.TryParse(value, out var result) ? result : null;
    }

    /// <summary>
    /// Converts the string to a DateTime, or returns null if the conversion fails.
    /// </summary>
    /// <param name="value">The string to convert.</param>
    /// <param name="formats">The date/time formats to try.</param>
    /// <returns>The DateTime value if conversion succeeds; otherwise, null.</returns>
    public static DateTime? ToDateTime(this string? value, params string[] formats)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        foreach (var format in formats)
        {
            if (DateTime.TryParseExact(value, format, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var result))
            {
                return result;
            }
        }

        return null;
    }
}
