namespace KeeperData.Core.Reports.Domain;

/// <summary>
/// Represents a full identifier for a LID (Logical Identifier) entity.
/// It is formatted like 'XX-[CPH]' where XX is a region specifier and CPH is in the format CC/PPP/HHHH.
/// </summary>
public class LidFullIdentifier
{
    /// <summary>
    /// Gets the original unparsed value.
    /// </summary>
    public string Value { get; }

    /// <summary>
    /// Gets the region specifier (XX).
    /// </summary>
    public string Region { get; }

    /// <summary>
    /// Gets the CPH component.
    /// </summary>
    public Cph Cph { get; }

    private LidFullIdentifier(string value, string region, Cph cph)
    {
        Value = value;
        Region = region;
        Cph = cph;
    }

    /// <summary>
    /// Parses a LID full identifier string in the format XX-CC/PPP/HHHH.
    /// </summary>
    /// <param name="value">The LID full identifier string to parse.</param>
    /// <returns>A new <see cref="LidFullIdentifier"/> instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
    /// <exception cref="FormatException">Thrown when value is not in the expected format.</exception>
    public static LidFullIdentifier Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var result = TryParse(value);
        if (result is null)
        {
            throw new FormatException($"The value '{value}' is not a valid LID full identifier. Expected format: XX-CC/PPP/HHHH");
        }

        return result;
    }

    /// <summary>
    /// Attempts to parse a LID full identifier string in the format XX-CC/PPP/HHHH.
    /// </summary>
    /// <param name="value">The LID full identifier string to parse.</param>
    /// <returns>A new <see cref="LidFullIdentifier"/> instance if parsing succeeds; otherwise, null.</returns>
    public static LidFullIdentifier? TryParse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var hyphenIndex = value.IndexOf('-');
        if (hyphenIndex <= 0 || hyphenIndex == value.Length - 1)
        {
            return null;
        }

        var region = value[..hyphenIndex];
        var cphPart = value[(hyphenIndex + 1)..];

        if (string.IsNullOrWhiteSpace(region))
        {
            return null;
        }

        var cph = Cph.TryParse(cphPart);
        if (cph is null)
        {
            return null;
        }

        return new LidFullIdentifier(value, region, cph);
    }

    /// <summary>
    /// Returns the LID full identifier in its canonical format XX-CC/PPP/HHHH.
    /// </summary>
    public override string ToString() => $"{Region}-{Cph}";
}
