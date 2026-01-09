using System;
using System.Collections.Generic;
using System.Text;

namespace KeeperData.Core.Reports;

/// <summary>
/// CPH value is formatted like CC/PPP/HHHH where CC is county-code PPP is parish-code and HHHH is holding-code.
/// </summary>
public class Cph
{
    /// <summary>
    /// Gets the original unparsed value.
    /// </summary>
    public string Value { get; }

    /// <summary>
    /// Gets the county code (CC).
    /// </summary>
    public string CountyCode { get; }

    /// <summary>
    /// Gets the parish code (PPP).
    /// </summary>
    public string ParishCode { get; }

    /// <summary>
    /// Gets the holding code (HHHH).
    /// </summary>
    public string HoldingCode { get; }

    private Cph(string value, string countyCode, string parishCode, string holdingCode)
    {
        Value = value;
        CountyCode = countyCode;
        ParishCode = parishCode;
        HoldingCode = holdingCode;
    }

    /// <summary>
    /// Parses a CPH string in the format CC/PPP/HHHH.
    /// </summary>
    /// <param name="value">The CPH string to parse.</param>
    /// <returns>A new <see cref="Cph"/> instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
    /// <exception cref="FormatException">Thrown when value is not in the expected format.</exception>
    public static Cph Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var result = TryParse(value);
        if (result is null)
        {
            throw new FormatException($"The value '{value}' is not a valid CPH. Expected format: CC/PPP/HHHH");
        }

        return result;
    }

    /// <summary>
    /// Attempts to parse a CPH string in the format CC/PPP/HHHH.
    /// </summary>
    /// <param name="value">The CPH string to parse.</param>
    /// <returns>A new <see cref="Cph"/> instance if parsing succeeds; otherwise, null.</returns>
    public static Cph? TryParse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var parts = value.Split('/');
        if (parts.Length != 3)
        {
            return null;
        }

        var countyCode = parts[0];
        var parishCode = parts[1];
        var holdingCode = parts[2];

        if (string.IsNullOrWhiteSpace(countyCode) ||
            string.IsNullOrWhiteSpace(parishCode) ||
            string.IsNullOrWhiteSpace(holdingCode))
        {
            return null;
        }

        return new Cph(value, countyCode, parishCode, holdingCode);
    }

    /// <summary>
    /// Returns the CPH in its canonical format CC/PPP/HHHH.
    /// </summary>
    public override string ToString() => $"{CountyCode}/{ParishCode}/{HoldingCode}";
}

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
