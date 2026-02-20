using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

public class CtsCphHoldingModel
{
    public required LidFullIdentifier Id { get; set; }
    public required Dictionary<string, object?> Holding { get; set; }
    public required QueryResult Keepers { get; set; }

    /// <summary>
    /// ADR_NAME in CTS is the "Location name"
    /// </summary>
    public string? LocationName => Holding.GetValueOrDefault(DataFields.CtsCphHoldingFields.AdrName)?.ToString();


    public string[] GetEmailAddresses()
    {
        var emails = Keepers.Data
            .Select(x => x[DataFields.CtsKeeperFields.ParEmailAddress]?.ToString())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        return emails;
    }

    public string[] GetPhoneNumbers()
    {
        var holdingNumbers = new[]
        {
            Holding[DataFields.CtsCphHoldingFields.LocMobileNumber]?.ToString(),
            Holding[DataFields.CtsCphHoldingFields.LocTelNumber]?.ToString()
        };

        var keeperNumbers = Keepers.Data
            .SelectMany(x => new[]
            {
                x[DataFields.CtsKeeperFields.ParMobileNumber]?.ToString(),
                x[DataFields.CtsKeeperFields.ParTelNumber]?.ToString()
            });

        return [.. holdingNumbers
            .Concat(keeperNumbers)
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Order()];
    }

}
