using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

public class SamCphHoldingModel
{
    public required Cph Cph { get; set; }
    public required Dictionary<string, object?> Holding { get; set; }
    public required QueryResult Herd { get; set; }
    public required QueryResult Parties { get; set; }
    public required QueryResult Holders { get; set; }

    public string? AnimalSpeciesCode => Holding.GetValueOrDefault(DataFields.SamCphHoldingFields.AnimalSpeciesCode)?.ToString();

    /// <summary>
    /// FEATURE_NAME in SAM is the "Location name"
    /// </summary>
    public string? LocationName => Holding.GetValueOrDefault(DataFields.SamCphHoldingFields.FeatureName)?.ToString();

    public string[] GetEmailAddresses()
    {
        var emails1 = Parties.Data
            .Select(x => x[DataFields.SamPartyFields.InternetEmailAddress]?.ToString())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var emails2 = Holders.Data
            .Select(x => x[DataFields.SamCphHolderFields.InternetEmailAddress]?.ToString())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return [.. emails1.Union(emails2, StringComparer.OrdinalIgnoreCase).Order()];
    }

    public string[] GetPhoneNumbers()
    {
        var nos1 = Parties.Data
            .SelectMany(x => new[]
            {
                x[DataFields.SamPartyFields.TelephoneNumber]?.ToString(),
                x[DataFields.SamPartyFields.MobileNumber]?.ToString()
            })
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s!)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var nos2 = Holders.Data
            .SelectMany(x => new[]
            {
                x[DataFields.SamCphHolderFields.TelephoneNumber]?.ToString(),
                x[DataFields.SamCphHolderFields.MobileNumber]?.ToString()
            })
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s!)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return [.. nos1.Union(nos2, StringComparer.OrdinalIgnoreCase).Order()];
    }
}
