using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Validation;

public static class ThrottlePolicyValidator
{
    private const int MinBatchSize = 1;
    private const int MaxBatchSize = 5000;
    private const int MinDelayMs = 0;
    private const int MaxDelayMs = 60000;
    private const int MaxNameLength = 100;

    public static List<string> Validate(string name, ThrottlePolicySettings settings)
    {
        var errors = new List<string>();

        ValidateName(name, errors);
        ValidateIngestion(settings.Ingestion, errors);
        ValidateCleanseAnalysis(settings.CleanseAnalysis, errors);
        ValidateCleanseExport(settings.CleanseExport, errors);
        ValidateIssueDeactivation(settings.IssueDeactivation, errors);
        ValidateIssueQuery(settings.IssueQuery, errors);

        return errors;
    }

    private static void ValidateName(string name, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(name))
            errors.Add("Name is required.");

        if (name.Length > MaxNameLength)
            errors.Add($"Name must be {MaxNameLength} characters or fewer.");
    }

    private static void ValidateIngestion(IngestionThrottleSettings s, List<string> errors)
    {
        ValidateBatch(s.BatchSize, "Ingestion.BatchSize", errors);
        ValidateDelay(s.BatchDelayMs, "Ingestion.BatchDelayMs", errors);
        ValidateBatch(s.ProgressUpdateInterval, "Ingestion.ProgressUpdateInterval", errors);
        ValidateBatch(s.LogInterval, "Ingestion.LogInterval", errors);
    }

    private static void ValidateCleanseAnalysis(CleanseAnalysisThrottleSettings s, List<string> errors)
    {
        ValidateBatch(s.PumpBatchSize, "CleanseAnalysis.PumpBatchSize", errors);
        ValidateDelay(s.PumpDelayMs, "CleanseAnalysis.PumpDelayMs", errors);
        ValidateDelay(s.RecordIssueDelayMs, "CleanseAnalysis.RecordIssueDelayMs", errors);
        ValidateBatch(s.ProgressUpdateInterval, "CleanseAnalysis.ProgressUpdateInterval", errors);
    }

    private static void ValidateCleanseExport(CleanseExportThrottleSettings s, List<string> errors)
    {
        ValidateBatch(s.StreamBatchSize, "CleanseExport.StreamBatchSize", errors);
        ValidateDelay(s.ThrottlingDelayMs, "CleanseExport.ThrottlingDelayMs", errors);
    }

    private static void ValidateIssueDeactivation(IssueDeactivationThrottleSettings s, List<string> errors)
    {
        ValidateBatch(s.BatchSize, "IssueDeactivation.BatchSize", errors);
        ValidateDelay(s.ThrottleDelayMs, "IssueDeactivation.ThrottleDelayMs", errors);
    }

    private static void ValidateIssueQuery(IssueQueryThrottleSettings s, List<string> errors)
    {
        ValidateBatch(s.StreamBatchSize, "IssueQuery.StreamBatchSize", errors);
    }

    private static void ValidateBatch(int value, string field, List<string> errors)
    {
        if (value < MinBatchSize || value > MaxBatchSize)
            errors.Add($"{field} must be between {MinBatchSize} and {MaxBatchSize}.");
    }

    private static void ValidateDelay(int value, string field, List<string> errors)
    {
        if (value < MinDelayMs || value > MaxDelayMs)
            errors.Add($"{field} must be between {MinDelayMs} and {MaxDelayMs}.");
    }
}
