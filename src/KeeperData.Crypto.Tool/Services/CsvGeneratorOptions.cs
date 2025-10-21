namespace KeeperData.Crypto.Tool.Services;

/// <summary>
/// Configuration options for CSV data generation.
/// </summary>
public class CsvGeneratorOptions
{
    /// <summary>
    /// The number of records to generate in the main CSV file.
    /// Default is 1000.
    /// </summary>
    public int MainFileRecordCount { get; set; } = 1000;

    /// <summary>
    /// The percentage of main file records to include in delta files (0.0 to 1.0).
    /// Default is 0.1 (10%).
    /// </summary>
    public double DeltaFileRecordPercentage { get; set; } = 0.1;

    /// <summary>
    /// The percentage of delta records that should be updates (vs deletes).
    /// Default is 0.8 (80% updates, 20% deletes).
    /// </summary>
    public double DeltaUpdatePercentage { get; set; } = 0.8;

    /// <summary>
    /// The output directory where CSV files will be generated.
    /// </summary>
    public required string OutputDirectory { get; set; }

    /// <summary>
    /// Random seed for reproducible data generation. If null, uses a random seed.
    /// </summary>
    public int? RandomSeed { get; set; }

    /// <summary>
    /// Number of additional columns to generate beyond the required columns.
    /// Default is 5.
    /// </summary>
    public int AdditionalColumnCount { get; set; } = 5;
}
