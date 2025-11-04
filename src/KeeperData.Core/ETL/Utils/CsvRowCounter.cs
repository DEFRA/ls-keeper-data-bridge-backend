using Microsoft.Extensions.Logging;

namespace KeeperData.Core.ETL.Utils;

/// <summary>
/// Utility class for counting rows in CSV files efficiently.
/// </summary>
public class CsvRowCounter
{
    private readonly ILogger<CsvRowCounter> _logger;
    private const int BufferSize = 65536; // 64KB buffer for efficient reading
    private const int MaxRetryAttempts = 3;
    private const int RetryDelayMs = 100;

    public CsvRowCounter(ILogger<CsvRowCounter> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Counts the approximate number of data rows in a CSV file (excluding header).
    /// This is a fast approximation that counts newlines and may not be 100% accurate
    /// for CSVs with quoted fields containing newlines.
    /// </summary>
    public async Task<int> CountRowsAsync(string filePath, CancellationToken ct)
    {
        for (int attempt = 1; attempt <= MaxRetryAttempts; attempt++)
        {
            try
            {
                return await CountRowsInternalAsync(filePath, ct);
            }
            catch (Exception ex) when (attempt < MaxRetryAttempts)
            {
                _logger.LogWarning(ex, "Failed to count rows in file {FilePath} on attempt {Attempt}/{MaxAttempts}. Retrying...",
                    filePath, attempt, MaxRetryAttempts);

                await Task.Delay(RetryDelayMs * attempt, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to count rows in file {FilePath} after {MaxAttempts} attempts",
                    filePath, MaxRetryAttempts);
                throw;
            }
        }

        // This should never be reached due to the throw in the catch block
        return 0;
    }

    private async Task<int> CountRowsInternalAsync(string filePath, CancellationToken ct)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"CSV file not found: {filePath}");
        }

        var fileInfo = new FileInfo(filePath);

        // Handle empty files
        if (fileInfo.Length == 0)
        {
            _logger.LogDebug("File {FilePath} is empty, returning 0 rows", filePath);
            return 0;
        }

        await using var fileStream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            BufferSize,
            useAsync: true);

        var buffer = new byte[BufferSize];
        var rowCount = 0;
        var bytesRead = 0;
        var lastCharWasNewline = false;

        while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length, ct)) > 0)
        {
            for (int i = 0; i < bytesRead; i++)
            {
                if (buffer[i] == '\n')
                {
                    rowCount++;
                    lastCharWasNewline = true;
                }
                else if (buffer[i] == '\r')
                {
                    // Handle Windows line endings (\r\n)
                    // Don't double-count if next char is \n
                    if (i + 1 < bytesRead && buffer[i + 1] == '\n')
                    {
                        i++; // Skip the \n
                    }
                    rowCount++;
                    lastCharWasNewline = true;
                }
                else
                {
                    lastCharWasNewline = false;
                }
            }
        }

        // If file doesn't end with newline, we still have a row
        if (!lastCharWasNewline && fileInfo.Length > 0)
        {
            rowCount++;
        }

        // Subtract 1 for header row (if file has at least one row)
        var dataRows = Math.Max(0, rowCount - 1);

        _logger.LogDebug("Counted approximately {RowCount} data rows in file {FilePath} (excluding header)",
            dataRows, filePath);

        return dataRows;
    }
}