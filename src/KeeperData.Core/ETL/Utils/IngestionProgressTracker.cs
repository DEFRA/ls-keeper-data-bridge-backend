using System.Diagnostics;
using KeeperData.Core.Reporting.Dtos;

namespace KeeperData.Core.ETL.Utils;

/// <summary>
/// Tracks and calculates progress metrics for file ingestion operations.
/// Uses exponential moving average for responsive rate calculations.
/// </summary>
public class IngestionProgressTracker
{
    private readonly string _fileName;
    private readonly int _estimatedTotalRows;
    private readonly Stopwatch _stopwatch;

    private int _rowsProcessed;
    private int _lastReportedRowCount;
    private double _exponentialMovingAverage;
    private DateTime _lastUpdateTime;

    // EMA smoothing factor (0.2 gives reasonable balance between responsiveness and stability)
    private const double Alpha = 0.2;
    private const int MinimumRowsForEstimate = 10;

    public IngestionProgressTracker(string fileName, int estimatedTotalRows)
    {
        _fileName = Path.GetFileName(fileName);
        _estimatedTotalRows = Math.Max(1, estimatedTotalRows); // Ensure at least 1 to avoid division by zero
        _stopwatch = Stopwatch.StartNew();
        _rowsProcessed = 0;
        _lastReportedRowCount = 0;
        _exponentialMovingAverage = 0;
        _lastUpdateTime = DateTime.UtcNow;
    }

    /// <summary>
    /// Updates the progress with the current number of rows processed.
    /// </summary>
    public void UpdateProgress(int rowsProcessed)
    {
        var currentTime = DateTime.UtcNow;
        var timeDeltaSeconds = (currentTime - _lastUpdateTime).TotalSeconds;
        var rowsDelta = rowsProcessed - _lastReportedRowCount;

        _rowsProcessed = rowsProcessed;

        // Calculate incremental rate based on rows processed since last update
        if (timeDeltaSeconds > 0 && rowsDelta > 0)
        {
            var currentRate = rowsDelta / timeDeltaSeconds;

            // Initialize or update exponential moving average
            if (_exponentialMovingAverage == 0)
            {
                _exponentialMovingAverage = currentRate;
            }
            else
            {
                _exponentialMovingAverage = (Alpha * currentRate) + ((1 - Alpha) * _exponentialMovingAverage);
            }
        }

        _lastReportedRowCount = rowsProcessed;
        _lastUpdateTime = currentTime;
    }

    /// <summary>
    /// Gets the current progress status for reporting.
    /// Handles cases where actual rows processed exceed the estimated count.
    /// </summary>
    public IngestionCurrentFileStatus GetCurrentStatus()
    {
        var rowsProcessed = _rowsProcessed;
        var elapsedSeconds = _stopwatch.Elapsed.TotalSeconds;

        // Calculate percentage - handle cases where actual rows exceed estimate
        var effectiveTotalRows = Math.Max(_estimatedTotalRows, rowsProcessed);
        var percentageCompleted = effectiveTotalRows > 0
            ? Math.Min(99, (int)((rowsProcessed * 100.0) / effectiveTotalRows))
            : 0;

        // If we've processed all estimated rows but haven't explicitly completed, cap at 99%
        if (rowsProcessed >= _estimatedTotalRows && !IsCompleted)
        {
            percentageCompleted = 99;
        }

        // Calculate rows per minute using exponential moving average
        decimal? rowsPerMinute = null;
        if (_exponentialMovingAverage > 0)
        {
            rowsPerMinute = (decimal)(_exponentialMovingAverage * 60);
        }

        // Calculate estimated time remaining
        TimeSpan? estimatedTimeRemaining = null;
        DateTime? estimatedCompletionUtc = null;

        if (_exponentialMovingAverage > 0 && rowsProcessed >= MinimumRowsForEstimate)
        {
            // Use the effective total rows for estimation to handle over-count
            var remainingRows = Math.Max(0, effectiveTotalRows - rowsProcessed);

            if (remainingRows > 0)
            {
                var remainingSeconds = remainingRows / _exponentialMovingAverage;
                estimatedTimeRemaining = TimeSpan.FromSeconds(remainingSeconds);
                estimatedCompletionUtc = DateTime.UtcNow.Add(estimatedTimeRemaining.Value);
            }
            else
            {
                // If we think we're done but haven't been marked complete
                estimatedTimeRemaining = TimeSpan.Zero;
                estimatedCompletionUtc = DateTime.UtcNow;
            }
        }

        return new IngestionCurrentFileStatus
        {
            FileName = _fileName,
            TotalRows = _estimatedTotalRows,
            RowNumber = rowsProcessed,
            PercentageCompleted = percentageCompleted,
            RowsPerMinute = rowsPerMinute,
            EstimatedTimeRemaining = estimatedTimeRemaining,
            EstimatedCompletionUtc = estimatedCompletionUtc
        };
    }

    /// <summary>
    /// Marks the tracking as completed and returns final status.
    /// </summary>
    public IngestionCurrentFileStatus Complete()
    {
        _stopwatch.Stop();
        IsCompleted = true;

        // Force 100% completion
        var finalStatus = GetCurrentStatus();
        return finalStatus with
        {
            PercentageCompleted = 100,
            EstimatedTimeRemaining = TimeSpan.Zero,
            EstimatedCompletionUtc = DateTime.UtcNow
        };
    }

    public bool IsCompleted { get; private set; }

    public TimeSpan ElapsedTime => _stopwatch.Elapsed;
}