using System.Diagnostics;

namespace KeeperData.Core.Reports.Operations;

/// <summary>
/// Measures wall-clock elapsed time around a delegate and returns the result
/// together with the elapsed milliseconds, replacing manual Stopwatch ceremony.
/// </summary>
public static class Timed
{
    public static async Task<(T Result, long ElapsedMs)> RunAsync<T>(Func<Task<T>> work)
    {
        var sw = Stopwatch.StartNew();
        var result = await work();
        return (result, sw.ElapsedMilliseconds);
    }

    public static async Task<long> RunAsync(Func<Task> work)
    {
        var sw = Stopwatch.StartNew();
        await work();
        return sw.ElapsedMilliseconds;
    }

    public static long Run(Action work)
    {
        var sw = Stopwatch.StartNew();
        work();
        return sw.ElapsedMilliseconds;
    }
}
