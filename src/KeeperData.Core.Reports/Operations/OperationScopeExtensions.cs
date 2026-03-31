namespace KeeperData.Core.Reports.Operations;

/// <summary>
/// Convenience extensions that wrap a delegate in the standard scope lifecycle:
/// try { work; Complete } catch (OCE) { Cancel; throw } catch { Fail; throw }.
/// </summary>
public static class OperationScopeExtensions
{
    /// <summary>
    /// Executes <paramref name="work"/> and finalises the scope automatically:
    /// completes on success, cancels on <see cref="OperationCanceledException"/>,
    /// fails on any other exception. Safe to call on a <c>null</c> scope.
    /// </summary>
    public static async Task RunAsync(this OperationScope? scope,
        Func<Task> work,
        string? cancelDescription = null,
        string? failDescription = null)
    {
        try
        {
            await work();
            scope?.Complete();
        }
        catch (OperationCanceledException)
        {
            scope?.Cancel(cancelDescription ?? "Cancelled");
            throw;
        }
        catch (Exception ex)
        {
            scope?.Fail(failDescription ?? ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Executes <paramref name="work"/>, returning its result, and finalises the
    /// scope automatically.
    /// </summary>
    public static async Task<T> RunAsync<T>(this OperationScope? scope,
        Func<Task<T>> work,
        string? cancelDescription = null,
        string? failDescription = null)
    {
        try
        {
            var result = await work();
            scope?.Complete();
            return result;
        }
        catch (OperationCanceledException)
        {
            scope?.Cancel(cancelDescription ?? "Cancelled");
            throw;
        }
        catch (Exception ex)
        {
            scope?.Fail(failDescription ?? ex.Message);
            throw;
        }
    }
}
