using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>Materialises one object, and removes it again if the write does not complete.
///
/// A write stream commits whatever it was given when it is disposed, and disposal happens on the way
/// out of a failure just as it does on success. So a stage that throws part-way through writing an
/// object still leaves that object behind, holding content that was never finished.
///
/// That matters here because every stage skips an object that already exists, which is what makes a
/// re-run cheap. A half-written object is therefore not retried: it is accepted as done, and the
/// failure resurfaces further down the pipeline as something that looks unrelated - a file whose
/// content makes no sense. Removing the object on the way out keeps "exists" meaning "finished".</summary>
public static class EtlArtefactWrite
{
    public static async Task<T> RunAsync<T>(
        IBlobStorageService storage,
        string objectKey,
        Func<Task<T>> write,
        ILogger logger)
    {
        try
        {
            return await write();
        }
        catch (Exception)
        {
            await RemovePartialAsync(storage, objectKey, logger);
            throw;
        }
    }

    public static Task RunAsync(
        IBlobStorageService storage,
        string objectKey,
        Func<Task> write,
        ILogger logger)
        => RunAsync<object?>(storage, objectKey, async () =>
        {
            await write();
            return null;
        }, logger);

    /// <summary>Best effort: the failure that brought us here is the one worth reporting, so a
    /// problem cleaning up is logged and swallowed rather than replacing it.</summary>
    private static async Task RemovePartialAsync(
        IBlobStorageService storage,
        string objectKey,
        ILogger logger)
    {
        try
        {
            // Deliberately not the caller's token: cleanup has to run even when the failure that
            // brought us here was the cancellation itself.
            await storage.DeleteAsync(objectKey, CancellationToken.None);

            logger.LogWarning(
                "Removed partially written {ObjectKey} after a failure, so a re-run does not skip it as already present",
                objectKey);
        }
        catch (Exception exception)
        {
            logger.LogError(
                exception,
                "Could not remove partially written {ObjectKey}; a re-run will skip it as already present and must be deleted by hand",
                objectKey);
        }
    }
}
