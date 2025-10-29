using KeeperData.Core.ETL.Impl;
using System.Collections.Immutable;

namespace KeeperData.Core.ETL.Abstract;

public interface IExternalCatalogueService
{
    /// <summary>
    /// Gets the file sets for a specific dataset for a specific date
    /// </summary>
    /// <param name="definition"></param>
    /// <param name="date"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<FileSet> GetFileSetAsync(DataSetDefinition definition, DateOnly date, CancellationToken ct);

    /// <summary>
    /// Gets the file sets for a specific dataset between two inclusive dates
    /// </summary>
    /// <param name="definition"></param>
    /// <param name="from"></param>
    /// <param name="to"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<FileSet> GetFileSetAsync(DataSetDefinition definition, DateOnly from, DateOnly to, CancellationToken ct);

    /// <summary>
    /// Gets the file sets for specific datasets for a specific date
    /// </summary>
    /// <param name="definitions"></param>
    /// <param name="date"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<ImmutableList<FileSet>> GetFileSetsAsync(ImmutableArray<DataSetDefinition> definitions, DateOnly date, CancellationToken ct);

    /// <summary>
    /// Gets the file sets for specific datasets between two inclusive dates
    /// </summary>
    /// <param name="definitions"></param>
    /// <param name="from"></param>
    /// <param name="to"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<ImmutableList<FileSet>> GetFileSetsAsync(ImmutableArray<DataSetDefinition> definitions, DateOnly from, DateOnly to, CancellationToken ct);

    /// <summary>
    /// Gets the file sets for a particular day
    /// </summary>
    /// <param name="date"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<ImmutableList<FileSet>> GetFileSetsAsync(DateOnly date, CancellationToken ct);

    /// <summary>
    /// Gets the file sets between two inclusive dates 
    /// </summary>
    /// <param name="from"></param>
    /// <param name="to"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<ImmutableList<FileSet>> GetFileSetsAsync(DateOnly from, DateOnly to, CancellationToken ct);

    /// <summary>
    /// Gets the file sets for last n-days including today
    /// </summary>
    /// <param name="days"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<ImmutableList<FileSet>> GetFileSetsAsync(int days, CancellationToken ct);

    /// <summary>
    /// Gets the file sets for today
    /// </summary>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<ImmutableList<FileSet>> GetFileSetsAsync(CancellationToken ct);
}