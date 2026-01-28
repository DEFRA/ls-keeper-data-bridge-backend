using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Database;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Impl;

/// <summary>
/// MongoDB implementation of the cleanse analysis repository.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class CleanseAnalysisRepository : ICleanseAnalysisRepository
{
    private const string CollectionName = "cleanse_analysis_operations";
    private readonly IMongoCollection<BsonDocument> _collection;

    public CleanseAnalysisRepository(IMongoClient mongoClient, IOptions<IDatabaseConfig> databaseConfig)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        _collection = database.GetCollection<BsonDocument>(CollectionName);
    }

    public async Task CreateOperationAsync(CleanseAnalysisOperation operation, CancellationToken ct = default)
    {
        var document = MapToDocument(operation);
        await _collection.InsertOneAsync(document, cancellationToken: ct);
    }

    public async Task UpdateProgressAsync(
        string operationId,
        double progressPercentage,
        string statusDescription,
        int recordsAnalyzed,
        int issuesFound,
        int issuesResolved,
        CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", operationId);
        var update = Builders<BsonDocument>.Update
            .Set("progress_percentage", progressPercentage)
            .Set("status_description", statusDescription)
            .Set("records_analyzed", recordsAnalyzed)
            .Set("issues_found", issuesFound)
            .Set("issues_resolved", issuesResolved);
        await _collection.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    public async Task CompleteOperationAsync(
        string operationId,
        int recordsAnalyzed,
        int issuesFound,
        int issuesResolved,
        long durationMs,
        CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", operationId);
        var update = Builders<BsonDocument>.Update
            .Set("status", CleanseAnalysisStatus.Completed.ToString())
            .Set("completed_at_utc", DateTime.UtcNow)
            .Set("progress_percentage", 100.0)
            .Set("status_description", "Analysis completed")
            .Set("records_analyzed", recordsAnalyzed)
            .Set("issues_found", issuesFound)
            .Set("issues_resolved", issuesResolved)
            .Set("duration_ms", durationMs);
        await _collection.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    public async Task FailOperationAsync(string operationId, string error, long durationMs, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", operationId);
        var update = Builders<BsonDocument>.Update
            .Set("status", CleanseAnalysisStatus.Failed.ToString())
            .Set("completed_at_utc", DateTime.UtcNow)
            .Set("status_description", "Analysis failed")
            .Set("error", error)
            .Set("duration_ms", durationMs);
        await _collection.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    public async Task<CleanseAnalysisOperation?> GetOperationAsync(string operationId, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", operationId);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document is null ? null : MapToEntity(document);
    }

    public async Task<IReadOnlyList<CleanseAnalysisOperationSummary>> GetOperationsAsync(int skip, int top, CancellationToken ct = default)
    {
        var documents = await _collection
            .Find(Builders<BsonDocument>.Filter.Empty)
            .SortByDescending(d => d["started_at_utc"])
            .Skip(skip)
            .Limit(top)
            .ToListAsync(ct);
        return documents.Select(MapToSummary).ToList();
    }




    public async Task<CleanseAnalysisOperation?> GetCurrentOperationAsync(CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("status", CleanseAnalysisStatus.Running.ToString());
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document is null ? null : MapToEntity(document);
    }

    public async Task SetReportDetailsAsync(string operationId, string objectKey, string reportUrl, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", operationId);
        var update = Builders<BsonDocument>.Update
            .Set("report_object_key", objectKey)
            .Set("report_url", reportUrl);
        await _collection.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    public async Task UpdateReportUrlAsync(string operationId, string reportUrl, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", operationId);
        var update = Builders<BsonDocument>.Update.Set("report_url", reportUrl);
        await _collection.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    public async Task<long> DeleteAllAsync(CancellationToken ct = default)
    {
        var result = await _collection.DeleteManyAsync(Builders<BsonDocument>.Filter.Empty, ct);
        return result.DeletedCount;
    }

    private static BsonDocument MapToDocument(CleanseAnalysisOperation operation) => new()
    {
        { "_id", operation.Id },
        { "status", operation.Status.ToString() },
        { "started_at_utc", operation.StartedAtUtc },
        { "completed_at_utc", operation.CompletedAtUtc.HasValue ? operation.CompletedAtUtc.Value : BsonNull.Value },
        { "progress_percentage", operation.ProgressPercentage },
        { "status_description", operation.StatusDescription },
        { "records_analyzed", operation.RecordsAnalyzed },
        { "total_records", operation.TotalRecords },
        { "issues_found", operation.IssuesFound },
        { "issues_resolved", operation.IssuesResolved },
        { "error", operation.Error ?? (BsonValue)BsonNull.Value },
        { "duration_ms", operation.DurationMs.HasValue ? operation.DurationMs.Value : BsonNull.Value },
        { "report_object_key", operation.ReportObjectKey ?? (BsonValue)BsonNull.Value },
        { "report_url", operation.ReportUrl ?? (BsonValue)BsonNull.Value }
    };

    private static CleanseAnalysisOperation MapToEntity(BsonDocument document) => new()
    {
        Id = document["_id"].AsString,
        Status = Enum.Parse<CleanseAnalysisStatus>(document["status"].AsString),
        StartedAtUtc = document["started_at_utc"].ToUniversalTime(),
        CompletedAtUtc = document["completed_at_utc"].IsBsonNull ? null : document["completed_at_utc"].ToUniversalTime(),
        ProgressPercentage = document["progress_percentage"].AsDouble,
        StatusDescription = document["status_description"].AsString,
        RecordsAnalyzed = document["records_analyzed"].AsInt32,
        TotalRecords = document["total_records"].AsInt32,
        IssuesFound = document["issues_found"].AsInt32,
        IssuesResolved = document["issues_resolved"].AsInt32,
        Error = document["error"].IsBsonNull ? null : document["error"].AsString,
        DurationMs = document["duration_ms"].IsBsonNull ? null : document["duration_ms"].AsInt64,
        ReportObjectKey = document.Contains("report_object_key") && !document["report_object_key"].IsBsonNull ? document["report_object_key"].AsString : null,
        ReportUrl = document.Contains("report_url") && !document["report_url"].IsBsonNull ? document["report_url"].AsString : null
    };

    private static CleanseAnalysisOperationSummary MapToSummary(BsonDocument document) => new()
    {
        Id = document["_id"].AsString,
        Status = document["status"].AsString,
        StartedAtUtc = document["started_at_utc"].ToUniversalTime(),
        CompletedAtUtc = document["completed_at_utc"].IsBsonNull ? null : document["completed_at_utc"].ToUniversalTime(),
        ProgressPercentage = document["progress_percentage"].AsDouble,
        RecordsAnalyzed = document["records_analyzed"].AsInt32,
        IssuesFound = document["issues_found"].AsInt32,
        IssuesResolved = document["issues_resolved"].AsInt32,
        DurationMs = document["duration_ms"].IsBsonNull ? null : document["duration_ms"].AsInt64,
        ReportObjectKey = document.Contains("report_object_key") && !document["report_object_key"].IsBsonNull ? document["report_object_key"].AsString : null,
        ReportUrl = document.Contains("report_url") && !document["report_url"].IsBsonNull ? document["report_url"].AsString : null
    };
}
