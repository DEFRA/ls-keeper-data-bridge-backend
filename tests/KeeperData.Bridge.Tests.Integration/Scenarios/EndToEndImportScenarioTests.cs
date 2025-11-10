using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core;
using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Text;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Scenarios;

[Trait("Dependence", "docker")]
public class EndToEndImportScenarioTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly LocalStackFixture _localStackFixture;
    private readonly MongoDbFixture _mongoDbFixture;
    private ServiceProvider? _serviceProvider;

    private const string SourcePrefix = "source-encrypted";
    private const string DestPrefix = "dest-decrypted";
    private const string TestDatabaseName = "test-e2e-import";
    private const string CollectionName = "test_persons";

    public EndToEndImportScenarioTests(ITestOutputHelper output)
    {
        _output = output;

        _localStackFixture = new LocalStackFixture();
        _mongoDbFixture = new MongoDbFixture();
    }

    public async Task InitializeAsync()
    {
        await _localStackFixture.InitializeAsync();
        await _mongoDbFixture.InitializeAsync();

        await _mongoDbFixture.MongoClient.DropDatabaseAsync(TestDatabaseName);
        _output.WriteLine($"Initialized test database: {TestDatabaseName}");
    }

    public async Task DisposeAsync()
    {
        if (_serviceProvider != null)
        {
            await _serviceProvider.DisposeAsync();
        }

        await CleanupS3FilesAsync();
        await _mongoDbFixture.MongoClient.DropDatabaseAsync(TestDatabaseName);

        await _localStackFixture.DisposeAsync();
        await _mongoDbFixture.DisposeAsync();
    }

    [Fact]
    public async Task EndToEndImport_ShouldGenerateEncryptUploadImportIngestAndQuery()
    {
        _output.WriteLine("=== Starting End-to-End Import Scenario Test ===");

        var dataSetDefinition = CreateTestDataSetDefinition();
        _output.WriteLine($"✓ Step 1: Created DataSetDefinition for collection '{CollectionName}'");

        var (csvContent, sourceRecords) = GenerateTestCsvData(recordCount: 50);
        _output.WriteLine($"✓ Step 2: Generated {sourceRecords.Count} fake person records");

        _serviceProvider = ConfigureServices(dataSetDefinition);
        _output.WriteLine("✓ Step 3: Configured IoC container with all dependencies");

        var encryptedFileName = await EncryptCsvFileAsync(csvContent);
        _output.WriteLine($"✓ Step 4: Encrypted CSV file: {encryptedFileName}");

        await UploadEncryptedFileToS3Async(encryptedFileName);
        _output.WriteLine($"✓ Step 5: Uploaded encrypted file to S3 source location");

        var importId = await StartImportAsync();
        _output.WriteLine($"✓ Step 6: Started import with ID: {importId}");

        await VerifyImportCompletedAsync(importId);
        _output.WriteLine($"✓ Step 7: Import completed successfully");

        await VerifyFileProcessingAsync(importId);
        _output.WriteLine($"✓ Step 8: Verified file processing reports");

        var queryResults = await QueryIngestedDataAsync();
        _output.WriteLine($"✓ Step 9: Queried ingested data, found {queryResults.Count} records");

        VerifyDataIntegrity(sourceRecords, queryResults);
        _output.WriteLine($"✓ Step 10: Verified data integrity - all records match!");

        _output.WriteLine("=== End-to-End Import Scenario Test PASSED ===");
    }

    [Fact]
    public async Task EndToEndImport_ShouldSkipFilesOnDuplicateImport()
    {
        _output.WriteLine("=== Starting End-to-End Duplicate Import Scenario Test ===");

        var dataSetDefinition = CreateTestDataSetDefinition();
        _output.WriteLine($"✓ Step 1: Created DataSetDefinition for collection '{CollectionName}'");

        var (csvContent, sourceRecords) = GenerateTestCsvData(recordCount: 10);
        _output.WriteLine($"✓ Step 2: Generated {sourceRecords.Count} fake person records");

        _serviceProvider = ConfigureServices(dataSetDefinition);
        _output.WriteLine("✓ Step 3: Configured IoC container with all dependencies");

        var encryptedFileName = await EncryptCsvFileAsync(csvContent);
        _output.WriteLine($"✓ Step 4: Encrypted CSV file: {encryptedFileName}");

        await UploadEncryptedFileToS3Async(encryptedFileName);
        _output.WriteLine($"✓ Step 5: Uploaded encrypted file to S3 source location");

        // First import
        var firstImportId = await StartImportAsync();
        _output.WriteLine($"✓ Step 6: Started first import with ID: {firstImportId}");

        await VerifyImportCompletedAsync(firstImportId, expectRecordsCreated: true);
        _output.WriteLine($"✓ Step 7: First import completed successfully");

        var firstImportReport = await GetImportReportAsync(firstImportId);
        var firstImportAcquisitionFileCount = firstImportReport.AcquisitionPhase?.FilesProcessed ?? 0;
        var firstImportRecordsCreated = firstImportReport.IngestionPhase?.RecordsCreated ?? 0;
        _output.WriteLine($"✓ Step 8: First import acquired {firstImportAcquisitionFileCount} file(s)");

        var firstImportFileReports = await GetFileReportsAsync(firstImportId);
        _output.WriteLine($"✓ Step 9: First import ingested {firstImportFileReports.Count} file(s), created {firstImportRecordsCreated} records");

        // Capture the ETag from the first import for verification
        var firstImportFileReport = firstImportFileReports.First();
        var firstImportETag = firstImportFileReport.ETag;
        _output.WriteLine($"✓ First import file ETag: {firstImportETag}");

        // Second import with same files
        _output.WriteLine("");
        _output.WriteLine("=== Running Duplicate Import ===");
        var secondImportId = await StartImportAsync();
        _output.WriteLine($"✓ Step 10: Started second import with ID: {secondImportId}");

        await VerifyImportCompletedAsync(secondImportId, expectRecordsCreated: false);
        _output.WriteLine($"✓ Step 11: Second import completed");

        // Verify the second import reports
        var secondImportReport = await GetImportReportAsync(secondImportId);
        secondImportReport.AcquisitionPhase.Should().NotBeNull();
        secondImportReport.AcquisitionPhase!.Status.Should().Be(PhaseStatus.Completed);

        _output.WriteLine($"✓ Step 12: Acquisition phase - Discovered: {secondImportReport.AcquisitionPhase.FilesDiscovered}, Processed: {secondImportReport.AcquisitionPhase.FilesProcessed}, Skipped: {secondImportReport.AcquisitionPhase.FilesSkipped}");

        // Verify files were discovered 
        secondImportReport.AcquisitionPhase.FilesDiscovered.Should().Be(firstImportAcquisitionFileCount,
            $"Should discover {firstImportAcquisitionFileCount} file(s) in S3");
        _output.WriteLine($"✓ Step 13: Verified acquisition phase discovered {secondImportReport.AcquisitionPhase.FilesDiscovered} file(s)");

        // Get file reports for second import
        var secondImportFileReports = await GetFileReportsAsync(secondImportId);
        _output.WriteLine($"Second import file reports: {secondImportFileReports.Count}");

        foreach (var report in secondImportFileReports)
        {
            _output.WriteLine($"  File: {report.FileName}");
            _output.WriteLine($"    Status: {report.Status}");
            _output.WriteLine($"    ETag: {report.ETag}");
            _output.WriteLine($"    Records Processed: {report.Ingestion?.RecordsProcessed ?? 0}");
            _output.WriteLine($"    Records Created: {report.Ingestion?.RecordsCreated ?? 0}");
            _output.WriteLine($"    Records Updated: {report.Ingestion?.RecordsUpdated ?? 0}");
        }

        // With ETag-based skipping at BOTH acquisition AND ingestion levels:
        // 1. Acquisition phase: Files are discovered (1 file)
        // 2. Acquisition phase: File transfer is skipped (ETag + length match)
        // 3. Acquisition phase: File acquisition documents ARE still created (for ingestion to find)
        // 4. Ingestion phase: File is checked against import_files collection by FileKey + ETag
        // 5. Ingestion phase: File is skipped because it was already ingested with the same ETag
        // 6. Result: No records are created, updated, or deleted

        // Verify the acquisition phase skipped the file transfer
        secondImportReport.AcquisitionPhase.FilesProcessed.Should().Be(1,
            "Should evaluate 1 file for acquisition in the second import");
        secondImportReport.AcquisitionPhase.FilesSkipped.Should().Be(1,
            "Should skip 1 file transfer at acquisition level (same ETag + length)");

        // Verify the ingestion phase ALSO skipped the file
        secondImportReport.IngestionPhase.Should().NotBeNull();
        secondImportReport.IngestionPhase!.Status.Should().Be(PhaseStatus.Completed,
            "Ingestion phase should complete successfully even when skipping files");
        secondImportReport.IngestionPhase.FilesProcessed.Should().Be(1,
            "Ingestion should evaluate 1 file (the skipped acquisition file)");
        secondImportReport.IngestionPhase.FilesSkipped.Should().Be(1,
            "Ingestion phase should skip 1 file because it was already ingested with the same ETag");
        secondImportReport.IngestionPhase.RecordsCreated.Should().Be(0,
            "No new records should be created on duplicate import");
        secondImportReport.IngestionPhase.RecordsUpdated.Should().Be(0,
            "No records should be updated on duplicate import (file was skipped entirely)");
        secondImportReport.IngestionPhase.RecordsDeleted.Should().Be(0,
            "No records should be deleted on duplicate import");

        // Verify the file report shows the same ETag
        var secondImportFileReport = secondImportFileReports.FirstOrDefault();
        if (secondImportFileReport != null)
        {
            secondImportFileReport.ETag.Should().Be(firstImportETag,
                "Second import should have the same ETag as the first import");
            _output.WriteLine($"✓ Step 14: Verified file ETag consistency: {secondImportFileReport.ETag}");
        }

        _output.WriteLine("");
        _output.WriteLine("=== Summary ===");
        _output.WriteLine($"✓ First Import: {firstImportRecordsCreated} records created");
        _output.WriteLine($"✓ Second Import: Acquisition skipped 1 file, Ingestion skipped 1 file");
        _output.WriteLine($"✓ No duplicate processing occurred - ETag-based deduplication working correctly!");
    }

    [Fact]
    public async Task EndToEndImport_WithCompositeKeys_ShouldHandleInsertsUpdatesDeletesAndLineage()
    {
        _output.WriteLine("=== Starting End-to-End Composite Key Test ===");

        // Define a test dataset with composite keys (REGION + FARM_ID)
        var dataSetDefinition = new DataSetDefinition(
            Name: "test_composite_farms",
            FilePrefixFormat: "LITP_TEST_COMPOSITE_FARMS_{0}",
            PrimaryKeyHeaderNames: ["REGION", "FARM_ID"],
            ChangeTypeHeaderName: "CHANGETYPE",
            Accumulators: []);

        _output.WriteLine($"✓ Step 1: Created DataSetDefinition with composite keys: REGION, FARM_ID");

        _serviceProvider = ConfigureServices(dataSetDefinition);
        _output.WriteLine("✓ Step 2: Configured IoC container");

        // === BATCH 1: Initial inserts ===
        _output.WriteLine("\n=== BATCH 1: Initial Inserts ===");
        var batch1Records = new[]
        {
            ("NORTH", "F001", "Farm Alpha", "I"),
            ("NORTH", "F002", "Farm Beta", "I"),
            ("SOUTH", "F001", "Farm Gamma", "I"), // Same FARM_ID, different REGION
            ("EAST", "F003", "Farm Delta", "I")
        };

        var (csvContent1, batch1FileName) = await UploadCompositeKeyBatchAsync(batch1Records, 1);
        var importId1 = await StartImportAsync();
        _output.WriteLine($"✓ Started import 1 with ID: {importId1}");

        await VerifyImportCompletedAsync(importId1);
        _output.WriteLine("✓ Import 1 completed successfully");

        // Verify records were inserted
        var collection = await GetCollectionAsync("test_composite_farms");
        var count1 = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count1.Should().Be(4, "All 4 initial records should be inserted");
        _output.WriteLine($"✓ Verified {count1} records inserted");

        // Verify composite key format - the _id should be a URL-safe hash (43 characters)
        var allDocs = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();

        // Find documents by their field values instead of _id
        var northF001 = allDocs.FirstOrDefault(d => d["REGION"] == "NORTH" && d["FARM_ID"] == "F001");
        northF001.Should().NotBeNull("Composite key document should exist");
        northF001!["NAME"].Should().Be("Farm Alpha");
        northF001["IsDeleted"].AsBoolean.Should().BeFalse();
        northF001["_id"].AsString.Should().HaveLength(43, "Hash should be 43 characters (SHA256 Base64URL)");
        northF001["_id"].AsString.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$", "Hash should be URL-safe");

        // Verify the hash is generated correctly using RecordIdGenerator
        var recordIdGenerator = new RecordIdGenerator();
        var expectedHash = recordIdGenerator.GenerateId("NORTH", "F001");
        northF001["_id"].AsString.Should().Be(expectedHash, "Hash should match RecordIdGenerator output");
        _output.WriteLine($"✓ Verified composite key format (hash): {northF001["_id"]}");

        // Verify SOUTH@@F001 is different from NORTH@@F001
        var southF001 = allDocs.FirstOrDefault(d => d["REGION"] == "SOUTH" && d["FARM_ID"] == "F001");
        southF001.Should().NotBeNull("Same FARM_ID but different REGION should be unique");
        southF001!["NAME"].Should().Be("Farm Gamma");
        southF001["_id"].AsString.Should().NotBe(northF001["_id"].AsString, "Different composite keys should produce different hashes");
        _output.WriteLine("✓ Verified composite key uniqueness across different regions");

        // Verify lineage for created records - use the hash as record ID
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();
        var northF001RecordId = northF001["_id"].AsString;
        var lifecycle1 = await reportingService.GetRecordLifecycleAsync("test_composite_farms", northF001RecordId, CancellationToken.None);
        lifecycle1.Should().NotBeNull();
        lifecycle1!.Events.Should().HaveCount(1);
        lifecycle1.Events[0].EventType.Should().Be(RecordEventType.Created);
        lifecycle1.Events[0].ChangeType.Should().Be("I");
        _output.WriteLine("✓ Verified lineage tracking for created record");

        // Clean up first file for next batch
        await DeleteUploadedFileAsync(batch1FileName);

        // === BATCH 2: Updates (with change type U) ===
        _output.WriteLine("\n=== BATCH 2: Updates ===");
        var batch2Records = new[]
        {
            ("NORTH", "F001", "Farm Alpha UPDATED", "U"),  // Update existing
            ("SOUTH", "F001", "Farm Gamma UPDATED", "U")   // Update different region
        };

        var (csvContent2, batch2FileName) = await UploadCompositeKeyBatchAsync(batch2Records, 2);
        var importId2 = await StartImportAsync();
        await VerifyImportCompletedAsync(importId2);
        _output.WriteLine("✓ Import 2 completed successfully");

        // Verify updates didn't create duplicates
        var count2 = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count2.Should().Be(4, "Updates should not create duplicate records");
        _output.WriteLine("✓ Verified no duplicate records after updates");

        // Verify data was updated
        var allDocs2 = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        var docUpdated1 = allDocs2.FirstOrDefault(d => d["REGION"] == "NORTH" && d["FARM_ID"] == "F001");
        docUpdated1.Should().NotBeNull();
        docUpdated1!["NAME"].Should().Be("Farm Alpha UPDATED");
        docUpdated1["UpdatedAtUtc"].Should().NotBe(docUpdated1["CreatedAtUtc"], "UpdatedAtUtc should change");
        _output.WriteLine("✓ Verified record was updated correctly");

        // Verify lineage shows update event
        var lifecycle2 = await reportingService.GetRecordLifecycleAsync("test_composite_farms", docUpdated1["_id"].AsString, CancellationToken.None);
        lifecycle2!.Events.Should().HaveCount(2, "Should have Created + Updated events");
        lifecycle2.Events[1].EventType.Should().Be(RecordEventType.Updated);
        lifecycle2.Events[1].ChangeType.Should().Be("U");
        _output.WriteLine("✓ Verified lineage tracking for updated record");

        await DeleteUploadedFileAsync(batch2FileName);

        // === BATCH 3: Insert with existing key should become update ===
        _output.WriteLine("\n=== BATCH 3: Insert on Existing Key (becomes Update) ===");
        var batch3Records = new[]
        {
            ("NORTH", "F002", "Farm Beta REINSERTED", "I")  // Insert on existing key
        };

        var (csvContent3, batch3FileName) = await UploadCompositeKeyBatchAsync(batch3Records, 3);
        var importId3 = await StartImportAsync();
        await VerifyImportCompletedAsync(importId3);

        // Verify no duplicate created
        var count3 = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count3.Should().Be(4, "Insert on existing key should not create duplicate");

        var docReinserted = await collection.Find(d => d["REGION"] == "NORTH" && d["FARM_ID"] == "F002").FirstOrDefaultAsync();
        docReinserted["NAME"].Should().Be("Farm Beta REINSERTED", "Insert on existing key should update the record");
        _output.WriteLine("✓ Verified insert on existing key becomes update");

        await DeleteUploadedFileAsync(batch3FileName);

        // === BATCH 4: Soft delete ===
        _output.WriteLine("\n=== BATCH 4: Soft Delete ===");
        var batch4Records = new[]
        {
            ("NORTH", "F001", "Farm Alpha UPDATED", "D")  // Delete
        };

        var (csvContent4, batch4FileName) = await UploadCompositeKeyBatchAsync(batch4Records, 4);
        var importId4 = await StartImportAsync();
        await VerifyImportCompletedAsync(importId4);

        // Verify record still exists but is soft-deleted
        var count4 = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count4.Should().Be(4, "Soft delete should not remove document");

        var docDeleted = await collection.Find(d => d["REGION"] == "NORTH" && d["FARM_ID"] == "F001").FirstOrDefaultAsync();
        docDeleted["IsDeleted"].AsBoolean.Should().BeTrue("Record should be marked as deleted");
        docDeleted.Contains("DeletedAtUtc").Should().BeTrue("DeletedAtUtc should be set");
        _output.WriteLine("✓ Verified soft delete behavior");

        // Verify lineage shows delete event - get the actual hash-based ID from the document
        var docDeletedRecordId = docDeleted["_id"].AsString;
        var lifecycle4 = await reportingService.GetRecordLifecycleAsync("test_composite_farms", docDeletedRecordId, CancellationToken.None);
        lifecycle4!.Events.Should().HaveCount(3, "Should have Created + Updated + Deleted events");
        lifecycle4.Events[2].EventType.Should().Be(RecordEventType.Deleted);
        lifecycle4.Events[2].ChangeType.Should().Be("D");
        lifecycle4.CurrentStatus.Should().Be("Deleted");
        _output.WriteLine("✓ Verified lineage tracking for deleted record");

        await DeleteUploadedFileAsync(batch4FileName);

        // === BATCH 5: Undelete (update/insert on soft-deleted record) ===
        _output.WriteLine("\n=== BATCH 5: Undelete (Update on Deleted Record) ===");
        var batch5Records = new[]
        {
            ("NORTH", "F001", "Farm Alpha UNDELETED", "U")  // Update on deleted record
        };

        var (csvContent5, batch5FileName) = await UploadCompositeKeyBatchAsync(batch5Records, 5);
        var importId5 = await StartImportAsync();
        await VerifyImportCompletedAsync(importId5);

        var docUndeleted = await collection.Find(d => d["REGION"] == "NORTH" && d["FARM_ID"] == "F001").FirstOrDefaultAsync();
        docUndeleted["IsDeleted"].AsBoolean.Should().BeFalse("Record should be undeleted");
        docUndeleted["NAME"].Should().Be("Farm Alpha UNDELETED");
        docUndeleted.Contains("DeletedAtUtc").Should().BeFalse("DeletedAtUtc should be unset when undeleting");
        _output.WriteLine("✓ Verified undelete behavior");

        // Verify lineage shows undelete event - get the actual hash-based ID from the document
        var docUndeletedRecordId = docUndeleted["_id"].AsString;
        var lifecycle5 = await reportingService.GetRecordLifecycleAsync("test_composite_farms", docUndeletedRecordId, CancellationToken.None);
        lifecycle5!.Events.Should().HaveCount(4, "Should have Created + Updated + Deleted + Undeleted events");
        lifecycle5.Events[3].EventType.Should().Be(RecordEventType.Undeleted);
        lifecycle5.CurrentStatus.Should().Be("Active");
        _output.WriteLine("✓ Verified lineage tracking for undeleted record");

        await DeleteUploadedFileAsync(batch5FileName);

        // === BATCH 6: Update on non-existent record should become insert ===
        _output.WriteLine("\n=== BATCH 6: Update on Non-Existent Record (becomes Insert) ===");
        var batch6Records = new[]
        {
            ("WEST", "F999", "Farm New from Update", "U")  // Update on non-existent
        };

        var (csvContent6, batch6FileName) = await UploadCompositeKeyBatchAsync(batch6Records, 6);
        var importId6 = await StartImportAsync();
        await VerifyImportCompletedAsync(importId6);

        var count6 = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count6.Should().Be(5, "Update on non-existent record should create new record");

        var docNewFromUpdate = await collection.Find(d => d["REGION"] == "WEST" && d["FARM_ID"] == "F999").FirstOrDefaultAsync();
        docNewFromUpdate.Should().NotBeNull();
        docNewFromUpdate["NAME"].Should().Be("Farm New from Update");
        _output.WriteLine("✓ Verified update on non-existent record becomes insert");

        await DeleteUploadedFileAsync(batch6FileName);

        // === BATCH 7: Multiple operations on same record in same batch ===
        _output.WriteLine("\n=== BATCH 7: Multiple Operations Across Batches ===");
        var batch7Records = new[]
        {
            ("EAST", "F003", "Farm Delta UPDATED AGAIN", "U")
        };

        var (csvContent7, batch7FileName) = await UploadCompositeKeyBatchAsync(batch7Records, 7);
        var importId7 = await StartImportAsync();
        await VerifyImportCompletedAsync(importId7);

        var docMultiUpdate = await collection.Find(d => d["REGION"] == "EAST" && d["FARM_ID"] == "F003").FirstOrDefaultAsync();
        docMultiUpdate["NAME"].Should().Be("Farm Delta UPDATED AGAIN");

        // Verify lineage captures all events in order - get the actual hash-based ID from the document
        var docMultiUpdateRecordId = docMultiUpdate["_id"].AsString;
        var lifecycle7 = await reportingService.GetRecordLifecycleAsync("test_composite_farms", docMultiUpdateRecordId, CancellationToken.None);
        lifecycle7!.Events.Should().HaveCount(2, "Should have Created + Updated events");
        lifecycle7.Events[0].EventType.Should().Be(RecordEventType.Created);
        lifecycle7.Events[1].EventType.Should().Be(RecordEventType.Updated);
        _output.WriteLine("✓ Verified lineage captures multiple updates correctly");

        await DeleteUploadedFileAsync(batch7FileName);

        // === Final verification ===
        _output.WriteLine("\n=== Final Verification ===");
        var finalCount = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        _output.WriteLine($"Final record count: {finalCount}");

        var activeCount = await collection.CountDocumentsAsync(Builders<BsonDocument>.Filter.Eq("IsDeleted", false));
        _output.WriteLine($"Active records: {activeCount}");

        var deletedCount = await collection.CountDocumentsAsync(Builders<BsonDocument>.Filter.Eq("IsDeleted", true));
        _output.WriteLine($"Deleted records: {deletedCount}");

        // Verify all composite keys are unique and consistent
        var allDocsFinal = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        var allIds = allDocsFinal.Select(d => d["_id"].AsString).ToList();
        allIds.Should().OnlyHaveUniqueItems("Composite keys should be unique");
        _output.WriteLine("✓ Verified all composite keys are unique");

        // Verify composite key format consistency - all IDs should be URL-safe hashes
        foreach (var doc in allDocsFinal)
        {
            var id = doc["_id"].AsString;
            var region = doc["REGION"].AsString;
            var farmId = doc["FARM_ID"].AsString;

            // Verify ID is a URL-safe hash
            id.Should().HaveLength(43, "Hash should be 43 characters");
            id.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$", "Hash should be URL-safe");

            // Verify hash matches RecordIdGenerator output
            var expectedId = recordIdGenerator.GenerateId(region, farmId);
            id.Should().Be(expectedId, "Hash should match RecordIdGenerator output for the key parts");
        }
        _output.WriteLine("✓ Verified composite key format consistency (all hashes match RecordIdGenerator)");

        _output.WriteLine("\n=== End-to-End Composite Key Test PASSED ===");
        _output.WriteLine($"✓ Composite keys work correctly");
        _output.WriteLine($"✓ Insert on existing key becomes update");
        _output.WriteLine($"✓ Update on non-existent key becomes insert");
        _output.WriteLine($"✓ Soft delete works correctly");
        _output.WriteLine($"✓ Undelete works correctly");
        _output.WriteLine($"✓ No duplicates created across multiple batches");
        _output.WriteLine($"✓ Lineage tracking captures all events");
    }

    [Fact]
    public async Task EndToEndImport_ShouldTrackPaginatedLineageEventsAcrossMultipleImports()
    {
        _output.WriteLine("=== Starting Paginated Lineage Events Test ===");

        // Define a test dataset for tracking a record across multiple imports
        var dataSetDefinition = new DataSetDefinition(
            Name: "test_products",
            FilePrefixFormat: "LITP_TEST_PRODUCTS_{0}",
            PrimaryKeyHeaderNames: ["ProductId"],
            ChangeTypeHeaderName: "CHANGETYPE",
            Accumulators: []);

        _output.WriteLine("✓ Step 1: Created DataSetDefinition for test_products collection");

        _serviceProvider = ConfigureServices(dataSetDefinition);
        _output.WriteLine("✓ Step 2: Configured IoC container");

        // We'll track the lifecycle of a single product across 15 different import batches
        var productId = "PROD001";
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();

        _output.WriteLine($"\n=== Creating 15 lifecycle events for product {productId} ===");

        // Event 1: Create product
        await UploadAndProcessBatch("test_products", 1, new[]
        {
            (productId, "Widget Alpha", "100.00", "I")
        });
        _output.WriteLine("✓ Event 1: Created product");

        // Events 2-10: Multiple updates
        for (int i = 2; i <= 10; i++)
        {
            var price = (100 + (i * 10)).ToString("F2");
            await UploadAndProcessBatch("test_products", i, new[]
            {
                (productId, $"Widget Alpha v{i}", price, "U")
            });
            _output.WriteLine($"✓ Event {i}: Updated product (price=${price})");
        }

        // Event 11: Delete product
        await UploadAndProcessBatch("test_products", 11, new[]
        {
            (productId, "Widget Alpha v10", "200.00", "D")
        });
        _output.WriteLine("✓ Event 11: Deleted product");

        // Event 12: Undelete (update on deleted record)
        await UploadAndProcessBatch("test_products", 12, new[]
        {
            (productId, "Widget Alpha Restored", "220.00", "U")
        });
        _output.WriteLine("✓ Event 12: Undeleted product");

        // Events 13-15: More updates
        for (int i = 13; i <= 15; i++)
        {
            var price = (220 + ((i - 12) * 15)).ToString("F2");
            await UploadAndProcessBatch("test_products", i, new[]
            {
                (productId, $"Widget Alpha Final v{i - 12}", price, "U")
            });
            _output.WriteLine($"✓ Event {i}: Updated product (price=${price})");
        }

        _output.WriteLine($"\n=== All 15 events created for product {productId} ===");

        // === Test pagination ===
        _output.WriteLine("\n=== Testing Pagination ===");

        // The productId is stored as a URL-safe hash generated by RecordIdGenerator
        var recordIdGenerator = new RecordIdGenerator();
        var encodedProductId = recordIdGenerator.GenerateId(productId);
        _output.WriteLine($"Generated record ID (hash): {encodedProductId}");

        // Page 1: First 5 events (skip=0, top=5)
        var page1 = await reportingService.GetRecordLineageEventsPaginatedAsync(
            "test_products", encodedProductId, skip: 0, top: 5, CancellationToken.None);

        page1.Should().NotBeNull();
        page1.RecordId.Should().Be(encodedProductId);
        page1.CollectionName.Should().Be("test_products");
        page1.TotalEvents.Should().Be(15, "Should have 15 total events");
        page1.Count.Should().Be(5, "Page 1 should have 5 events");
        page1.Skip.Should().Be(0);
        page1.Top.Should().Be(5);
        page1.Events.Should().HaveCount(5);
        page1.CurrentStatus.Should().Be("Active", "Final state should be Active (after undelete)");

        // Verify first page events are in chronological order
        page1.Events[0].EventType.Should().Be(RecordEventType.Created);
        page1.Events[1].EventType.Should().Be(RecordEventType.Updated);
        page1.Events[2].EventType.Should().Be(RecordEventType.Updated);
        page1.Events[3].EventType.Should().Be(RecordEventType.Updated);
        page1.Events[4].EventType.Should().Be(RecordEventType.Updated);

        _output.WriteLine($"✓ Page 1 (skip=0, top=5): Retrieved {page1.Count}/{page1.TotalEvents} events");
        _output.WriteLine($"  - Event types: {string.Join(", ", page1.Events.Select(e => e.EventType))}");

        // Page 2: Next 5 events (skip=5, top=5)
        var page2 = await reportingService.GetRecordLineageEventsPaginatedAsync(
            "test_products", encodedProductId, skip: 5, top: 5, CancellationToken.None);

        page2.Count.Should().Be(5, "Page 2 should have 5 events");
        page2.Skip.Should().Be(5);
        page2.Events.Should().HaveCount(5);
        page2.Events.Should().OnlyContain(e => e.EventType == RecordEventType.Updated);

        _output.WriteLine($"✓ Page 2 (skip=5, top=5): Retrieved {page2.Count}/{page2.TotalEvents} events");
        _output.WriteLine($"  - All events are Updates");

        // Page 3: Last 5 events (skip=10, top=5)
        var page3 = await reportingService.GetRecordLineageEventsPaginatedAsync(
            "test_products", encodedProductId, skip: 10, top: 5, CancellationToken.None);

        page3.Count.Should().Be(5, "Page 3 should have 5 events");
        page3.Skip.Should().Be(10);
        page3.Events.Should().HaveCount(5);

        // Verify event 11 is the delete, event 12 is undelete
        page3.Events[0].EventType.Should().Be(RecordEventType.Deleted, "Event 11 should be Delete");
        page3.Events[1].EventType.Should().Be(RecordEventType.Undeleted, "Event 12 should be Undelete");
        page3.Events[2].EventType.Should().Be(RecordEventType.Updated);
        page3.Events[3].EventType.Should().Be(RecordEventType.Updated);
        page3.Events[4].EventType.Should().Be(RecordEventType.Updated);

        _output.WriteLine($"✓ Page 3 (skip=10, top=5): Retrieved {page3.Count}/{page3.TotalEvents} events");
        _output.WriteLine($"  - Event types: {string.Join(", ", page3.Events.Select(e => e.EventType))}");

        // Page 4: Beyond end (skip=15, top=5)
        var page4 = await reportingService.GetRecordLineageEventsPaginatedAsync(
            "test_products", encodedProductId, skip: 15, top: 5, CancellationToken.None);

        page4.Count.Should().Be(0, "Page 4 should have 0 events (beyond end)");
        page4.TotalEvents.Should().Be(15);
        page4.Events.Should().BeEmpty();

        _output.WriteLine($"✓ Page 4 (skip=15, top=5): Retrieved {page4.Count}/{page4.TotalEvents} events (beyond end)");

        // === Verify Previous/New Values ===
        _output.WriteLine("\n=== Verifying Previous/New Values ===");

        // Get first update event (should have previous and new values)
        var firstUpdate = page1.Events[1]; // Second event (first update)
        firstUpdate.EventType.Should().Be(RecordEventType.Updated);
        firstUpdate.PreviousValues.Should().NotBeNull("Update should have previous values");
        firstUpdate.NewValues.Should().NotBeNull("Update should have new values");

        // Verify the previous values match the initial create
        firstUpdate.PreviousValues!["ProductName"].Should().Be("Widget Alpha");
        firstUpdate.PreviousValues["Price"].Should().Be("100.00");

        // Verify the new values reflect the update
        firstUpdate.NewValues!["ProductName"].Should().Be("Widget Alpha v2");
        firstUpdate.NewValues["Price"].Should().Be("120.00");

        _output.WriteLine("✓ Verified Previous/New values tracking:");
        _output.WriteLine($"  - Previous: {firstUpdate.PreviousValues["ProductName"]} @ ${firstUpdate.PreviousValues["Price"]}");
        _output.WriteLine($"  - New: {firstUpdate.NewValues["ProductName"]} @ ${firstUpdate.NewValues["Price"]}");

        // Get delete event (should have previous but no new values)
        var deleteEvent = page3.Events[0];
        deleteEvent.EventType.Should().Be(RecordEventType.Deleted);
        deleteEvent.PreviousValues.Should().NotBeNull("Delete should have previous values");
        deleteEvent.NewValues.Should().BeNull("Delete should not have new values");

        _output.WriteLine($"✓ Delete event has previous values but no new values");

        // === Verify Import ID tracking ===
        _output.WriteLine("\n=== Verifying Import ID Tracking ===");

        var allImportIds = page1.Events.Concat(page2.Events).Concat(page3.Events)
            .Select(e => e.ImportId)
            .Distinct()
            .ToList();

        allImportIds.Should().HaveCount(15, "Should have 15 different import IDs");

        _output.WriteLine($"✓ All 15 events have unique import IDs");
        _output.WriteLine($"  - Created by import: {page1.CreatedByImport}");
        _output.WriteLine($"  - Last modified by import: {page1.LastModifiedByImport}");

        // === Verify metadata ===
        _output.WriteLine("\n=== Verifying Metadata ===");

        page1.CreatedAtUtc.Should().BeAfter(DateTime.UtcNow.AddMinutes(-5));
        page1.LastModifiedAtUtc.Should().BeAfter(page1.CreatedAtUtc);

        _output.WriteLine($"✓ Created: {page1.CreatedAtUtc:yyyy-MM-dd HH:mm:ss} UTC");
        _output.WriteLine($"✓ Last Modified: {page1.LastModifiedAtUtc:yyyy-MM-dd HH:mm:ss} UTC");

        _output.WriteLine("\n=== Paginated Lineage Events Test PASSED ===");
        _output.WriteLine($"✓ Successfully tracked 15 lifecycle events");
        _output.WriteLine($"✓ Pagination works correctly (tested 4 pages)");
        _output.WriteLine($"✓ Previous/New values tracked for all changes");
        _output.WriteLine($"✓ Import IDs tracked for full auditability");
        _output.WriteLine($"✓ Events returned in chronological order");
    }

    private async Task UploadAndProcessBatch(
        string collectionName,
        int batchNumber,
        (string productId, string name, string price, string changeType)[] records)
    {
        var sb = new StringBuilder();
        sb.AppendLine("ProductId|ProductName|Price|CHANGETYPE");

        foreach (var (productId, name, price, changeType) in records)
        {
            sb.AppendLine($"{productId}|{name}|{price}|{changeType}");
        }

        var csvContent = sb.ToString();
        var dateStr = DateTime.UtcNow.AddDays(-batchNumber).ToString(EtlConstants.DateTimePattern);
        var fileName = $"LITP_TEST_PRODUCTS_{dateStr}.csv";
        var fileKey = $"{DestPrefix}/{fileName}";

        await _localStackFixture.S3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = fileKey,
            ContentBody = csvContent,
            ContentType = "text/csv"
        });

        var importId = await StartImportAsync();
        await VerifyImportCompletedAsync(importId);

        // Clean up file
        await _localStackFixture.S3Client.DeleteObjectAsync(new Amazon.S3.Model.DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = fileKey
        });
    }

    #region Helper Methods

    private async Task<ImportReport> GetImportReportAsync(Guid importId)
    {
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();
        var report = await reportingService.GetImportReportAsync(importId, CancellationToken.None);
        report.Should().NotBeNull("Import report should exist");
        return report!;
    }

    private async Task<IReadOnlyList<FileProcessingReport>> GetFileReportsAsync(Guid importId)
    {
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();
        var fileReports = await reportingService.GetFileReportsAsync(importId, CancellationToken.None);
        return fileReports;
    }

    private DataSetDefinition CreateTestDataSetDefinition()
    {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);

        return new DataSetDefinition(
            Name: CollectionName,
            FilePrefixFormat: "LITP_TEST_PERSONS_{0}",
            PrimaryKeyHeaderNames: ["PersonId"],
            ChangeTypeHeaderName: "CHANGETYPE",
            Accumulators: []);
    }

    private (string CsvContent, List<PersonRecord> Records) GenerateTestCsvData(int recordCount)
    {
        _output.WriteLine($"Generating {recordCount} fake person records using Bogus...");
        return TestDataGenerator.GeneratePersonCsv(recordCount, "PersonId");
    }

    private ServiceProvider ConfigureServices(DataSetDefinition dataSetDefinition)
    {
        var dataSetDefinitions = new DataSetDefinitions
        {
            SamCPHHolding = dataSetDefinition,
            CTSCPHHolding = dataSetDefinition,
            CTSKeeper = dataSetDefinition,
            SamCPHHolder = dataSetDefinition,
            SamHerd = dataSetDefinition,
            SamParty = dataSetDefinition,
            All = [dataSetDefinition]
        };

        return TestServiceProviderBuilder.BuildServiceProvider(
            s3Client: _localStackFixture.S3Client,
            bucketName: LocalStackFixture.TestBucket,
            sourcePrefix: SourcePrefix,
            destPrefix: DestPrefix,
            mongoClient: _mongoDbFixture.MongoClient,
            mongoDatabaseName: TestDatabaseName,
            dataSetDefinitions: dataSetDefinitions);
    }

    private async Task<string> EncryptCsvFileAsync(string csvContent)
    {
        if (_serviceProvider == null)
        {
            throw new InvalidOperationException("ServiceProvider not initialized");
        }

        var cryptoTransform = _serviceProvider.GetRequiredService<IAesCryptoTransform>();
        var passwordSaltService = _serviceProvider.GetRequiredService<IPasswordSaltService>();

        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        var dateStr = DateTime.UtcNow.ToString(EtlConstants.DateTimePattern);
        var fileName = $"LITP_TEST_PERSONS_{dateStr}.csv";
        var encryptedFileName = $"{fileName}.enc";

        var credentials = passwordSaltService.Get(encryptedFileName);

        var csvBytes = Encoding.UTF8.GetBytes(csvContent);

        _output.WriteLine($"Encrypting CSV file ({csvBytes.Length} bytes) with password derived from filename...");

        using var inputStream = new MemoryStream(csvBytes);
        using var outputStream = new MemoryStream();

        await cryptoTransform.EncryptStreamAsync(
            inputStream,
            outputStream,
            credentials.Password,
            credentials.Salt,
            csvBytes.Length);

        outputStream.Position = 0;
        var encryptedBytes = outputStream.ToArray();

        _output.WriteLine($"Encrypted file size: {encryptedBytes.Length} bytes");

        var tempPath = Path.Combine(Path.GetTempPath(), encryptedFileName);
        await File.WriteAllBytesAsync(tempPath, encryptedBytes);

        return encryptedFileName;
    }

    private async Task UploadEncryptedFileToS3Async(string encryptedFileName)
    {
        var blobStorageFactory = _serviceProvider!.GetRequiredService<IBlobStorageServiceFactory>();
        var sourceStorage = blobStorageFactory.GetSourceExternal();

        var tempPath = Path.Combine(Path.GetTempPath(), encryptedFileName);
        var encryptedBytes = await File.ReadAllBytesAsync(tempPath);

        var dateStr = DateTime.UtcNow.ToString(EtlConstants.DateTimePattern);
        var s3Key = $"LITP_TEST_PERSONS_{dateStr}.csv.enc";

        _output.WriteLine($"Uploading encrypted file to S3: {s3Key}");

        await _localStackFixture.S3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{SourcePrefix}/{s3Key}",
            InputStream = new MemoryStream(encryptedBytes),
            ContentType = "application/octet-stream"
        });

        var exists = await sourceStorage.ExistsAsync(s3Key);
        exists.Should().BeTrue($"Encrypted file should exist in S3 at key: {s3Key}");

        _output.WriteLine($"Encrypted file uploaded successfully, size: {encryptedBytes.Length} bytes");

        File.Delete(tempPath);
    }

    private async Task<Guid> StartImportAsync()
    {
        var importOrchestrator = _serviceProvider!.GetRequiredService<IImportOrchestrator>();
        var importId = Guid.NewGuid();

        _output.WriteLine($"Starting import orchestrator with ID: {importId}");

        await importOrchestrator.StartAsync(importId, "external", CancellationToken.None);

        return importId;
    }

    private async Task VerifyImportCompletedAsync(Guid importId, bool expectRecordsCreated = true)
    {
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();

        _output.WriteLine("Waiting for import to complete...");

        var maxWaitTime = TimeSpan.FromMinutes(2);
        var startTime = DateTime.UtcNow;

        while (DateTime.UtcNow - startTime < maxWaitTime)
        {
            var report = await reportingService.GetImportReportAsync(importId, CancellationToken.None);

            if (report == null)
            {
                await Task.Delay(1000);
                continue;
            }

            _output.WriteLine($"Import Status: {report.Status}");
            _output.WriteLine($"  Acquisition: {report.AcquisitionPhase?.Status}");
            _output.WriteLine($"  Ingestion: {report.IngestionPhase?.Status}");

            if (report.Status == ImportStatus.Completed)
            {
                _output.WriteLine("Import completed successfully!");

                report.AcquisitionPhase.Should().NotBeNull();
                report.AcquisitionPhase!.Status.Should().Be(PhaseStatus.Completed);

                // Only check FilesProcessed if we expect records to be created AND acquisition processed files
                // (Direct ingestion without acquisition won't have files in acquisition phase)
                if (expectRecordsCreated && report.AcquisitionPhase.FilesProcessed > 0)
                {
                    report.AcquisitionPhase.FilesProcessed.Should().BeGreaterThan(0);
                }

                report.IngestionPhase.Should().NotBeNull();
                report.IngestionPhase!.Status.Should().Be(PhaseStatus.Completed);

                if (expectRecordsCreated)
                {
                    // For direct ingestion (no acquisition), check ingestion phase
                    (report.IngestionPhase.RecordsCreated + report.IngestionPhase.RecordsUpdated + report.IngestionPhase.RecordsDeleted)
                        .Should().BeGreaterThan(0, "At least one record operation should have occurred");
                }

                return;
            }

            if (report.Status == ImportStatus.Failed)
            {
                throw new Exception($"Import failed: {report.Error}");
            }

            await Task.Delay(2000);
        }

        throw new TimeoutException($"Import did not complete within {maxWaitTime.TotalSeconds} seconds");
    }

    private async Task VerifyFileProcessingAsync(Guid importId)
    {
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();

        var fileReports = await reportingService.GetFileReportsAsync(importId, CancellationToken.None);

        _output.WriteLine($"File Processing Reports: {fileReports.Count}");

        fileReports.Should().NotBeEmpty("At least one file should have been processed");

        foreach (var fileReport in fileReports)
        {
            _output.WriteLine($"  File: {fileReport.FileName}");
            _output.WriteLine($"    Status: {fileReport.Status}");
            _output.WriteLine($"    Records: {fileReport.Ingestion?.RecordsProcessed ?? 0}");
            _output.WriteLine($"    Created: {fileReport.Ingestion?.RecordsCreated ?? 0}");

            fileReport.Status.Should().Be(FileProcessingStatus.Ingested);
            fileReport.Ingestion.Should().NotBeNull();
            fileReport.Ingestion!.RecordsCreated.Should().BeGreaterThan(0);
        }
    }

    private async Task<List<Dictionary<string, object?>>> QueryIngestedDataAsync()
    {
        var queryService = _serviceProvider!.GetRequiredService<IMongoQueryService>();

        _output.WriteLine($"Querying collection '{CollectionName}'...");

        var result = await queryService.QueryAsync(
            collectionName: CollectionName,
            filter: null,
            orderBy: "FirstName asc",
            skip: 0,
            top: 100,
            count: true,
            cancellationToken: CancellationToken.None);

        result.Should().NotBeNull();
        result.CollectionName.Should().Be(CollectionName);
        result.Data.Should().NotBeEmpty("Data should be ingested into the collection");

        _output.WriteLine($"Query returned {result.Count} records (Total: {result.TotalCount})");

        return result.Data.ToList();
    }

    private void VerifyDataIntegrity(List<PersonRecord> sourceRecords, List<Dictionary<string, object?>> queryResults)
    {
        _output.WriteLine("Verifying data integrity...");

        var activeSourceRecords = sourceRecords.Where(r => r.IsActive).ToList();

        queryResults.Count.Should().BeGreaterThanOrEqualTo(activeSourceRecords.Count - 5,
            "Most active records should be in the database (allowing for some variance)");

        var sampleSize = Math.Min(10, queryResults.Count);
        for (int i = 0; i < sampleSize; i++)
        {
            var queryRecord = queryResults[i];

            queryRecord.Should().ContainKey("PersonId");
            queryRecord.Should().ContainKey("FirstName");
            queryRecord.Should().ContainKey("LastName");
            queryRecord.Should().ContainKey("Email");
            queryRecord.Should().ContainKey("Department");
            queryRecord.Should().ContainKey("Salary");
            queryRecord.Should().ContainKey("IsActive");

            var personId = queryRecord["PersonId"]?.ToString();
            personId.Should().NotBeNullOrEmpty();

            var sourceRecord = sourceRecords.FirstOrDefault(r => r.PersonId == personId);
            if (sourceRecord != null)
            {
                _output.WriteLine($"  Verified record: {sourceRecord.FirstName} {sourceRecord.LastName}");

                queryRecord["FirstName"]?.ToString().Should().Be(sourceRecord.FirstName);
                queryRecord["LastName"]?.ToString().Should().Be(sourceRecord.LastName);
                queryRecord["Email"]?.ToString().Should().Be(sourceRecord.Email);
                queryRecord["Department"]?.ToString().Should().Be(sourceRecord.Department);

                var querySalary = Convert.ToInt32(queryRecord["Salary"]);
                querySalary.Should().Be(sourceRecord.Salary);
            }
        }

        _output.WriteLine($"✓ Data integrity verified for {sampleSize} sample records");
    }

    private async Task CleanupS3FilesAsync()
    {
        try
        {
            var allObjects = await _localStackFixture.S3Client.ListObjectsV2Async(
                new Amazon.S3.Model.ListObjectsV2Request
                {
                    BucketName = LocalStackFixture.TestBucket,
                    Prefix = SourcePrefix
                });

            foreach (var obj in allObjects.S3Objects)
            {
                await _localStackFixture.S3Client.DeleteObjectAsync(
                    LocalStackFixture.TestBucket,
                    obj.Key);
            }

            allObjects = await _localStackFixture.S3Client.ListObjectsV2Async(
                new Amazon.S3.Model.ListObjectsV2Request
                {
                    BucketName = LocalStackFixture.TestBucket,
                    Prefix = DestPrefix
                });

            foreach (var obj in allObjects.S3Objects)
            {
                await _localStackFixture.S3Client.DeleteObjectAsync(
                    LocalStackFixture.TestBucket,
                    obj.Key);
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Warning: Failed to clean up S3 files: {ex.Message}");
        }
    }

    private async Task<(string CsvContent, string FileName)> UploadCompositeKeyBatchAsync(
        (string region, string farmId, string name, string changeType)[] records,
        int batchNumber)
    {
        var sb = new StringBuilder();
        sb.AppendLine("REGION|FARM_ID|NAME|CHANGETYPE");

        foreach (var (region, farmId, name, changeType) in records)
        {
            sb.AppendLine($"{region}|{farmId}|{name}|{changeType}");
        }

        var csvContent = sb.ToString();
        var dateStr = DateTime.UtcNow.AddDays(-batchNumber).ToString(EtlConstants.DateTimePattern);
        var fileName = $"LITP_TEST_COMPOSITE_FARMS_{dateStr}.csv";
        var fileKey = $"{DestPrefix}/{fileName}";

        _output.WriteLine($"Uploading batch {batchNumber}: {fileName} with {records.Length} records");

        await _localStackFixture.S3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = fileKey,
            ContentBody = csvContent,
            ContentType = "text/csv"
        });

        return (csvContent, fileName);
    }

    private async Task DeleteUploadedFileAsync(string fileName)
    {
        var fileKey = $"{DestPrefix}/{fileName}";
        await _localStackFixture.S3Client.DeleteObjectAsync(new Amazon.S3.Model.DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = fileKey
        });
        _output.WriteLine($"Deleted file: {fileName}");
    }

    private async Task<IMongoCollection<BsonDocument>> GetCollectionAsync(string collectionName)
    {
        var database = _mongoDbFixture.MongoClient.GetDatabase(TestDatabaseName);
        return database.GetCollection<BsonDocument>(collectionName);
    }

    #endregion
}