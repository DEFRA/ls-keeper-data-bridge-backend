using Amazon.S3;
using CsvHelper;
using FluentAssertions;
using KeeperData.Core;
using KeeperData.Core.Crypto;
using KeeperData.Core.Database;
using KeeperData.Core.Database.Configuration;
using KeeperData.Core.Database.Resilience;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Locking;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Impl;
using KeeperData.Core.Reporting.Services;
using KeeperData.Core.Reports;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Issues.Query.Abstract;
using KeeperData.Core.Reports.Issues.Query.Dtos;
using KeeperData.Core.Reports.SamCtsHoldings.Query;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Storage;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Locking;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Moq;
using System.Globalization;
using System.IO.Compression;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.CleanseReporting;

[Collection("LocalStackAndMongo"), Trait("Dependence", "docker")]
public class CleanseAnalysisEndToEndTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly LocalStackFixture _localStackFixture;
    private readonly MongoDbFixture _mongoDbFixture;
    private ServiceProvider? _serviceProvider;

    private const string SourcePrefix = "source-encrypted";
    private const string DestPrefix = "dest-decrypted";
    private const string TestDatabaseName = "test-cleanse-analysis";

    private readonly DataSetDefinitions _dataSetDefinitions;
    private readonly Mock<ICleanseReportNotificationService> _mockNotificationService = new();

    public CleanseAnalysisEndToEndTests(
        ITestOutputHelper output,
        LocalStackFixture localStackFixture,
        MongoDbFixture mongoDbFixture)
    {
        _output = output;
        _localStackFixture = localStackFixture;
        _mongoDbFixture = mongoDbFixture;
        _dataSetDefinitions = StandardDataSetDefinitionsBuilder.Build();
    }

    public async Task InitializeAsync()
    {
        await _mongoDbFixture.MongoClient.DropDatabaseAsync(TestDatabaseName);

        _serviceProvider = BuildServiceProvider();

        // Run one-time initialisation (e.g. MongoDB indexes)
        var facade = _serviceProvider.GetRequiredService<ICleanseFacade>();
        await facade.Initialisation.InitialiseAsync();

        _output.WriteLine($"Initialized test database: {TestDatabaseName}");
    }

    public async Task DisposeAsync()
    {
        if (_serviceProvider != null)
            await _serviceProvider.DisposeAsync();

        await _mongoDbFixture.MongoClient.DropDatabaseAsync(TestDatabaseName);
    }

    [Fact]
    public async Task CleanseAnalysis_HappyPath_ShouldDetectIssuesFixDataAndReRun()
    {
        _output.WriteLine("=== Starting Cleanse Analysis Happy-Path End-to-End Test ===");
        var ct = CancellationToken.None;

        // ----------------------------------------------------------------
        // Step 1: Ingest test data that activates cleanse rules
        // ----------------------------------------------------------------
        _output.WriteLine("\n--- Step 1: Ingesting test data ---");
        await IngestInitialTestDataAsync();
        _output.WriteLine("✓ Step 1 complete: test data ingested");

        // ----------------------------------------------------------------
        // Step 2: Run first cleanse analysis
        // ----------------------------------------------------------------
        _output.WriteLine("\n--- Step 2: Running first cleanse analysis ---");
        var analysisService = _serviceProvider!.GetRequiredService<ICleanseAnalysisCommandService>();
        var firstResult = await analysisService.RunAnalysisAsync(ct);

        firstResult.Should().NotBeNull("First analysis should complete successfully");
        firstResult!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        _output.WriteLine($"✓ Step 2 complete: first analysis finished (records={firstResult.RecordsAnalyzed}, issues={firstResult.IssuesFound})");

        // ----------------------------------------------------------------
        // Step 3: Verify issues created
        // ----------------------------------------------------------------
        _output.WriteLine("\n--- Step 3: Verifying issues ---");
        var issueQueries = _serviceProvider!.GetRequiredService<IIssueQueries>();
        var activeIssueCount = await issueQueries.GetActiveIssuesCountAsync(ct);
        var activeIssues = await issueQueries.GetActiveIssuesAsync(0, 100, ct);

        _output.WriteLine($"Active issues: {activeIssueCount}");
        foreach (var issue in activeIssues)
        {
            _output.WriteLine($"  [{issue.IssueCode}] CPH={issue.Cph} CTS={issue.CtsLidFullIdentifier}");
        }

        // Scenario 1: CPH 12/345/6001 in CTS only → Rule 2A
        activeIssues.Should().Contain(i => i.IssueCode == RuleIds.CTS_CPH_NOT_IN_SAM && i.Cph == "12/345/6001",
            "Rule 2A should fire for CTS-only CPH 12/345/6001");

        // Scenario 2: CPH 12/345/6002 in SAM only → Rule 2B
        activeIssues.Should().Contain(i => i.IssueCode == RuleIds.SAM_CPH_NOT_IN_CTS && i.Cph == "12/345/6002",
            "Rule 2B should fire for SAM-only CPH 12/345/6002");

        // Scenario 3: CPH 12/345/6003 in both but with issues
        activeIssues.Should().Contain(i => i.IssueCode == RuleIds.CTS_SAM_NO_EMAIL_ADDRESSES && i.Cph == "12/345/6003",
            "Rule 4 should fire for CPH 12/345/6003 (no emails)");
        activeIssues.Should().Contain(i => i.IssueCode == RuleIds.CTS_SAM_NO_PHONE_NUMBERS && i.Cph == "12/345/6003",
            "Rule 5 should fire for CPH 12/345/6003 (no phones)");
        activeIssues.Should().Contain(i => i.IssueCode == RuleIds.SAM_NO_CATTLE_UNIT && i.Cph == "12/345/6003",
            "Rule 1 should fire for CPH 12/345/6003 (ANIMAL_SPECIES_CODE != CTT)");
        activeIssues.Should().Contain(i => i.IssueCode == RuleIds.CTS_SAM_INCONSISTENT_EMAIL_ADDRESSES && i.Cph == "12/345/6003",
            "Rule 6 should fire for CPH 12/345/6003 (no missing CTS emails triggers inconsistency check)");

        activeIssueCount.Should().Be(6, "Exactly 6 issues should be raised across the 3 scenarios");

        // --- QueryAsync: retrieve all active issues ---
        _output.WriteLine("  Verifying QueryAsync for active issues...");
        var allActiveQuery = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create().WhereActive().Page(0, 50), ct);
        allActiveQuery.TotalCount.Should().Be(6, "QueryAsync should return 6 active issues");
        allActiveQuery.Items.Should().HaveCount(6);
        allActiveQuery.Skip.Should().Be(0);
        allActiveQuery.Top.Should().Be(50);
        allActiveQuery.HasMore.Should().BeFalse("all 6 issues fit within a single page of 50");

        // --- QueryAsync: filter by issue code ---
        _output.WriteLine("  Verifying QueryAsync filtered by issue code...");
        var rule2aQuery = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create().WhereActive().WithIssueCode(RuleIds.CTS_CPH_NOT_IN_SAM), ct);
        rule2aQuery.TotalCount.Should().Be(1, "Only one Rule 2A issue should exist");
        rule2aQuery.Items.Should().ContainSingle()
            .Which.Cph.Should().Be("12/345/6001");

        // --- QueryAsync: filter by CPH contains ---
        _output.WriteLine("  Verifying QueryAsync filtered by CPH contains...");
        var cph6003Query = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create().WhereActive().WithCphContaining("6003"), ct);
        cph6003Query.TotalCount.Should().Be(4, "CPH 12/345/6003 should have 4 active issues (Rules 4, 5, 1, 6)");
        cph6003Query.Items.Should().HaveCount(4);
        cph6003Query.Items.Should().OnlyContain(i => i.Cph == "12/345/6003");

        // --- QueryAsync: sorting ---
        _output.WriteLine("  Verifying QueryAsync with sorting...");
        var sortedByCphAsc = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create()
                .WhereActive()
                .OrderBy(CleanseIssueSortField.Cph, descending: false)
                .Page(0, 50), ct);
        sortedByCphAsc.Items.Should().BeInAscendingOrder(i => i.Cph, "issues should be sorted by CPH ascending");

        var sortedByCphDesc = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create()
                .WhereActive()
                .OrderBy(CleanseIssueSortField.Cph, descending: true)
                .Page(0, 50), ct);
        sortedByCphDesc.Items.Should().BeInDescendingOrder(i => i.Cph, "issues should be sorted by CPH descending");

        // --- QueryAsync: pagination ---
        _output.WriteLine("  Verifying QueryAsync with pagination...");
        var page1 = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create().WhereActive().Page(0, 2), ct);
        page1.Items.Should().HaveCount(2, "first page should return 2 items");
        page1.TotalCount.Should().Be(6, "total count should still reflect all matching issues");
        page1.HasMore.Should().BeTrue("there are more issues beyond the first page");

        var page2 = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create().WhereActive().Page(2, 2), ct);
        page2.Items.Should().HaveCount(2, "second page should return 2 items");
        page2.HasMore.Should().BeTrue("there are still more issues on the next page");

        var page3 = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create().WhereActive().Page(4, 2), ct);
        page3.Items.Should().HaveCount(2, "third page should return the remaining 2 items");
        page3.HasMore.Should().BeFalse("no more issues remain");

        // --- QueryAsync: no results ---
        _output.WriteLine("  Verifying QueryAsync returns empty for non-matching filter...");
        var noResults = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create().WhereActive().WithIssueCode("NON_EXISTENT_CODE"), ct);
        noResults.TotalCount.Should().Be(0);
        noResults.Items.Should().BeEmpty();

        _output.WriteLine("✓ Step 3 complete: all 6 expected issues and QueryAsync verified");

        // ----------------------------------------------------------------
        // Step 4: Verify the exported CSV report and notification
        // ----------------------------------------------------------------
        _output.WriteLine("\n--- Step 4: Verifying CSV report output and notification ---");
        await VerifyCsvReportAsync(activeIssues);

        // Verify the notification service was called with a presigned URL
        _mockNotificationService.Verify(
            x => x.SendReportNotificationAsync(It.Is<string>(url => url.Contains("cleanse-report")), It.IsAny<CancellationToken>()),
            Times.Once,
            "Notification service should have been called once with the report URL after the first analysis");
        _output.WriteLine("  ✓ Notification sent with report URL");
        _output.WriteLine("✓ Step 4 complete: CSV report and notification verified");

        // ----------------------------------------------------------------
        // Step 5: Fix data — add a SAM holding + holder for CPH 12/345/6001
        //         This resolves Rule 2A for that CPH.
        // ----------------------------------------------------------------
        _output.WriteLine("\n--- Step 5: Fixing data (adding SAM record for 12/345/6001) ---");
        await IngestFixDataAsync();
        _output.WriteLine("✓ Step 5 complete: fix data ingested");

        // ----------------------------------------------------------------
        // Step 6: Run second cleanse analysis
        // ----------------------------------------------------------------
        _output.WriteLine("\n--- Step 6: Running second cleanse analysis ---");
        var secondResult = await analysisService.RunAnalysisAsync(ct);

        secondResult.Should().NotBeNull("Second analysis should complete successfully");
        secondResult!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        _output.WriteLine($"✓ Step 6 complete: second analysis finished (records={secondResult.RecordsAnalyzed}, issues={secondResult.IssuesFound}, resolved={secondResult.IssuesResolved})");

        // ----------------------------------------------------------------
        // Step 7: Verify the Rule 2A issue for 12/345/6001 is deactivated
        // ----------------------------------------------------------------
        _output.WriteLine("\n--- Step 7: Verifying issue deactivation ---");
        var activeIssuesAfterFix = await issueQueries.GetActiveIssuesAsync(0, 100, ct);
        var activeCountAfterFix = await issueQueries.GetActiveIssuesCountAsync(ct);

        _output.WriteLine($"Active issues after fix: {activeCountAfterFix}");
        foreach (var issue in activeIssuesAfterFix)
        {
            _output.WriteLine($"  [{issue.IssueCode}] CPH={issue.Cph}");
        }

        activeCountAfterFix.Should().Be(6, "Rule 2A deactivated for 12/345/6001 but Rule 6 now fires for it (emails match)");
        activeIssuesAfterFix.Should().NotContain(i => i.IssueCode == RuleIds.CTS_CPH_NOT_IN_SAM && i.Cph == "12/345/6001",
            "Rule 2A for 12/345/6001 should no longer be active after fixing the data");

        // Verify the deactivated issue exists but is inactive
        var allIssuesQuery = await issueQueries.QueryAsync(
            CleanseIssueQueryDto.Create()
                .WhereInactive()
                .WithIssueCode(RuleIds.CTS_CPH_NOT_IN_SAM)
                .WithCphContaining("12/345/6001"), ct);
        allIssuesQuery.TotalCount.Should().Be(1, "The deactivated Rule 2A issue should still exist as inactive");

        // Verify issue history for the deactivated issue
        var deactivatedIssue = allIssuesQuery.Items.First();
        var history = await issueQueries.GetIssueHistoryAsync(deactivatedIssue.Id, 0, 50, ct);

        _output.WriteLine($"Issue history for deactivated issue {deactivatedIssue.Id}:");
        foreach (var entry in history)
        {
            _output.WriteLine($"  [{entry.Action}] at {entry.OccurredAtUtc:HH:mm:ss} by {entry.PerformedBy}");
        }

        history.Should().HaveCountGreaterThanOrEqualTo(1, "At least a 'created' history entry should exist");
        _output.WriteLine("✓ Step 7 complete: issue deactivation and history verified");

        // ----------------------------------------------------------------
        // Step 8: Verify 2 cleanse operations exist
        // ----------------------------------------------------------------
        _output.WriteLine("\n--- Step 8: Verifying cleanse operations ---");
        var operationQueries = _serviceProvider!.GetRequiredService<ICleanseAnalysisOperationsQueries>();
        var operations = await operationQueries.GetOperationsAsync(0, 10, ct);

        _output.WriteLine($"Cleanse operations: {operations.Count}");
        foreach (var op in operations)
        {
            _output.WriteLine($"  [{op.Status}] started={op.StartedAtUtc:HH:mm:ss}");
        }

        operations.Should().HaveCount(2, "Two cleanse analysis operations should have been recorded");
        operations.Should().OnlyContain(op => op.Status == CleanseAnalysisStatus.Completed.ToString(),
            "Both operations should be completed");
        _output.WriteLine("✓ Step 8 complete: 2 operations verified");

        _output.WriteLine("\n=== Cleanse Analysis Happy-Path End-to-End Test PASSED ===");
    }

    #region CSV Report Verification

    private static readonly string[] ExpectedCsvHeaders =
        ["CPH", "Rule No", "Error Code", "Error Description", "Email CTS", "Email SAM", "Tel CTS", "Tel SAM", "FSA"];

    private async Task VerifyCsvReportAsync(IReadOnlyList<IssueDto> expectedIssues)
    {
        // Find the exported zip in S3
        var reportObjects = await _localStackFixture.S3Client.ListObjectsV2Async(
            new Amazon.S3.Model.ListObjectsV2Request
            {
                BucketName = LocalStackFixture.TestBucket,
                Prefix = "cleanse-reports/"
            });

        reportObjects.S3Objects.Should().ContainSingle("Exactly one report zip should have been uploaded");
        var reportKey = reportObjects.S3Objects[0].Key;
        reportKey.Should().EndWith(".zip");
        _output.WriteLine($"  Found report: {reportKey}");

        // Download and extract CSV from the zip
        var getResponse = await _localStackFixture.S3Client.GetObjectAsync(LocalStackFixture.TestBucket, reportKey);
        using var zipStream = new MemoryStream();
        await getResponse.ResponseStream.CopyToAsync(zipStream);
        zipStream.Position = 0;

        using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read);
        archive.Entries.Should().ContainSingle()
            .Which.Name.Should().Be("cleanse-report.csv");

        var csvEntry = archive.Entries[0];
        using var entryStream = csvEntry.Open();
        using var streamReader = new StreamReader(entryStream);
        var csvContent = await streamReader.ReadToEndAsync();
        _output.WriteLine($"  CSV content:\n{csvContent}");

        // Parse CSV
        using var textReader = new StringReader(csvContent);
        using var csv = new CsvReader(textReader, CultureInfo.InvariantCulture);
        csv.Read();
        csv.ReadHeader();

        // Verify headers are exactly the 9 expected columns
        csv.HeaderRecord.Should().BeEquivalentTo(ExpectedCsvHeaders,
            options => options.WithStrictOrdering(),
            "CSV should have exactly 9 columns in the specified order");

        var rows = new List<Dictionary<string, string>>();
        while (csv.Read())
        {
            var row = new Dictionary<string, string>();
            foreach (var header in ExpectedCsvHeaders)
            {
                row[header] = csv.GetField(header) ?? string.Empty;
            }
            rows.Add(row);
        }

        rows.Should().HaveCount(expectedIssues.Count,
            $"CSV should contain {expectedIssues.Count} data rows matching the active issues");

        // Verify ordering: grouped by rule priority (2A → 2B → 4 → 5 → 1 → 6), sorted by CPH within each group
        var expectedRuleOrder = new[] { "2A", "2B", "4", "5", "1", "6" };
        var actualRuleOrder = rows.Select(r => r["Rule No"]).ToArray();
        actualRuleOrder.Should().BeEquivalentTo(expectedRuleOrder,
            options => options.WithStrictOrdering(),
            "Rows should be ordered by rule priority (2A, 2B, 4, 5, 1, 6)");

        // Verify each row's content
        foreach (var row in rows)
        {
            _output.WriteLine($"  Row: CPH={row["CPH"]}, Rule No={row["Rule No"]}, Error Code={row["Error Code"]}");
        }

        // Row 1: Rule 2A — CPH 12/345/6001
        rows[0]["CPH"].Should().Be("12/345/6001");
        rows[0]["Rule No"].Should().Be("2A");
        rows[0]["Error Code"].Should().Be("02A");
        rows[0]["Error Description"].Should().Be("Active CTS CPH inactive / missing in Sam");

        // Row 2: Rule 2B — CPH 12/345/6002
        rows[1]["CPH"].Should().Be("12/345/6002");
        rows[1]["Rule No"].Should().Be("2B");
        rows[1]["Error Code"].Should().Be("2B");
        rows[1]["Error Description"].Should().Be("Active SAM CPH inactive / missing in CTS");

        // Row 3: Rule 4 — CPH 12/345/6003
        rows[2]["CPH"].Should().Be("12/345/6003");
        rows[2]["Rule No"].Should().Be("4");
        rows[2]["Error Code"].Should().Be("04");
        rows[2]["Error Description"].Should().Be("CPH present in both CTS and SAM but no email addresses in either system");

        // Row 4: Rule 5 — CPH 12/345/6003
        rows[3]["CPH"].Should().Be("12/345/6003");
        rows[3]["Rule No"].Should().Be("5");
        rows[3]["Error Code"].Should().Be("05");
        rows[3]["Error Description"].Should().Be("No telephone numbers in CTS or Sam");

        // Row 5: Rule 1 — CPH 12/345/6003
        rows[4]["CPH"].Should().Be("12/345/6003");
        rows[4]["Rule No"].Should().Be("1");
        rows[4]["Error Code"].Should().Be("01");
        rows[4]["Error Description"].Should().Be("No cattle unit defined in SAM");

        // Row 6: Rule 6 — CPH 12/345/6003
        rows[5]["CPH"].Should().Be("12/345/6003");
        rows[5]["Rule No"].Should().Be("6");
        rows[5]["Error Code"].Should().Be("06");
        rows[5]["Error Description"].Should().Be("SAM is missing email addresses found in CTS");
    }

    #endregion

    #region Data Ingestion Helpers

    private async Task IngestInitialTestDataAsync()
    {
        var dateStr = DateTime.UtcNow.AddDays(-1).ToString(EtlConstants.DateTimePattern);

        // CTS CPH Holdings — 2 records (scenarios 1 & 3)
        await UploadCsvAsync(
            $"LITP_CTSCPHHOLDING_{dateStr}.csv",
            """
            LID_FULL_IDENTIFIER|ADR_NAME|LOC_TEL_NUMBER|LOC_MOBILE_NUMBER|CHANGE_TYPE
            UK-12/345/6001|FarmA|||I
            UK-12/345/6003|FarmC|||I
            """);

        // CTS Keepers — 2 records
        await UploadCsvAsync(
            $"LITP_CTSKEEPER_{dateStr}.csv",
            """
            PAR_ID|LID_FULL_IDENTIFIER|PAR_EMAIL_ADDRESS|PAR_TEL_NUMBER|PAR_MOBILE_NUMBER|CHANGE_TYPE
            K001|UK-12/345/6001|keeper1@test.com|01234567890||I
            K003|UK-12/345/6003||||I
            """);

        // SAM CPH Holdings — 2 records (scenarios 2 & 3)
        await UploadCsvAsync(
            $"LITP_SAMCPHHOLDING_{dateStr}.csv",
            """
            CPH|FEATURE_NAME|SECONDARY_CPH|ANIMAL_SPECIES_CODE|CHANGE_TYPE
            12/345/6002|FarmB|N/A|CTT|I
            12/345/6003|FarmC|N/A|SHP|I
            """);

        // Run import to ingest all 3 files
        await RunImportAsync();

        // Clean up S3 files so they aren't re-processed
        await DeleteAllDestFilesAsync();
    }

    private async Task IngestFixDataAsync()
    {
        var dateStr = DateTime.UtcNow.ToString(EtlConstants.DateTimePattern);

        // SAM CPH Holdings — add record for CPH 12/345/6001
        await UploadCsvAsync(
            $"LITP_SAMCPHHOLDING_{dateStr}.csv",
            """
            CPH|FEATURE_NAME|SECONDARY_CPH|ANIMAL_SPECIES_CODE|CHANGE_TYPE
            12/345/6001|FarmA|N/A|CTT|I
            """);

        // SAM CPH Holders — add holder linked to 12/345/6001 with matching email/phone
        await UploadCsvAsync(
            $"LITP_SAMCPHHOLDER_{dateStr}.csv",
            """
            PARTY_ID|CPHS|INTERNET_EMAIL_ADDRESS|TELEPHONE_NUMBER|MOBILE_NUMBER|CHANGE_TYPE
            H001|12/345/6001|keeper1@test.com|01234567890||I
            """);

        await RunImportAsync();
        await DeleteAllDestFilesAsync();
    }

    private async Task UploadCsvAsync(string fileName, string csvContent)
    {
        var fileKey = $"{DestPrefix}/{fileName}";
        _output.WriteLine($"  Uploading {fileName}");

        await _localStackFixture.S3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = fileKey,
            ContentBody = csvContent.Trim(),
            ContentType = "text/csv"
        });
    }

    private async Task RunImportAsync()
    {
        var importOrchestrator = _serviceProvider!.GetRequiredService<IImportOrchestrator>();
        var importId = Guid.NewGuid();
        _output.WriteLine($"  Running import {importId}...");
        await importOrchestrator.StartAsync(importId, "external", CancellationToken.None);
        _output.WriteLine($"  Import {importId} completed");
    }

    private async Task DeleteAllDestFilesAsync()
    {
        var listResponse = await _localStackFixture.S3Client.ListObjectsV2Async(
            new Amazon.S3.Model.ListObjectsV2Request
            {
                BucketName = LocalStackFixture.TestBucket,
                Prefix = DestPrefix
            });

        foreach (var obj in listResponse.S3Objects)
        {
            await _localStackFixture.S3Client.DeleteObjectAsync(LocalStackFixture.TestBucket, obj.Key);
        }
    }

    #endregion

    #region Service Provider

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();

        services.AddLogging(builder => builder.AddConsole());
        services.AddSingleton(TimeProvider.System);

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["AesSalt"] = "Jr8Lm2PXzd7qNbVyWutRfGBxhkHTpE"
            })
            .Build();
        services.AddSingleton<IConfiguration>(configuration);

        // MongoDB
        var mongoConfig = new MongoConfig
        {
            DatabaseName = TestDatabaseName,
            DatabaseUri = "not-used-in-test",
            EnableTransactions = false,
            HealthcheckEnabled = false
        };
        services.AddSingleton(Options.Create(mongoConfig));
        services.AddSingleton<IOptions<IDatabaseConfig>>(Options.Create<IDatabaseConfig>(mongoConfig));
        services.AddSingleton(_mongoDbFixture.MongoClient);

        var resilienceSection = configuration.GetSection("MongoResilience");
        services.Configure<MongoResilienceConfig>(resilienceSection);
        services.AddSingleton<ResilientMongoOperations>();

        // DataSet definitions
        services.AddSingleton<IDataSetDefinitions>(_dataSetDefinitions);
        services.AddSingleton(_dataSetDefinitions);

        // S3 / Blob storage
        var storageConfig = new StorageConfiguration
        {
            ExternalStorage = new StorageWithCredentialsConfiguration
            {
                BucketName = LocalStackFixture.TestBucket,
                AccessKeySecretName = "not-used",
                SecretKeySecretName = "not-used",
                HealthcheckEnabled = false
            },
            InternalStorage = new StorageConfigurationDetails
            {
                BucketName = LocalStackFixture.TestBucket,
                HealthcheckEnabled = false
            },
            SourceExternalPrefix = SourcePrefix,
            SourceInternalPrefix = DestPrefix,
            TargetInternalPrefix = DestPrefix
        };
        services.AddSingleton(storageConfig);
        services.AddSingleton(new AmazonS3Config
        {
            ServiceURL = "http://localhost:4566",
            ForcePathStyle = true,
            UseHttp = true
        });

        var s3ClientFactory = new S3ClientFactory();
        s3ClientFactory.RegisterMockClient<ExternalStorageClient>(LocalStackFixture.TestBucket, _localStackFixture.S3Client);
        s3ClientFactory.RegisterMockClient<InternalStorageClient>(LocalStackFixture.TestBucket, _localStackFixture.S3Client);
        services.AddSingleton<IS3ClientFactory>(s3ClientFactory);
        services.AddTransient<IBlobStorageServiceFactory, S3BlobStorageServiceFactory>();

        // Crypto
        services.AddSingleton<IPasswordSaltService, PasswordSaltService>();
        services.AddSingleton<IAesCryptoTransform, AesCryptoTransform>();

        // Lineage / Reporting
        services.AddSingleton<ILineageIdGenerator, LineageIdGenerator>();
        services.AddSingleton<ILineageMapper, LineageMapper>();
        services.AddSingleton<ILineageIndexManagerFactory, LineageIndexManagerFactory>();
        services.AddScoped<IImportReportingService, ImportReportingService>();

        // ETL pipeline
        services.AddTransient<CsvRowCounter>();
        services.AddTransient<IExternalCatalogueServiceFactory, ExternalCatalogueServiceFactory>();
        services.AddSingleton<KeeperData.Core.Telemetry.IApplicationMetrics>(Mock.Of<KeeperData.Core.Telemetry.IApplicationMetrics>());
        services.AddScoped<IAcquisitionPipeline, AcquisitionPipeline>();
        services.AddScoped<IIngestionPipeline, IngestionPipeline>();
        services.AddScoped<IImportOrchestrator, ImportOrchestrator>();

        // Query services
        services.AddScoped<IQueryService, QueryService>();
        services.AddScoped<IODataQueryService, ODataQueryService>();

        // CtsSamQueryService (used by the cleanse engine)
        services.AddScoped<ICtsSamQueryService, CtsSamQueryService>();

        // Distributed lock (used by CleanseAnalysisCommandService)
        services.AddSingleton<IDistributedLock, MongoDistributedLock>();

        // Mock notification service — returns a successful result so the export path completes cleanly
        _mockNotificationService
            .Setup(x => x.SendReportNotificationAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new CleanseReportNotificationResult
            {
                Success = true,
                NotificationId = "test-notification-id",
                Recipient = "test@example.com"
            });
        services.AddSingleton<ICleanseReportNotificationService>(_mockNotificationService.Object);

        // Register all cleanse report dependencies
        services.AddCleanseReportDependencies();

        return services.BuildServiceProvider();
    }

    #endregion
}
