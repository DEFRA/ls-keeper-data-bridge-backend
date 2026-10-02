using FluentAssertions;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Views;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Status;

/// <summary>The store-independent half of import status. Two stores keep these documents, and a
/// disagreement between them about a lease or a merged dataset entry would be a difference nothing
/// points at - so the rules are asserted here rather than in either store.</summary>
public class EtlImportProgressTests
{
    private const string External = "external";

    private static readonly DateTime Now = new(2026, 10, 2, 9, 0, 0, DateTimeKind.Utc);

    private static EtlImportDocument Queued()
        => EtlImportProgress.Queued(Guid.NewGuid(), External, "cts_locations", Now);

    private static EtlImportStageProgress Stage(
        string name = "optimise",
        int items = 3,
        params EtlImportDatasetProgress[] datasets)
        => new(name, items, TimeSpan.FromSeconds(2), datasets);

    [Fact]
    public void Queued_starts_the_run_with_a_lease()
    {
        var importId = Guid.NewGuid();

        var document = EtlImportProgress.Queued(importId, External, "sam_herd", Now);

        document.ImportId.Should().Be(importId);
        document.Status.Should().Be(nameof(EtlImportStatus.Queued));
        document.SourceType.Should().Be(External);
        document.Dataset.Should().Be("sam_herd");
        document.RequestedAtUtc.Should().Be(Now);
        document.LeaseExpiresAtUtc.Should().Be(Now.Add(EtlImportProgress.LeaseDuration));
    }

    [Fact]
    public void Purged_records_the_wipe_as_a_finished_entry_that_is_not_a_run()
    {
        var purgeId = Guid.NewGuid();

        var document = EtlImportProgress.Purged(
            new EtlPurgeRecord(purgeId, External, null, ["raw", "normalised"], 42),
            Now);

        document.ImportId.Should().Be(purgeId);
        document.Status.Should().Be(nameof(EtlImportStatus.Purged));
        document.CompletedAtUtc.Should().Be(Now, "a purge is over the moment it is recorded");
        document.LeaseExpiresAtUtc.Should().BeNull("a purge is not something that can be abandoned");
        document.Stages.Should().BeEmpty();
        document.Datasets.Should().BeEmpty();
        document.Purge!.Stages.Should().Equal("raw", "normalised");
        document.Purge.DeletedCount.Should().Be(42);
    }

    [Fact]
    public void ApplyStage_appends_the_stage_and_extends_the_lease()
    {
        var document = Queued();
        var later = Now.AddMinutes(5);

        EtlImportProgress.ApplyStage(document, Stage("decrypt", 7), later);

        document.Stages.Should().ContainSingle();
        document.Stages[0].Name.Should().Be("decrypt");
        document.Stages[0].ItemCount.Should().Be(7);
        document.Stages[0].ElapsedMs.Should().Be(2000);
        document.Stages[0].CompletedAtUtc.Should().Be(later);
        document.LeaseExpiresAtUtc.Should().Be(later.Add(EtlImportProgress.LeaseDuration),
            "the lease is only extended between stages, so each one has to push it out");
    }

    [Fact]
    public void ApplyStage_folds_successive_stages_into_one_dataset_entry()
    {
        var document = Queued();

        EtlImportProgress.ApplyStage(
            document,
            Stage(datasets: new EtlImportDatasetProgress("cts_locations") { RawKeys = ["raw/a"] }),
            Now);

        EtlImportProgress.ApplyStage(
            document,
            Stage(datasets: new EtlImportDatasetProgress("cts_locations") { SnapshotKey = "snapshots/a", RowCount = 9 }),
            Now);

        document.Datasets.Should().ContainSingle("the second stage describes the same dataset, not another one");
        document.Datasets[0].RawKeys.Should().ContainSingle("a later stage that says nothing about raw keys leaves them alone")
            .Which.Should().Be("raw/a");
        document.Datasets[0].SnapshotKey.Should().Be("snapshots/a");
        document.Datasets[0].RowCount.Should().Be(9);
    }

    [Fact]
    public void ApplyStage_keeps_a_dataset_entry_per_dataset()
    {
        var document = Queued();

        EtlImportProgress.ApplyStage(
            document,
            Stage(datasets:
            [
                new EtlImportDatasetProgress("cts_locations") { RowCount = 1 },
                new EtlImportDatasetProgress("cts_parties") { RowCount = 2 }
            ]),
            Now);

        document.Datasets.Select(d => d.Dataset).Should().Equal("cts_locations", "cts_parties");
        document.Datasets.Select(d => d.RowCount).Should().Equal(1, 2);
    }

    [Fact]
    public void ApplyStage_leaves_a_value_alone_when_the_stage_does_not_own_it()
    {
        var document = Queued();

        EtlImportProgress.ApplyStage(
            document,
            Stage(datasets: new EtlImportDatasetProgress("cts_locations")
            {
                RowsUpserted = 5,
                ColumnsAdded = ["LOC_ID"]
            }),
            Now);

        EtlImportProgress.ApplyStage(
            document,
            Stage(datasets: new EtlImportDatasetProgress("cts_locations") { RowsDeleted = 1 }),
            Now);

        var dataset = document.Datasets.Single();
        dataset.RowsUpserted.Should().Be(5, "a null count means the stage had nothing to say, not zero");
        dataset.RowsDeleted.Should().Be(1);
        dataset.ColumnsAdded.Should().ContainSingle("an empty list means the stage had nothing to say")
            .Which.Should().Be("LOC_ID");
    }

    [Fact]
    public void ApplyStage_carries_every_field_a_stage_can_report()
    {
        var document = Queued();
        var snapshotAt = new DateTimeOffset(2026, 9, 30, 7, 0, 0, TimeSpan.Zero);

        EtlImportProgress.ApplyStage(
            document,
            Stage(datasets: new EtlImportDatasetProgress("cts_locations")
            {
                SourceFiles = [("cads/cts/bulk/a.hcdt", 1024)],
                RawKeys = ["raw/a"],
                NormalisedKeys = ["normalised/a"],
                OptimisedKeys = ["optimised/a"],
                SnapshotKey = "snapshots/a",
                SnapshotSourceTimestamp = snapshotAt,
                RowCount = 10,
                RowsUpserted = 8,
                RowsDeleted = 1,
                RowsIgnoredDeletes = 2,
                RowsRejected = 3,
                ColumnsNullified = ["LOC_COMMENTS"],
                ColumnsAdded = ["LOC_REASON_CODE"]
            }),
            Now);

        var dataset = document.Datasets.Single();
        dataset.SourceFiles.Should().ContainSingle();
        dataset.SourceFiles[0].Key.Should().Be("cads/cts/bulk/a.hcdt");
        dataset.SourceFiles[0].Size.Should().Be(1024);
        dataset.RawKeys.Should().ContainSingle();
        dataset.NormalisedKeys.Should().ContainSingle();
        dataset.OptimisedKeys.Should().ContainSingle();
        dataset.SnapshotKey.Should().Be("snapshots/a");
        dataset.SnapshotSourceTimestampUtc.Should().Be(snapshotAt.UtcDateTime);
        dataset.RowCount.Should().Be(10);
        dataset.RowsUpserted.Should().Be(8);
        dataset.RowsDeleted.Should().Be(1);
        dataset.RowsIgnoredDeletes.Should().Be(2);
        dataset.RowsRejected.Should().Be(3);
        dataset.ColumnsNullified.Should().ContainSingle();
        dataset.ColumnsAdded.Should().ContainSingle();
    }

    [Fact]
    public void ApplyStage_records_the_outputs_only_when_a_stage_supplies_them()
    {
        var document = Queued();

        EtlImportProgress.ApplyStage(document, Stage("snapshot"), Now);

        document.DuckDbKey.Should().BeNull();
        document.SqliteKey.Should().BeNull();

        EtlImportProgress.ApplyStage(
            document,
            new EtlImportStageProgress("export-sqlite", 1, TimeSpan.Zero, [],
                DuckDbKey: "staging/db.duckdb",
                SqliteKey: "views/db.sqlite",
                SqliteTables: [new SqliteViewTable("Holding", 4)]),
            Now);

        document.DuckDbKey.Should().Be("staging/db.duckdb");
        document.SqliteKey.Should().Be("views/db.sqlite");
        document.SqliteTables.Should().ContainSingle();
        document.SqliteTables[0].Name.Should().Be("Holding");
        document.SqliteTables[0].RowCount.Should().Be(4);
    }

    [Fact]
    public void Complete_ends_the_run_and_drops_the_lease()
    {
        var document = Queued();
        EtlImportProgress.ApplyStage(document, Stage(), Now);

        var detail = new EtlImportErrorDetail { Stage = "optimise", Type = "InvalidOperationException" };

        EtlImportProgress.Complete(document, EtlImportStatus.Failed, "boom", detail, Now.AddHours(1));

        document.Status.Should().Be(nameof(EtlImportStatus.Failed));
        document.CompletedAtUtc.Should().Be(Now.AddHours(1));
        document.CurrentStage.Should().BeNull();
        document.Error.Should().Be("boom");
        document.ErrorDetail.Should().BeSameAs(detail);
        document.LeaseExpiresAtUtc.Should().BeNull("a finished run cannot lapse");
    }

    [Theory]
    [InlineData(nameof(EtlImportStatus.Running))]
    [InlineData(nameof(EtlImportStatus.Queued))]
    public void AsAbandonedIfLapsed_fails_a_run_whose_lease_has_passed(string status)
    {
        var document = Queued();
        document.Status = status;

        var result = EtlImportProgress.AsAbandonedIfLapsed(document, Now.Add(EtlImportProgress.LeaseDuration).AddSeconds(1));

        result.Status.Should().Be(nameof(EtlImportStatus.Failed));
        result.Error.Should().Contain("abandoned");
    }

    [Fact]
    public void AsAbandonedIfLapsed_leaves_a_run_whose_lease_is_still_live()
    {
        var document = Queued();
        document.Status = nameof(EtlImportStatus.Running);

        var result = EtlImportProgress.AsAbandonedIfLapsed(document, Now.AddMinutes(1));

        result.Status.Should().Be(nameof(EtlImportStatus.Running));
        result.Error.Should().BeNull();
    }

    [Fact]
    public void AsAbandonedIfLapsed_leaves_a_run_that_already_finished()
    {
        var document = Queued();
        EtlImportProgress.Complete(document, EtlImportStatus.Succeeded, null, null, Now);

        var result = EtlImportProgress.AsAbandonedIfLapsed(document, Now.AddDays(1));

        result.Status.Should().Be(nameof(EtlImportStatus.Succeeded), "a finished run has no lease to lapse");
    }

    [Fact]
    public void IsInFlight_is_true_only_while_the_lease_holds()
    {
        var document = Queued();
        document.Status = nameof(EtlImportStatus.Running);

        EtlImportProgress.IsInFlight(document, Now.AddMinutes(1)).Should().BeTrue();
        EtlImportProgress.IsInFlight(document, Now.Add(EtlImportProgress.LeaseDuration).AddSeconds(1)).Should().BeFalse();
    }

    [Fact]
    public void IsInFlight_is_false_once_the_run_has_finished()
    {
        var document = Queued();
        EtlImportProgress.Complete(document, EtlImportStatus.Succeeded, null, null, Now);

        EtlImportProgress.IsInFlight(document, Now).Should().BeFalse();
    }
}
