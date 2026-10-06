using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.EtlPipeline.Views;

/// <summary>One source system's half of the transformation, with what it needs from staging and
/// what it leaves behind.
///
/// A part declares its staging tables so the writer can decide whether to run it. Not every
/// environment loads every source system - a deployment with no CTS extracts has no cts_ tables at
/// all - and without this the whole script fails to bind, taking the read model down with it.</summary>
/// <param name="Name">Identifies the part in logs. The script file's name, without its extension.</param>
/// <param name="Sql">The part's body, run as a single command.</param>
/// <param name="RequiredSourceTables">The staging tables the body binds against. All or nothing: a
/// part that can see only some of its tables does not run.</param>
/// <param name="TableNames">The target tables the body produces, counted once it has run.</param>
/// <param name="Required">Whether a missing source table is a failure rather than a skip. True for
/// a part the read model would be meaningless without.</param>
[ExcludeFromCodeCoverage(Justification = "Transformation definition record - no logic to test.")]
public sealed record SqliteViewPart(
    string Name,
    string Sql,
    IReadOnlyList<string> RequiredSourceTables,
    IReadOnlyList<string> TableNames,
    bool Required);
