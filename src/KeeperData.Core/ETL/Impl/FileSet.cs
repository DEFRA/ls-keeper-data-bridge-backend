using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.ETL.Impl;

/// <summary>The files discovered for a single dataset definition.</summary>
[ExcludeFromCodeCoverage(Justification = "Simple data transfer record.")]
public record FileSet(DataSetDefinition Definition, EtlFile[] Files);
