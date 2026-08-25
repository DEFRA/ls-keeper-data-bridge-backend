using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Core.ETL.Impl;

/// <summary>A discovered source file and the timestamp encoded in its name.</summary>
[ExcludeFromCodeCoverage(Justification = "Simple data transfer record.")]
public record EtlFile(StorageObjectInfo StorageObject, DateTimeOffset Timestamp);
