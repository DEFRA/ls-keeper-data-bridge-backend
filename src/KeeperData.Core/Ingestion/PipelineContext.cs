using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Contracts;

namespace KeeperData.Core.Ingestion;

/// <summary>Everything a stage needs at run time. Passed in by the executor.</summary>
public sealed class PipelineContext : IPipelineContext
{
    public IDataBridgeStore Store { get; }
    public IDataSetDefinitions Definitions { get; }
    public IWorkflowLog Log { get; }

    public PipelineContext(IDataBridgeStore store, IDataSetDefinitions definitions, IWorkflowLog log)
    {
        Store = store;
        Definitions = definitions;
        Log = log;
    }
}
