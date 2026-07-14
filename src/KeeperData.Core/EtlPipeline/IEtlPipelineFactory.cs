using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline;

public interface IEtlPipelineFactory
{
    PipelineDefinition Create();
}
