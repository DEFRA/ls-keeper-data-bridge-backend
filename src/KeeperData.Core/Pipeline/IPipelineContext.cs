namespace KeeperData.Core.Pipeline;

// Framework view of the run context. The concrete (domain) context implements this, so the framework
// never references domain services. Empty for now; stages get their dependencies via constructors.
public interface IPipelineContext
{
}
