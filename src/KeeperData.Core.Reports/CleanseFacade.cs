using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;

namespace KeeperData.Core.Reports;

public interface ICleanseFacade
{
    ICleanseFacadeCommands Commands { get; }
    ICleanseFacadeQueries Queries { get; }
}

public interface ICleanseFacadeCommands
{
    IIssueCommandService IssueCommandService { get; }
    ICleanseOperationCommandService CleanseOperationCommandService { get; }
    ICleanseAnalysisCommandService CleanseAnalysisCommandService { get; }
    ICleanseReportExportCommandService CleanseReportExportCommandService { get; }
}

public interface ICleanseFacadeQueries
{
    ICleanseAnalysisOperationsQueries CleanseAnalysisOperationsQueries { get; }
    IIssueQueries IssueQueries { get; }
    ICtsSamQueryService CtsSamQueryService { get; }
}

public class CleanseFacade(ICleanseFacadeCommands commands, ICleanseFacadeQueries queries) : ICleanseFacade
{
    public ICleanseFacadeCommands Commands { get; } = commands;
    public ICleanseFacadeQueries Queries { get; } = queries;
}

public class CleanseFacadeCommands(
    IIssueCommandService issueCommandService,
    ICleanseOperationCommandService cleanseOperationCommandService,
    ICleanseAnalysisCommandService cleanseAnalysisCommandService,
    ICleanseReportExportCommandService cleanseReportExportCommandService) : ICleanseFacadeCommands
{
    public IIssueCommandService IssueCommandService { get; } = issueCommandService;
    public ICleanseOperationCommandService CleanseOperationCommandService { get; } = cleanseOperationCommandService;
    public ICleanseAnalysisCommandService CleanseAnalysisCommandService { get; } = cleanseAnalysisCommandService;
    public ICleanseReportExportCommandService CleanseReportExportCommandService { get; } = cleanseReportExportCommandService;
}

public class CleanseFacadeQueries(
    ICleanseAnalysisOperationsQueries analysisOperationsQueries,
    IIssueQueries issueQueries,
    ICtsSamQueryService ctsSamQueryService) : ICleanseFacadeQueries
{
    public ICleanseAnalysisOperationsQueries CleanseAnalysisOperationsQueries { get; } = analysisOperationsQueries;
    public IIssueQueries IssueQueries { get; } = issueQueries;
    public ICtsSamQueryService CtsSamQueryService { get; } = ctsSamQueryService;
}