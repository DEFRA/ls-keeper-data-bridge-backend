using KeeperData.Etl.Tool.Commands;
using System.CommandLine;

var root = new RootCommand(
    """
    KeeperData ETL tool - runs the ETL pipeline against a local source folder.

    Configuration comes from appsettings.json, then environment variables, then the command line.
    AesSalt must be set in the environment; the pipeline cannot decrypt a source file without it.
    """);

root.Add(RunCommand.Create());
root.Add(ListCommand.Create());
root.Add(ShowCommand.Create());
root.Add(PurgeCommand.Create());
root.Add(ConfigCommand.Create());

return await root.Parse(args).InvokeAsync();
