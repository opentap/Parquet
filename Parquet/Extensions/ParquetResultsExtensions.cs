using OpenTap.Plugins.Parquet.Core;

namespace OpenTap.Plugins.Parquet.Extensions;

internal static class ParquetResultsExtensions
{
    public static void AddResultRow(this ParquetFile file, TestStepRun run, ResultTable table)
    {
        file.AddResultRow(table.Name, run.Id.ToString(), run.Parent.ToString(), run.TestStepId.ToString(), run.GetParameters(), table.GetResults());
    }
    
    public static void AddStepRow(this ParquetFile file, TestStepRun run)
    {
        file.AddStepRow(run.Id.ToString(), run.Parent.ToString(), run.TestStepId.ToString(), run.GetParameters());
    }
    
    public static void AddPlanRow(this ParquetFile file, TestPlanRun plan)
    {
        file.AddPlanRow(plan.Id.ToString(), plan.GetParameters());
    }
}