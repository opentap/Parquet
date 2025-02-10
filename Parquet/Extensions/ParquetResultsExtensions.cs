namespace OpenTap.Plugins.Parquet.Extensions;

internal static class ParquetResultsExtensions
{
    public static void AddResultRow(this ParquetResult result, TestStepRun run, ResultTable table)
    {
        result.AddResultRow(table.Name, run.Id.ToString(), run.Parent.ToString(), run.TestStepId.ToString(), run.GetParameters(), table.GetResults());
    }
    
    public static void AddStepRow(this ParquetResult result, TestStepRun run)
    {
        result.AddStepRow(run.Id.ToString(), run.Parent.ToString(), run.TestStepId.ToString(), run.GetParameters());
    }
    
    public static void AddPlanRow(this ParquetResult result, TestPlanRun plan)
    {
        result.AddPlanRow(plan.Id.ToString(), plan.GetParameters());
    }
}