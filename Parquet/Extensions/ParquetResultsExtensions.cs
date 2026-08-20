using System.Collections.Generic;
using OpenTap.Plugins.Parquet.Core;

namespace OpenTap.Plugins.Parquet.Extensions;

internal static class ParquetResultsExtensions
{
    public static void AddResultRow(this ParquetFile file, TestStepRun run, ResultTable table)
    {
        Dictionary<string, System.Array> results = table.GetResults(out Dictionary<string, string> nameMapping);
        file.AddResultRow(table.Name, run.Id.ToString(), run.Parent.ToString(), run.TestStepId.ToString(), run.GetParameters(), results);
        foreach (KeyValuePair<string, string> mapping in nameMapping)
        {
            file.AddNameMapping("Result/" + mapping.Key, "Result/" + mapping.Value);
        }
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