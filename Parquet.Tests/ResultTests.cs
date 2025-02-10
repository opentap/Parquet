using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using Parquet.Tests.Extensions;

namespace Parquet.Tests;

public class ResultTests
{
    [Test]
    public async Task ResultRowTest()
    {
        string path = $"Tests/{nameof(ResultTests)}/{nameof(ResultRowTest)}.parquet";
        
        string resultName = "Test";
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();

        Dictionary<string, IConvertible> parameters = new Dictionary<string, IConvertible>()
        {
            { "Param1", "Param1" },
            { "Param2", 2 },
            { "Param3", 3.141 },
            { "Group/Param", true },
        };
        Dictionary<string, Array> results = new Dictionary<string, Array>()
        {
            { "Value1", Enumerable.Repeat("test", 50).ToArray() },
            { "Value2", Enumerable.Range(0, 50).ToArray() },
            { "Value3", Enumerable.Repeat<string?>(null, 50).ToArray() }
        };

        ParquetResult result = new ParquetResult(path);
        result.AddResultRow(resultName, guid, parent, stepId, parameters, results);
        result.Dispose();
        
        Assert.True(System.IO.File.Exists(path));

        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId", "Step/Param1", "Step/Param2", "Step/Param3", "Step/Group/Param", "Result/Value1", "Result/Value2", "Result/Value3");
        table.AssertRows(50, (row, i) => row.AssertValues(resultName, guid, parent, stepId, "Param1", 2, 3.141, true, "test", i, null));
    }
    
    [Test]
    public async Task StepRowTest()
    {
        string path = $"Tests/{nameof(ResultTests)}/{nameof(StepRowTest)}.parquet";
        
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();

        Dictionary<string, IConvertible> parameters = new Dictionary<string, IConvertible>()
        {
            { "Param1", "Param1" },
            { "Param2", 2 },
            { "Param3", 3.141 },
            { "Group/Param", true },
        };

        ParquetResult result = new ParquetResult(path);
        result.AddStepRow(guid, parent, stepId, parameters);
        result.Dispose();
        
        Assert.True(System.IO.File.Exists(path));

        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId", "Step/Param1", "Step/Param2", "Step/Param3", "Step/Group/Param");
        table.AssertRows(1, row => row.AssertValues(null, guid, parent, stepId, "Param1", 2, 3.141, true));
    }
    
    [Test]
    public async Task PlanRowTest()
    {
        string path = $"Tests/{nameof(ResultTests)}/{nameof(PlanRowTest)}.parquet";
        
        string guid = Guid.NewGuid().ToString();

        Dictionary<string, IConvertible> parameters = new Dictionary<string, IConvertible>()
        {
            { "Param1", "Param1" },
            { "Param2", 2 },
            { "Param3", 3.141 },
            { "Group/Param", true },
        };

        ParquetResult result = new ParquetResult(path);
        result.AddPlanRow(guid, parameters);
        result.Dispose();
        
        Assert.True(System.IO.File.Exists(path));

        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId", "Plan/Param1", "Plan/Param2", "Plan/Param3", "Plan/Group/Param");
        table.AssertRows(1, row => row.AssertValues(null, guid, null, null, "Param1", 2, 3.141, true));
    }
}