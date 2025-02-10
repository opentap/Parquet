using NUnit.Framework;
using OpenTap.Plugins.Parquet;

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
        string[] fields =
        [
            "ResultName", "Guid", "Parent", "StepId",
            "Step/Param1", "Step/Param2", "Step/Param3", "Step/Group/Param",
            "Result/Value1", "Result/Value2", "Result/Value3"
        ];
        Assert.That(table.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(table.Count, Is.EqualTo(50));
        for (int i = 0; i < 50; i++)
        {
            object?[] values = [
                resultName, guid, parent, stepId,
                "Param1", 2, 3.141, true,
                "test", i, null
            ];
            Assert.That(table[i], Is.EquivalentTo(values));
        }
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
        string[] fields = [
            "ResultName", "Guid", "Parent", "StepId",
            "Step/Param1", "Step/Param2", "Step/Param3", "Step/Group/Param"
        ];
        object?[] values = [
            null, guid, parent, stepId,
            "Param1", 2, 3.141, true
        ];
        Assert.That(table.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(table.Count, Is.EqualTo(1));
        Assert.That(table[0], Is.EquivalentTo(values));
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
        string[] fields = [
            "ResultName", "Guid", "Parent", "StepId",
            "Plan/Param1", "Plan/Param2", "Plan/Param3", "Plan/Group/Param"
        ];
        object?[] values = [
            null, guid, null, null,
            "Param1", 2, 3.141, true
        ];
        Assert.That(table.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(table.Count, Is.EqualTo(1));
        Assert.That(table[0], Is.EquivalentTo(values));
    }
}