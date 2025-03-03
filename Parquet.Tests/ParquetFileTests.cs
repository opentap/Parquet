using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using OpenTap.Plugins.Parquet.Core;

namespace Parquet.Tests;

public class ParquetFileTests
{
    [Test]
    public async Task ResultRowTest()
    {
        string path = $"Tests/{nameof(ParquetFileTests)}/{nameof(ResultRowTest)}.parquet";
        
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

        ParquetFile file = new ParquetFile(path);
        file.AddResultRow(resultName, guid, parent, stepId, parameters, results);
        file.Dispose();
        
        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields =
        [
            "ResultName", "Guid", "Parent", "StepId",
            "Step/Param1", "Step/Param2", "Step/Param3", "Step/Group/Param",
            "Result/Value1", "Result/Value2", "Result/Value3"
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(50));
        for (int i = 0; i < 50; i++)
        {
            object?[] values = [
                resultName, guid, parent, stepId,
                "Param1", 2, 3.141, true,
                "test", i, null
            ];
            Assert.That(reader.ReadRow(i), Is.EquivalentTo(values));
        }
    }
    
    [Test]
    public async Task StepRowTest()
    {
        string path = $"Tests/{nameof(ParquetFileTests)}/{nameof(StepRowTest)}.parquet";
        
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

        ParquetFile file = new ParquetFile(path);
        file.AddStepRow(guid, parent, stepId, parameters);
        file.Dispose();
        
        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = [
            "ResultName", "Guid", "Parent", "StepId",
            "Step/Param1", "Step/Param2", "Step/Param3", "Step/Group/Param"
        ];
        object?[] values = [
            null, guid, parent, stepId,
            "Param1", 2, 3.141, true
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(1));
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values));
    }
    
    [Test]
    public async Task PlanRowTest()
    {
        string path = $"Tests/{nameof(ParquetFileTests)}/{nameof(PlanRowTest)}.parquet";
        
        string guid = Guid.NewGuid().ToString();

        Dictionary<string, IConvertible> parameters = new Dictionary<string, IConvertible>()
        {
            { "Param1", "Param1" },
            { "Param2", 2 },
            { "Param3", 3.141 },
            { "Group/Param", true },
        };

        ParquetFile file = new ParquetFile(path);
        file.AddPlanRow(guid, parameters);
        file.Dispose();
        
        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = [
            "ResultName", "Guid", "Parent", "StepId",
            "Plan/Param1", "Plan/Param2", "Plan/Param3", "Plan/Group/Param"
        ];
        object?[] values = [
            null, guid, null, null,
            "Param1", 2, 3.141, true
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(1));
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values));
    }
    
    // TODO: Insert tests with file merging.
    // Test one: Can files be merged at all
    // Test two: Do files keep their order when merged
    [Test]
    public async Task FileMerging()
    {
        string path = $"Tests/{nameof(ParquetFileTests)}/{nameof(FileMerging)}.parquet";
        
        string guid1 = Guid.NewGuid().ToString();
        string guid2 = Guid.NewGuid().ToString();

        Dictionary<string, IConvertible> parameters1 = new Dictionary<string, IConvertible>()
        {
            { "Param1", "Param1" },
        };
        Dictionary<string, IConvertible> parameters2 = new Dictionary<string, IConvertible>()
        {
            { "Param2", "Param2" },
        };

        ParquetFile file = new ParquetFile(path, new Options()
        {
            RowGroupSize = 1,
        });
        file.AddPlanRow(guid1, parameters1);
        file.AddPlanRow(guid2, parameters2);
        Assert.That(file.FragmentCount, Is.EqualTo(2));
        file.Dispose();
        
        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = [
            "ResultName", "Guid", "Parent", "StepId",
            "Plan/Param1", "Plan/Param2"
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        object?[] values1 = [null, guid1, null, null, "Param1", null];
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values1));
        object?[] values2 = [null, guid2, null, null, null, "Param2"];
        Assert.That(reader.ReadRow(1), Is.EquivalentTo(values2));
    }
}