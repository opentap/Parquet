using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using OpenTap.Plugins.Parquet.Core;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Parquet.Tests;

public class ParquetFileTests
{
    [Test]
    public async Task DuplicateResultNameMappingTest()
    {
        string path = Path.GetTempFileName();

        string resultName = "Test";
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();

        // The caller writes two results that share the same logical name "a" at the same time.
        // It is the caller's responsibility to de-duplicate the keys before writing, so the two
        // arrays are written under "a" and "a/1" respectively.
        Dictionary<string, Array> results = new Dictionary<string, Array>()
        {
            { "a", Enumerable.Range(0, 50).ToArray() },
            { "a/1", Enumerable.Repeat("value", 50).ToArray() },
        };

        ParquetFile file = new ParquetFile(path);
        file.AddResultRow(resultName, guid, parent, stepId, new Dictionary<string, IConvertible>(), results);
        // The caller registers the mapping so both physical columns map back to the same display name.
        file.AddNameMapping("Result/a/1", "Result/a");
        file.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields =
        [
            "ResultName", "Guid", "Parent", "StepId",
            "Result/a", "Result/a/1"
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(50));
        for (int i = 0; i < 50; i++)
        {
            object?[] values = [resultName, guid, parent, stepId, i, "value"];
            Assert.That(reader.ReadRow(i), Is.EquivalentTo(values));
        }

        var mappings = new Dictionary<string, string>()
        {
            ["Result/a/1"] = "Result/a",
        };
        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(JsonSerializer.Serialize(mappings)));
    }

    [Test]
    public async Task DuplicateResultNameLookupTest()
    {
        string path = Path.GetTempFileName();

        string resultName = "Test";
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();

        // Two results share the same logical name "a" at the same time. Passing them as a lookup
        // lets ParquetFile disambiguate the columns and register the name mapping automatically.
        ILookup<string, Array> results = new[]
        {
            ("a", (Array)Enumerable.Range(0, 50).ToArray()),
            ("a", (Array)Enumerable.Repeat("value", 50).ToArray()),
            ("b", (Array)Enumerable.Range(100, 50).ToArray()),
        }.ToLookup(t => t.Item1, t => t.Item2);

        ILookup<string, IConvertible> parameters = Array.Empty<(string, IConvertible)>()
            .ToLookup(t => t.Item1, t => t.Item2);

        ParquetFile file = new ParquetFile(path);
        file.AddResultRow(resultName, guid, parent, stepId, parameters, results);
        file.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields =
        [
            "ResultName", "Guid", "Parent", "StepId",
            "Result/a", "Result/a/1", "Result/b"
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(50));
        for (int i = 0; i < 50; i++)
        {
            object?[] values = [resultName, guid, parent, stepId, i, "value", 100 + i];
            Assert.That(reader.ReadRow(i), Is.EquivalentTo(values));
        }

        // The unique column keeps its name; the duplicate is suffixed and mapped back to "Result/a".
        var mappings = new Dictionary<string, string>()
        {
            ["Result/a/1"] = "Result/a",
        };
        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(JsonSerializer.Serialize(mappings)));
    }

    [Test]
    public async Task DuplicateParameterNameLookupTest()
    {
        string path = Path.GetTempFileName();

        string resultName = "Test";
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();

        // Two parameters share the same logical name "a" at the same time. Passing them as a lookup
        // lets ParquetFile disambiguate the columns and register the name mapping automatically.
        ILookup<string, IConvertible> parameters = new (string, IConvertible)[]
        {
            ("a", "first"),
            ("a", "second"),
            ("b", 42),
        }.ToLookup(t => t.Item1, t => t.Item2);
        ILookup<string, Array> results = Array.Empty<(string, Array)>()
            .ToLookup(t => t.Item1, t => t.Item2);

        ParquetFile file = new ParquetFile(path);
        file.AddResultRow(resultName, guid, parent, stepId, parameters, results);
        file.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields =
        [
            "ResultName", "Guid", "Parent", "StepId",
            "Step/a", "Step/a/1", "Step/b"
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(1));
        object?[] values = [resultName, guid, parent, stepId, "first", "second", 42];
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values));

        // The unique column keeps its name; the duplicate is suffixed and mapped back to "Step/a".
        var mappings = new Dictionary<string, string>()
        {
            ["Step/a/1"] = "Step/a",
        };
        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(JsonSerializer.Serialize(mappings)));
    }

    [Test]
    public async Task DuplicatePlanParameterNameLookupTest()
    {
        string path = Path.GetTempFileName();

        string guid = Guid.NewGuid().ToString();

        ILookup<string, IConvertible> parameters = new (string, IConvertible)[]
        {
            ("a", "first"),
            ("a", "second"),
            ("b", 42),
        }.ToLookup(t => t.Item1, t => t.Item2);

        ParquetFile file = new ParquetFile(path);
        file.AddPlanRow(guid, parameters);
        file.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields =
        [
            "ResultName", "Guid", "Parent", "StepId",
            "Plan/a", "Plan/a/1", "Plan/b"
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(1));
        object?[] values = [null, guid, null, null, "first", "second", 42];
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values));

        var mappings = new Dictionary<string, string>()
        {
            ["Plan/a/1"] = "Plan/a",
        };
        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(JsonSerializer.Serialize(mappings)));
    }

    [Test]
    public async Task ResultRowTest()
    {
        string path = Path.GetTempFileName();
        
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
        string path = Path.GetTempFileName();
        
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
        string path = Path.GetTempFileName();
        
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
        string path = Path.GetTempFileName();
        
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