using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using OpenTap.Plugins.Parquet.Core;

namespace Parquet.Tests;

public class ParquetFileTests
{
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
    
    [Test]
    public async Task DuplicateResultAndParameterNamesTest()
    {
        string path = Path.GetTempFileName();

        string resultName = "Test";
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();

        // Two parameters named "P" and two result columns named "V" published together.
        ILookup<string, IConvertible> parameters = new (string Key, IConvertible Value)[]
        {
            ("P", 1),
            ("P", 2),
        }.ToLookup(x => x.Key, x => x.Value);
        ILookup<string, Array> results = new (string Key, Array Value)[]
        {
            ("V", Enumerable.Range(0, 5).ToArray()),
            ("V", Enumerable.Range(100, 5).ToArray()),
        }.ToLookup(x => x.Key, x => x.Value);

        ParquetFile file = new ParquetFile(path);
        file.AddResultRow(resultName, guid, parent, stepId, parameters, results);
        file.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields =
        [
            "ResultName", "Guid", "Parent", "StepId",
            "Step/P", "Step/P/1", "Result/V", "Result/V/1"
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields),
            "Duplicate parameter and result names should be prefixed and split into indexed columns.");
        Assert.That(reader.Count, Is.EqualTo(5));
        for (int i = 0; i < 5; i++)
        {
            Assert.That(reader.ReadCell(i, "Step/P"), Is.EqualTo(1), $"row {i}: first parameter value.");
            Assert.That(reader.ReadCell(i, "Step/P/1"), Is.EqualTo(2), $"row {i}: second parameter value.");
            Assert.That(reader.ReadCell(i, "Result/V"), Is.EqualTo(i), $"row {i}: first result column.");
            Assert.That(reader.ReadCell(i, "Result/V/1"), Is.EqualTo(100 + i), $"row {i}: second result column.");
        }

        var mappings = new Dictionary<string, string>
        {
            ["Step/P/1"] = "Step/P",
            ["Result/V/1"] = "Result/V",
        };
        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(System.Text.Json.JsonSerializer.Serialize(mappings)),
            "Both indexed columns should map back to their prefixed display names.");
    }

    [Test]
    public async Task DuplicatePlanParameterNamesTest()
    {
        string path = Path.GetTempFileName();

        string guid = Guid.NewGuid().ToString();

        ILookup<string, IConvertible> parameters = new (string Key, IConvertible Value)[]
        {
            ("Operator", "Alice"),
            ("Operator", "Bob"),
            ("Operator", "Carol"),
        }.ToLookup(x => x.Key, x => x.Value);

        ParquetFile file = new ParquetFile(path);
        file.AddPlanRow(guid, parameters);
        file.Dispose();

        var reader = await Reader.CreateAsync(path);
        string[] fields =
        [
            "ResultName", "Guid", "Parent", "StepId",
            "Plan/Operator", "Plan/Operator/1", "Plan/Operator/2"
        ];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields),
            "Three plan parameters with the same name should become sequential indexed columns.");
        Assert.That(reader.Count, Is.EqualTo(1));
        Assert.That(reader.ReadCell(0, "Plan/Operator"), Is.EqualTo("Alice"));
        Assert.That(reader.ReadCell(0, "Plan/Operator/1"), Is.EqualTo("Bob"));
        Assert.That(reader.ReadCell(0, "Plan/Operator/2"), Is.EqualTo("Carol"));

        var mappings = new Dictionary<string, string>
        {
            ["Plan/Operator/1"] = "Plan/Operator",
            ["Plan/Operator/2"] = "Plan/Operator",
        };
        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(System.Text.Json.JsonSerializer.Serialize(mappings)));
    }

    [Test]
    public async Task DuplicateResultNamesSurviveFragmentMergeTest()
    {
        string path = Path.GetTempFileName();

        string guid1 = Guid.NewGuid().ToString();
        string guid2 = Guid.NewGuid().ToString();

        // Call 1 splits duplicate "V" into "V"/"V/1". Call 2 introduces a brand new column, which
        // changes the schema and forces a second fragment. The merge must keep the split column names
        // from the first fragment intact - the whole point of never renaming existing columns.
        ParquetFile file = new ParquetFile(path, new Options { RowGroupSize = 1 });

        ILookup<string, Array> results1 = new (string Key, Array Value)[]
        {
            ("V", new[] { 1 }),
            ("V", new[] { 10 }),
        }.ToLookup(x => x.Key, x => x.Value);
        ILookup<string, Array> results2 = new (string Key, Array Value)[]
        {
            ("W", new[] { 2 }),
        }.ToLookup(x => x.Key, x => x.Value);

        var noParams = Array.Empty<(string, IConvertible)>().ToLookup(x => x.Item1, x => x.Item2);
        file.AddResultRow("R", guid1, "", "", noParams, results1);
        file.AddResultRow("R", guid2, "", "", noParams, results2);
        Assert.That(file.FragmentCount, Is.GreaterThan(1), "A new column in call 2 should produce a second fragment.");
        file.Dispose();

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", "Result/V", "Result/V/1", "Result/W"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields),
            "Split duplicate-result columns must retain the same names after merging fragments.");
        Assert.That(reader.Count, Is.EqualTo(2));
        Assert.That(reader.ReadCell(0, "Result/V"), Is.EqualTo(1), "fragment 1 first column.");
        Assert.That(reader.ReadCell(0, "Result/V/1"), Is.EqualTo(10), "fragment 1 second column.");
        Assert.That(reader.ReadCell(0, "Result/W"), Is.EqualTo(null), "column W absent in fragment 1.");
        Assert.That(reader.ReadCell(1, "Result/V"), Is.EqualTo(null), "V absent in fragment 2.");
        Assert.That(reader.ReadCell(1, "Result/V/1"), Is.EqualTo(null), "V/1 absent in fragment 2.");
        Assert.That(reader.ReadCell(1, "Result/W"), Is.EqualTo(2), "fragment 2 new column.");

        var mappings = new Dictionary<string, string> { ["Result/V/1"] = "Result/V" };
        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(System.Text.Json.JsonSerializer.Serialize(mappings)),
            "Mapping for the split column must survive the merge.");
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