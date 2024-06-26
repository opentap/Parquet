using NUnit.Framework;
using OpenTap.Plugins.Parquet;

namespace Parquet.Tests;

public class ParquetFragmentTests
{
    private int _testCase = 0;
    [Test]
    public async Task CreatingSimpleFileTest([Values(null, "Result")] string? resultName,
        [Values(true, false)] bool hasGuid,
        [Values(true, false)] bool hasParentId,
        [Values(true, false)] bool hasStepId)
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CreatingSimpleFileTest)}{_testCase++}.parquet";
        Guid? guid = hasGuid ? Guid.NewGuid() : null;
        Guid? parentId = hasParentId ? Guid.NewGuid() : null;
        Guid? stepId = hasStepId ? Guid.NewGuid() : null;
        
        var frag = new ParquetFragment(path, new ParquetResult.Options());
        frag.AddRows(resultName, guid, parentId, stepId, null, null, null);
        frag.WriteCache();
        frag.Dispose(null);

        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(1));

        var rowGroup = await reader.ReadEntireRowGroupAsync(0);
        Assert.That(rowGroup.Length, Is.EqualTo(4));
        Assert.Multiple(() =>
        {
            Assert.That(rowGroup[0].Data, Is.EquivalentTo(Enumerable.Repeat(resultName, 1).ToArray()));
            Assert.That(rowGroup[0].Field.Name, Is.EqualTo("ResultName"));
            Assert.That(rowGroup[1].Data, Is.EquivalentTo(Enumerable.Repeat(guid, 1).ToArray()));
            Assert.That(rowGroup[1].Field.Name, Is.EqualTo("Guid"));
            Assert.That(rowGroup[2].Data, Is.EquivalentTo(Enumerable.Repeat(parentId, 1).ToArray()));
            Assert.That(rowGroup[2].Field.Name, Is.EqualTo("Parent"));
            Assert.That(rowGroup[3].Data, Is.EquivalentTo(Enumerable.Repeat(stepId, 1).ToArray()));
            Assert.That(rowGroup[3].Field.Name, Is.EqualTo("StepId"));
        });
    }
    
    [Test]
    public async Task CreateParameterRowsTest([Values("Step", "Plan")] string type)
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CreateParameterRowsTest)}.{type}.parquet";

        var parameters = new Dictionary<string, IConvertible>()
        {
            {"param1", 0},
            {"group/param2", 1.41},
            {"this/is/a/lot/of/groups/param3", 3.14},
            {"param4", "value"},
        };
        
        var frag = new ParquetFragment(path, new ParquetResult.Options());
        frag.AddRows(null, null, null, null,
            type == "Plan" ? parameters : null,
            type == "Step" ? parameters : null, null);
        frag.WriteCache();
        frag.Dispose(null);

        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(1));

        var rowGroup = await reader.ReadEntireRowGroupAsync(0);
        Assert.That(rowGroup.Length, Is.EqualTo(4 + parameters.Count));
        Assert.Multiple(() =>
        {
            Assert.That(rowGroup[0].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 1).ToArray()));
            Assert.That(rowGroup[1].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 1).ToArray()));
            Assert.That(rowGroup[2].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 1).ToArray()));
            Assert.That(rowGroup[3].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 1).ToArray()));
        });
        foreach (var (key, value) in parameters)
        {
            Assert.That(rowGroup.Select(r => r.Field.Name), Has.Member(type + "/" + key));
            Assert.That(rowGroup.First(r => r.Field.Name == type + "/" + key).Data,
                Is.EquivalentTo(Enumerable.Repeat<object?>(value, 1).ToArray()));
        }
    }
    
    [Test]
    public async Task CreateResultsTest()
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CreateResultsTest)}.parquet";

        var results = new Dictionary<string, Array>()
        {
            {"param1", Enumerable.Range(0, 50).ToArray()},
            {"group/param2", Enumerable.Range(25, 50).Reverse().ToArray()},
            {"group/strings/param3", Enumerable.Repeat("hello", 50).ToArray()},
            {"group/strings/param4", Enumerable.Repeat("world", 50).ToArray()},
        };
        
        var frag = new ParquetFragment(path, new ParquetResult.Options());
        frag.AddRows(null, null, null, null, null, null, results);
        frag.WriteCache();
        frag.Dispose(null);

        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(1));

        var rowGroup = await reader.ReadEntireRowGroupAsync(0);
        Assert.That(rowGroup.Length, Is.EqualTo(4 + results.Count));
        Assert.Multiple(() =>
        {
            Assert.That(rowGroup[0].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50).ToArray()));
            Assert.That(rowGroup[1].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50).ToArray()));
            Assert.That(rowGroup[2].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50).ToArray()));
            Assert.That(rowGroup[3].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50).ToArray()));
        });
        foreach (var (key, value) in results)
        {
            Assert.That(rowGroup.Select(r => r.Field.Name), Has.Member("Results/" + key));
            Assert.That(rowGroup.First(r => r.Field.Name == "Results/" + key).Data,
                Is.EquivalentTo(value));
        }
    }
}