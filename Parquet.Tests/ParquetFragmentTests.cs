using System.Data;
using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using Parquet.Schema;
using DataColumn = Parquet.Data.DataColumn;

namespace Parquet.Tests;

public class ParquetFragmentTests
{
    private (string? resultName, Guid? guid, Guid? parentId, Guid? stepId) GenIds(string? resultName = null, bool hasGuid = true, bool hasParentId = true, bool hasStepId = true)
    {
        return (resultName,
            hasGuid ? Guid.NewGuid() : null,
            hasParentId ? Guid.NewGuid() : null,
            hasStepId ? Guid.NewGuid() : null);
    }

    private Dictionary<string, IConvertible> GenParameters()
    {
        return new Dictionary<string, IConvertible>()
        {
            {"param1", 0},
            {"group/param2", 1.41},
            {"this/is/a/lot/of/groups/param3", 3.14},
            {"param4", "value"},
        };
    }
    
    private Dictionary<string, Array> GenResults()
    {
        return new Dictionary<string, Array>()
        {
            { "param1", Enumerable.Range(0, 50).ToArray() },
            { "group/param2", Enumerable.Range(25, 25).Reverse().ToArray() },
            { "group/strings/param3", Enumerable.Repeat("hello", 1).ToArray() },
            { "group/strings/param4", Enumerable.Repeat("world", 37).ToArray() },
        };
    }
    
    private int _testCase = 0;
    [Test]
    public async Task CreatingSimpleFileTest([Values(null, "Result")] string? resultName,
        [Values(true, false)] bool hasGuid,
        [Values(true, false)] bool hasParentId,
        [Values(true, false)] bool hasStepId)
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CreatingSimpleFileTest)}{_testCase++}.parquet";
        (resultName, var guid, var parentId, var stepId) = GenIds(resultName, hasGuid, hasParentId, hasStepId);
        
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

        var parameters = GenParameters();
        
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

        var results = GenResults();
        
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
            DataColumn column = rowGroup.First(r => r.Field.Name == "Results/" + key);
            Assert.That(column.Data.OfType<object?>().Take(value.Length), Is.EquivalentTo(value));
            Assert.True(column.Data.OfType<object?>().Skip(value.Length).All(d => d is null));
        }
    }

    [Test]
    public async Task MultipleRowGroupsKeepsOrder()
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(MultipleRowGroupsKeepsOrder)}.parquet";

        var results = new Dictionary<string, Array>()
        {
            { "data", Enumerable.Range(1, 50).ToArray() }
        };
        
        var guid1 = Guid.NewGuid();
        var guid2 = Guid.NewGuid();

        var frag = new ParquetFragment(path, new ParquetResult.Options() { RowGroupSize = 50 });
        Assert.True(frag.AddRows(null, null, null, null, null, null, null));
        Assert.True(frag.AddRows(null, guid1, null, null, null, null, results));
        Assert.True(frag.AddRows(null, guid2, null, null, null, null, results));

        frag.WriteCache();
        frag.Dispose(null);
        
        int guid1Val = 0;
        int guid2Val = 0;

        var reader = await ParquetReader.CreateAsync(path);
        var fields = reader.Schema.Fields.Select((f, i) => (f.Name, i)).ToDictionary(t => t.Name, t => t.i);
        var guidField = fields["Guid"];
        var resultField = fields["Results/data"];
        var table = await reader.ReadAsTableAsync();
        for (int i = 0; i < table.Count; i++)
        {
            var row = table[i];
            if (row[guidField]?.Equals(guid1) ?? false)
            {
                Assert.That(row[resultField], Is.EqualTo(++guid1Val));
            }
            if (row[guidField]?.Equals(guid2) ?? false)
            {
                Assert.That(row[resultField], Is.EqualTo(++guid2Val));
            }
        }
    }
    
    [TestCase]
    public async Task MultipleFilesKeepsOrder(bool splitRowgroups = true)
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(MultipleFilesKeepsOrder)}.parquet";

        var results1 = new Dictionary<string, Array>()
        {
            { "data1", Enumerable.Range(1, 50).ToArray() }
        };
        var results2 = new Dictionary<string, Array>()
        {
            { "data2", Enumerable.Range(1, 50).ToArray() }
        };
        
        var guid1 = Guid.NewGuid();
        var guid2 = Guid.NewGuid();

        var frag1 = new ParquetFragment(path, new ParquetResult.Options() { RowGroupSize = 50 });
        if (splitRowgroups)
        {
            Assert.True(frag1.AddRows(null, null, null, null, null, null, null));
        }
        Assert.True(frag1.AddRows(null, guid1, null, null, null, null, results1));
        Assert.False(frag1.AddRows(null, guid2, null, null, null, null, results2));
        frag1.Dispose();
        var frag2 = new ParquetFragment(frag1);

        frag2.WriteCache();
        frag2.Dispose(new ParquetFragment[]{frag1});
        
        int guid1Val = 0;
        int guid2Val = 0;

        var reader = await ParquetReader.CreateAsync(path);
        var fields = reader.Schema.Fields.Select((f, i) => (f.Name, i)).ToDictionary(t => t.Name, t => t.i);
        var guidField = fields["Guid"];
        var resultField1 = fields["Results/data1"];
        var resultField2 = fields["Results/data2"];
        var table = await reader.ReadAsTableAsync();
        for (int i = 0; i < table.Count; i++)
        {
            var row = table[i];
            if (row[guidField]?.Equals(guid1) ?? false)
            {
                Assert.That(row[resultField1], Is.EqualTo(++guid1Val));
            }
            if (row[guidField]?.Equals(guid2) ?? false)
            {
                Assert.That(row[resultField2], Is.EqualTo(++guid2Val));
            }
        }
    }

    [Test]
    public async Task FileMergingTest()
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(FileMergingTest)}.parquet";

        var row1 = GenIds("row1");
        var row2 = GenIds("row2");
        
        var frag1 = new ParquetFragment(path, new ParquetResult.Options());
        frag1.AddRows(row1.resultName, row1.guid, row1.parentId, row1.stepId, null, null, null);
        frag1.WriteCache();
        frag1.Dispose();

        var frag2 = new ParquetFragment(frag1);
        frag2.AddRows(row2.resultName, row2.guid, row2.parentId, row2.stepId, null, null, null);
        frag2.WriteCache();
        frag2.Dispose(new []{frag1});
        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(2));
        var table = await reader.ReadAsTableAsync();

        Assert.That(table.Count, Is.EqualTo(2));
        for (int i = 0; i < table.Count; i++)
        {
            var row = table[i];
            Assert.That(row.Length, Is.EqualTo(4));
            var equalRow = (string)row[0]! == row1.Item1 ? row1 : row2;
            Assert.Multiple(() =>
            {
                Assert.That(row[0], Is.EqualTo(equalRow.resultName));
                Assert.That(row[1], Is.EqualTo(equalRow.guid));
                Assert.That(row[2], Is.EqualTo(equalRow.parentId));
                Assert.That(row[3], Is.EqualTo(equalRow.stepId));
            });
        }
    }

    [Test]
    public void CantMergeFragmentsWhenNotCompatibleTest()
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CantMergeFragmentsWhenNotCompatibleTest)}.parquet";

        var frag1 = new ParquetFragment(path + "1", new ParquetResult.Options());
        var frag2 = new ParquetFragment(path + "2", new ParquetResult.Options());
        frag2.AddRows(null, null, null, null, null, null, null);
        Assert.Throws<InvalidOperationException>(() => frag2.Dispose(new []{frag1}));
    }
}