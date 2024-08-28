using System.Data;
using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using Parquet.Schema;
using DataColumn = Parquet.Data.DataColumn;
using SchemaType = Parquet.Schema.SchemaType;

namespace Parquet.Tests;

public class ParquetFragmentTests
{
    [Test]
    public async Task CreateEmptyFileTest()
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CreateEmptyFileTest)}.parquet";
        
        var frag = new ParquetFragment(path, new Options());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(1));
        Assert.That(reader.Schema.Fields.Count, Is.EqualTo(4));
        Assert.That(reader.Schema.Fields[0].Name, Is.EqualTo("ResultName"));
        Assert.That(reader.Schema.Fields[1].Name, Is.EqualTo("Guid"));
        Assert.That(reader.Schema.Fields[2].Name, Is.EqualTo("Parent"));
        Assert.That(reader.Schema.Fields[3].Name, Is.EqualTo("StepId"));

        using var groupReader = reader.OpenRowGroupReader(0);
        Assert.That(groupReader.RowCount, Is.EqualTo(0));
    }
    
    [Test]
    public async Task EmptyRowTest()
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(EmptyRowTest)}.parquet";
        
        var frag = new ParquetFragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>(), new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(1));
        Assert.That(reader.Schema.Fields.Count, Is.EqualTo(4));

        var group = await reader.ReadEntireRowGroupAsync();
        using var groupReader = reader.OpenRowGroupReader(0);
        Assert.That(groupReader.RowCount, Is.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(group[0].Data.GetValue(0), Is.EqualTo(null));
            Assert.That(group[1].Data.GetValue(0), Is.EqualTo(null));
            Assert.That(group[2].Data.GetValue(0), Is.EqualTo(null));
            Assert.That(group[3].Data.GetValue(0), Is.EqualTo(null));
        });
    }
    
    [Test]
    public async Task PopulateDefaultColumnsTest()
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(PopulateDefaultColumnsTest)}.parquet";
        
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();
        
        var frag = new ParquetFragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            {"ResultName", "Test"},
            {"Guid", guid},
            {"Parent", parent},
            {"StepId", stepId}
        }, new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(1));
        Assert.That(reader.Schema.Fields.Count, Is.EqualTo(4));

        var group = await reader.ReadEntireRowGroupAsync();
        using var groupReader = reader.OpenRowGroupReader(0);
        Assert.That(groupReader.RowCount, Is.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(group[0].Data.GetValue(0), Is.EqualTo("Test"));
            Assert.That(group[1].Data.GetValue(0), Is.EqualTo(guid));
            Assert.That(group[2].Data.GetValue(0), Is.EqualTo(parent));
            Assert.That(group[3].Data.GetValue(0), Is.EqualTo(stepId));
        });
    }
    
    [TestCase(0, "Hello", "World")]
    [TestCase(1, "This/Is/A/Group", "Some value")]
    [TestCase(2, "Values/int32", -5432)]
    [TestCase(3, "Values/uint32", 5432u)]
    [TestCase(4, "Values/float", 3.141f)]
    [TestCase(5, "Values/double", 6.282)]
    public async Task PopulateCustomColumnsTest(int caseId, string name, IConvertible value)
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(PopulateCustomColumnsTest)}-{caseId}.parquet";
        
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();
        
        var frag = new ParquetFragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            {name, value},
        }, new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(1));
        Assert.That(reader.Schema.Fields.Count, Is.EqualTo(5));
        Assert.That(reader.Schema.Fields[4].Name, Is.EqualTo(name));

        var group = await reader.ReadEntireRowGroupAsync();
        using var groupReader = reader.OpenRowGroupReader(0);
        Assert.That(groupReader.RowCount, Is.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(group[0].Data.GetValue(0), Is.EqualTo(null));
            Assert.That(group[1].Data.GetValue(0), Is.EqualTo(null));
            Assert.That(group[2].Data.GetValue(0), Is.EqualTo(null));
            Assert.That(group[3].Data.GetValue(0), Is.EqualTo(null));
            Assert.That(group[4].Data.GetValue(0), Is.EqualTo(value));
        });
    }
    
    [Test]
    public async Task PopulateCustomArrayColumnsTest()
    {
        string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(PopulateCustomColumnsTest)}.parquet";
        
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();
        
        var frag = new ParquetFragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>(), new Dictionary<string, Array>()
        {
            {"Custom/Int/Column", Enumerable.Range(0, 50).ToArray()},
            {"Custom/Float/Column", Enumerable.Range(0, 50).Select(i => i + 0.123f).ToArray()},
        });
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await ParquetReader.CreateAsync(path);
        Assert.That(reader.RowGroupCount, Is.EqualTo(1));
        Assert.That(reader.Schema.Fields.Count, Is.EqualTo(6));
        Assert.That(reader.Schema.Fields[4].Name, Is.EqualTo("Custom/Int/Column"));
        Assert.That(reader.Schema.Fields[5].Name, Is.EqualTo("Custom/Float/Column"));

        var group = await reader.ReadEntireRowGroupAsync();
        using var groupReader = reader.OpenRowGroupReader(0);
        Assert.That(groupReader.RowCount, Is.EqualTo(50));
        Assert.Multiple(() =>
        {
            Assert.That(group[0].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50)));
            Assert.That(group[1].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50)));
            Assert.That(group[2].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50)));
            Assert.That(group[3].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50)));
            Assert.That(group[4].Data, Is.EquivalentTo(Enumerable.Range(0, 50)));
            Assert.That(group[5].Data, Is.EquivalentTo(Enumerable.Range(0, 50).Select(i => i + 0.123f)));
        });
    }
    
    // [Test]
    // public async Task CreateParameterRowsTest([Values("Step", "Plan")] string type)
    // {
    //     string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CreateParameterRowsTest)}.{type}.parquet";
    //
    //     var parameters = GenParameters();
    //     
    //     var frag = new ParquetFragment(path, new Options());
    //     frag.AddRows(null, null, null, null,
    //         type == "Plan" ? parameters : null,
    //         type == "Step" ? parameters : null, null);
    //     frag.WriteCache();
    //     frag.Dispose(null);
    //
    //     Assert.True(System.IO.File.Exists(path));
    //
    //     var reader = await ParquetReader.CreateAsync(path);
    //     Assert.That(reader.RowGroupCount, Is.EqualTo(1));
    //
    //     var rowGroup = await reader.ReadEntireRowGroupAsync(0);
    //     Assert.That(rowGroup.Length, Is.EqualTo(4 + parameters.Count));
    //     Assert.Multiple(() =>
    //     {
    //         Assert.That(rowGroup[0].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 1).ToArray()));
    //         Assert.That(rowGroup[1].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 1).ToArray()));
    //         Assert.That(rowGroup[2].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 1).ToArray()));
    //         Assert.That(rowGroup[3].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 1).ToArray()));
    //     });
    //     foreach (var (key, value) in parameters)
    //     {
    //         Assert.That(rowGroup.Select(r => r.Field.Name), Has.Member(type + "/" + key));
    //         Assert.That(rowGroup.First(r => r.Field.Name == type + "/" + key).Data,
    //             Is.EquivalentTo(Enumerable.Repeat<object?>(value, 1).ToArray()));
    //     }
    // }
    //
    // [Test]
    // public async Task CreateResultsTest()
    // {
    //     string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CreateResultsTest)}.parquet";
    //
    //     var results = GenResults();
    //     
    //     var frag = new ParquetFragment(path, new Options());
    //     frag.AddRows(null, null, null, null, null, null, results);
    //     frag.WriteCache();
    //     frag.Dispose(null);
    //
    //     Assert.True(System.IO.File.Exists(path));
    //
    //     var reader = await ParquetReader.CreateAsync(path);
    //     Assert.That(reader.RowGroupCount, Is.EqualTo(1));
    //
    //     var rowGroup = await reader.ReadEntireRowGroupAsync(0);
    //     Assert.That(rowGroup.Length, Is.EqualTo(4 + results.Count));
    //     Assert.Multiple(() =>
    //     {
    //         Assert.That(rowGroup[0].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50).ToArray()));
    //         Assert.That(rowGroup[1].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50).ToArray()));
    //         Assert.That(rowGroup[2].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50).ToArray()));
    //         Assert.That(rowGroup[3].Data, Is.EquivalentTo(Enumerable.Repeat<object?>(null, 50).ToArray()));
    //     });
    //     foreach (var (key, value) in results)
    //     {
    //         Assert.That(rowGroup.Select(r => r.Field.Name), Has.Member("Results/" + key));
    //         DataColumn column = rowGroup.First(r => r.Field.Name == "Results/" + key);
    //         Assert.That(column.Data.OfType<object?>().Take(value.Length), Is.EquivalentTo(value));
    //         Assert.True(column.Data.OfType<object?>().Skip(value.Length).All(d => d is null));
    //     }
    // }
    //
    // [Test]
    // public async Task MultipleRowGroupsKeepsOrder()
    // {
    //     string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(MultipleRowGroupsKeepsOrder)}.parquet";
    //
    //     var results = new Dictionary<string, Array>()
    //     {
    //         { "data", Enumerable.Range(1, 50).ToArray() }
    //     };
    //     
    //     var guid1 = Guid.NewGuid();
    //     var guid2 = Guid.NewGuid();
    //
    //     var frag = new ParquetFragment(path, new Options() { RowGroupSize = 50 });
    //     Assert.True(frag.AddRows(null, null, null, null, null, null, null));
    //     Assert.True(frag.AddRows(null, guid1, null, null, null, null, results));
    //     Assert.True(frag.AddRows(null, guid2, null, null, null, null, results));
    //
    //     frag.WriteCache();
    //     frag.Dispose(null);
    //     
    //     int guid1Val = 0;
    //     int guid2Val = 0;
    //
    //     var reader = await ParquetReader.CreateAsync(path);
    //     var fields = reader.Schema.Fields.Select((f, i) => (f.Name, i)).ToDictionary(t => t.Name, t => t.i);
    //     var guidField = fields["Guid"];
    //     var resultField = fields["Results/data"];
    //     var table = await reader.ReadAsTableAsync();
    //     for (int i = 0; i < table.Count; i++)
    //     {
    //         var row = table[i];
    //         if (row[guidField]?.Equals(guid1) ?? false)
    //         {
    //             Assert.That(row[resultField], Is.EqualTo(++guid1Val));
    //         }
    //         if (row[guidField]?.Equals(guid2) ?? false)
    //         {
    //             Assert.That(row[resultField], Is.EqualTo(++guid2Val));
    //         }
    //     }
    // }
    //
    // [TestCase]
    // public async Task MultipleFilesKeepsOrder(bool splitRowgroups = true)
    // {
    //     string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(MultipleFilesKeepsOrder)}.parquet";
    //
    //     var results1 = new Dictionary<string, Array>()
    //     {
    //         { "data1", Enumerable.Range(1, 50).ToArray() }
    //     };
    //     var results2 = new Dictionary<string, Array>()
    //     {
    //         { "data2", Enumerable.Range(1, 50).ToArray() }
    //     };
    //     
    //     var guid1 = Guid.NewGuid();
    //     var guid2 = Guid.NewGuid();
    //
    //     var frag1 = new ParquetFragment(path, new Options() { RowGroupSize = 50 });
    //     if (splitRowgroups)
    //     {
    //         Assert.True(frag1.AddRows(null, null, null, null, null, null, null));
    //     }
    //     Assert.True(frag1.AddRows(null, guid1, null, null, null, null, results1));
    //     Assert.False(frag1.AddRows(null, guid2, null, null, null, null, results2));
    //     frag1.Dispose();
    //     var frag2 = new ParquetFragment(frag1);
    //
    //     frag2.WriteCache();
    //     frag2.Dispose(new ParquetFragment[]{frag1});
    //     
    //     int guid1Val = 0;
    //     int guid2Val = 0;
    //
    //     var reader = await ParquetReader.CreateAsync(path);
    //     var fields = reader.Schema.Fields.Select((f, i) => (f.Name, i)).ToDictionary(t => t.Name, t => t.i);
    //     var guidField = fields["Guid"];
    //     var resultField1 = fields["Results/data1"];
    //     var resultField2 = fields["Results/data2"];
    //     var table = await reader.ReadAsTableAsync();
    //     for (int i = 0; i < table.Count; i++)
    //     {
    //         var row = table[i];
    //         if (row[guidField]?.Equals(guid1) ?? false)
    //         {
    //             Assert.That(row[resultField1], Is.EqualTo(++guid1Val));
    //         }
    //         if (row[guidField]?.Equals(guid2) ?? false)
    //         {
    //             Assert.That(row[resultField2], Is.EqualTo(++guid2Val));
    //         }
    //     }
    // }
    //
    // [Test]
    // public async Task FileMergingTest()
    // {
    //     string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(FileMergingTest)}.parquet";
    //
    //     var row1 = GenIds("row1");
    //     var row2 = GenIds("row2");
    //     
    //     var frag1 = new ParquetFragment(path, new Options());
    //     frag1.AddRows(row1.resultName, row1.guid, row1.parentId, row1.stepId, null, null, null);
    //     frag1.WriteCache();
    //     frag1.Dispose();
    //
    //     var frag2 = new ParquetFragment(frag1);
    //     frag2.AddRows(row2.resultName, row2.guid, row2.parentId, row2.stepId, null, null, null);
    //     frag2.WriteCache();
    //     frag2.Dispose(new []{frag1});
    //     Assert.True(System.IO.File.Exists(path));
    //
    //     var reader = await ParquetReader.CreateAsync(path);
    //     Assert.That(reader.RowGroupCount, Is.EqualTo(2));
    //     var table = await reader.ReadAsTableAsync();
    //
    //     Assert.That(table.Count, Is.EqualTo(2));
    //     for (int i = 0; i < table.Count; i++)
    //     {
    //         var row = table[i];
    //         Assert.That(row.Length, Is.EqualTo(4));
    //         var equalRow = (string)row[0]! == row1.Item1 ? row1 : row2;
    //         Assert.Multiple(() =>
    //         {
    //             Assert.That(row[0], Is.EqualTo(equalRow.resultName));
    //             Assert.That(row[1], Is.EqualTo(equalRow.guid));
    //             Assert.That(row[2], Is.EqualTo(equalRow.parentId));
    //             Assert.That(row[3], Is.EqualTo(equalRow.stepId));
    //         });
    //     }
    // }
    //
    // [Test]
    // public void CantMergeFragmentsWhenNotCompatibleTest()
    // {
    //     string path = $"Tests/{nameof(ParquetFragmentTests)}/{nameof(CantMergeFragmentsWhenNotCompatibleTest)}.parquet";
    //
    //     var frag1 = new ParquetFragment(path + "1", new Options());
    //     var frag2 = new ParquetFragment(path + "2", new Options());
    //     frag2.AddRows(null, null, null, null, null, null, null);
    //     Assert.Throws<InvalidOperationException>(() => frag2.Dispose(new []{frag1}));
    // }
}