using System.Data;
using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using Parquet.Schema;
using Parquet.Tests.Extensions;
using DataColumn = Parquet.Data.DataColumn;
using SchemaType = Parquet.Schema.SchemaType;

namespace Parquet.Tests;

public class FragmentTests
{
    [Test]
    public async Task CreateEmptyFileTest()
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(CreateEmptyFileTest)}.parquet";
        
        var frag = new Fragment(path, new Options());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId");
        table.AssertRows(0, row => row.AssertValues());
    }
    
    [Test]
    public async Task EmptyRowTest()
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(EmptyRowTest)}.parquet";
        
        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>(), new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId");
        table.AssertRows(1, row => row.AssertValues(null, null, null, null));
    }
    
    [Test]
    public async Task PopulateDefaultColumnsTest()
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(PopulateDefaultColumnsTest)}.parquet";

        string resultName = "test";
        string guid = Guid.NewGuid().ToString();
        string parent = Guid.NewGuid().ToString();
        string stepId = Guid.NewGuid().ToString();
        
        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            {"ResultName", resultName},
            {"Guid", guid},
            {"Parent", parent},
            {"StepId", stepId}
        }, new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId");
        table.AssertRows(1, row => row.AssertValues(resultName, guid, parent, stepId));
    }
    
    [TestCase(0, "Hello", "World")]
    [TestCase(1, "This/Is/A/Group", "Some value")]
    [TestCase(2, "Values/int32", -5432)]
    [TestCase(3, "Values/uint32", 5432u)]
    [TestCase(4, "Values/float", 3.141f)]
    [TestCase(5, "Values/double", 6.282)]
    public async Task PopulateCustomColumnsTest(int caseId, string name, IConvertible value)
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(PopulateCustomColumnsTest)}-{caseId}.parquet";
        
        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            {name, value},
        }, new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId", name);
        table.AssertRows(1, row => row.AssertValues(null, null, null, null, value));
    }
    
    [Test]
    public async Task PopulateCustomArrayColumnsTest()
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(PopulateCustomColumnsTest)}.parquet";
        
        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>(), new Dictionary<string, Array>()
        {
            {"Custom/Int/Column", Enumerable.Range(0, 50).ToArray()},
            {"Custom/Float/Column", Enumerable.Range(0, 50).Select(i => i + 0.123f).ToArray()},
        });
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId", "Custom/Int/Column", "Custom/Float/Column");
        table.AssertRows(50, (row, i) => row.AssertValues(null, null, null, null, i, i + 0.123f));
    }
    
    [TestCase(1)]
    [TestCase(25)]
    [TestCase(50)]
    [TestCase(75)]
    [TestCase(100)]
    [TestCase(150)]
    public async Task MultipleResultsKeepOrder(int rowGroupSize)
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(MultipleResultsKeepOrder)}-{rowGroupSize}.parquet";
        
        var guid1 = Guid.NewGuid().ToString();
        var guid2 = Guid.NewGuid().ToString();
    
        var frag = new Fragment(path, new Options() { RowGroupSize = rowGroupSize });
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            {"Guid", guid1},
        }, new Dictionary<string, Array>()
        {
            {"Result/data", Enumerable.Range(0, 50).ToArray()}
        });
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            {"Guid", guid2},
        }, new Dictionary<string, Array>()
        {
            {"Result/data", Enumerable.Range(50, 50).ToArray()}
        });
    
        frag.Dispose();
        
        Assert.True(System.IO.File.Exists(path));
    
        var table = await ParquetReader.ReadTableFromFileAsync(path);
        table.AssertSchema("ResultName", "Guid", "Parent", "StepId", "Result/data");
        table.AssertRows(100, (row, i) => row.AssertValues(null, i < 50 ? guid1 : guid2, null, null, i));
    }
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