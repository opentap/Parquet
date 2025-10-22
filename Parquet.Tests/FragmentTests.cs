using Newtonsoft.Json;
using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using OpenTap.Plugins.Parquet.Core;
using ZstdSharp.Unsafe;
using JsonSerializer = System.Text.Json.JsonSerializer;

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

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task EmptyRowTest()
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(EmptyRowTest)}.parquet";

        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>(), new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));

        Assert.That(reader.RowGroupCount, Is.EqualTo(1));
        object?[] values = [null, null, null, null];
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values));
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
            { "ResultName", resultName },
            { "Guid", guid },
            { "Parent", parent },
            { "StepId", stepId }
        }, new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));

        Assert.That(reader.Count, Is.EqualTo(1));
        for (int i = 0; i < 1; i++)
        {
            object?[] values = [resultName, guid, parent, stepId];
            Assert.That(reader.ReadRow(i), Is.EquivalentTo(values));
        }
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
            { name, value },
        }, new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", name];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(1));
        object?[] values = [null, null, null, null, value];
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values));
    }

    [Test]
    public async Task PopulateCustomArrayColumnsTest()
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(PopulateCustomColumnsTest)}.parquet";

        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>(), new Dictionary<string, Array>()
        {
            { "Custom/Int/Column", Enumerable.Range(0, 50).ToArray() },
            { "Custom/Float/Column", Enumerable.Range(0, 50).Select(i => i + 0.123f).ToArray() },
        });
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", "Custom/Int/Column", "Custom/Float/Column"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(50));
        for (int i = 0; i < 50; i++)
        {
            object?[] values = [null, null, null, null, i, i + 0.123f];
            Assert.That(reader.ReadRow(i), Is.EquivalentTo(values));
        }
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
            { "Guid", guid1 },
        }, new Dictionary<string, Array>()
        {
            { "Result/data", Enumerable.Range(0, 50).ToArray() }
        });
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            { "Guid", guid2 },
        }, new Dictionary<string, Array>()
        {
            { "Result/data", Enumerable.Range(50, 50).ToArray() }
        });

        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", "Result/data"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(100));
        for (int i = 0; i < 100; i++)
        {
            object?[] values = [null, i < 50 ? guid1 : guid2, null, null, i];
            Assert.That(reader.ReadRow(i), Is.EquivalentTo(values));
        }
    }

    [TestCase(1)]
    [TestCase(24)]
    [TestCase(25)]
    [TestCase(26)]
    [TestCase(50)]
    [TestCase(75)]
    public async Task ArraysOfDifferentSizeTest(int rowGroupSize)
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(ArraysOfDifferentSizeTest)}-{rowGroupSize}.parquet";

        var results = new Dictionary<string, Array>()
        {
            { "Column1", Enumerable.Range(0, 50).ToArray() },
            { "Column2", Enumerable.Range(0, 25).ToArray() },
        };

        var frag = new Fragment(path, new Options() { RowGroupSize = rowGroupSize });
        frag.AddRows(new Dictionary<string, IConvertible>(), results);
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", "Column1", "Column2"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(50));
        for (int i = 0; i < 50; i++)
        {
            object?[] values = [null, null, null, null, i, i < 25 ? i : null];
            Assert.That(reader.ReadRow(i), Is.EquivalentTo(values));
        }
    }

    [TestCase(1, new [] { 0, 1, 2 }, new [] {0.1f, 0.2f, 0.3f },"Custom/Int32", "Custom/Single")]
    [TestCase(2, new [] { 0, 1, 2 }, new [] {0.1, 0.2, 0.3 },"Custom/Int32", "Custom/Double")]
    [TestCase(3, new [] { 0.1, 1.2, 2.3 }, new [] {0.1f, 0.2f, 0.3f },"Custom/Double", "Custom/Single")]
    [TestCase(4, new [] { "String", "Test", "Hello" }, new [] {0.1f, 0.2f, 0.3f },"Custom/String", "Custom/Single")]
    public async Task ArrayColumnTypeCollisionTest(int testCase, Array arr1, Array arr2, string name1, string name2)
    {
        string path = $"Tests/{nameof(FragmentTests)}/{nameof(PopulateCustomColumnsTest)}-{testCase}.parquet";
        
        var guid1 = Guid.NewGuid().ToString();
        var guid2 = Guid.NewGuid().ToString();
        
        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            {"Guid", guid1},
        }, new Dictionary<string, Array>()
        {
            {"Custom", arr1},
        });
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            {"Guid", guid2},
        }, new Dictionary<string, Array>()
        {
            {"Custom", arr2},
        });
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));
        
        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", name1, name2];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(arr1.Length + arr2.Length));
        for (int i = 0; i < arr1.Length; i++)
        {
            object?[] values = [null, guid1, null, null, arr1.GetValue(i), null];
            Assert.That(reader.ReadRow(i), Is.EquivalentTo(values));
        }
        for (int i = 0; i < arr2.Length; i++)
        {
            object?[] values = [null, guid2, null, null, null, arr2.GetValue(i)];
            Assert.That(reader.ReadRow(i + arr1.Length), Is.EquivalentTo(values));
        }

        var metadata = reader.CustomMetadata;
        var mappings = new Dictionary<string, string>()
        {
            [name1] = "Custom",
            [name2] = "Custom",
        };
        Assert.That(metadata["Mappings"], Is.EqualTo(JsonSerializer.Serialize(mappings)));
    }
}