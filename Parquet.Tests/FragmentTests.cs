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
        string path = Path.GetTempFileName();

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
        string path = Path.GetTempFileName();

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
        string path = Path.GetTempFileName();

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

    [TestCase("Hello", "World")]
    [TestCase("This/Is/A/Group", "Some value")]
    [TestCase("Values/int32", -5432)]
    [TestCase("Values/uint32", 5432u)]
    [TestCase("Values/float", 3.141f)]
    [TestCase("Values/double", 6.282)]
    public async Task PopulateCustomColumnsTest(string name, IConvertible value)
    {
        string path = Path.GetTempFileName();
        
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

    [TestCase("Column", "0.1f", 0.1f)]
    [TestCase("Column", 0.1f, "0.1f")]
    public async Task ColumnTypeCollisionTest(string name, IConvertible value1, IConvertible value2)
    {
        string path = Path.GetTempFileName();

        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            { name, value1 },
        },  new Dictionary<string, Array>());
        frag.AddRows(new Dictionary<string, IConvertible>()
        {
            { name, value2 },
        },  new Dictionary<string, Array>());
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", name + "/" + value1.GetType().Name, name + "/" + value2.GetType().Name];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(2));
        object?[] values1 = [null, null, null, null, value1, null];
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values1));
        object?[] values2 = [null, null, null, null, null, value2];
        Assert.That(reader.ReadRow(1), Is.EquivalentTo(values2));
    }

    private enum MyEnum
    {
        A, B, C
    }
    
    public static IEnumerable<object[]> PopulateDefaultColumnsSource()
    {
        yield return [false, "Custom/Int/Column", Enumerable.Range(0, 50).ToArray()];
        yield return [false, "Custom/Float/Column", Enumerable.Range(0, 100).Select(i => i + 0.123f).ToArray()];
        yield return [true, "Enum/Column", Enumerable.Range(0, 10).Select(i => (MyEnum)(i % 3)).ToArray()];
        yield return [true, "Do/Objects/Work", Enumerable.Range(0, 100).Select(i => new object()).ToArray()];
    }

    [TestCaseSource(nameof(PopulateDefaultColumnsSource))]
    public async Task PopulateCustomArrayColumnsTest(bool convertToString, string name, Array expected)
    {
        string path = Path.GetTempFileName();

        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, IConvertible>(), new Dictionary<string, Array>()
        {
            { name, expected },
        });
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", name];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(expected.Length));
        for (int i = 0; i < expected.Length; i++)
        {
            object? expectedValue = expected.GetValue(i);
            object?[] values = [null, null, null, null, convertToString ? expectedValue?.ToString() : expectedValue];
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
        string path = Path.GetTempFileName();

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
        string path = Path.GetTempFileName();

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

    public static IEnumerable<object[]> ArrayColumnTypeCollisionSource()
    {
        yield return [new float?[] { 0.1f, 0.2f, 0.3f, null }, new string[] { "1", "2" },
            "Custom/Single", "Custom/String"];
    }

    [TestCaseSource(nameof(ArrayColumnTypeCollisionSource))]
    [TestCase(new [] { 0, 1, 2 }, new [] {0.1f, 0.2f, 0.3f },"Custom/Int32", "Custom/Single")]
    [TestCase(new [] { 0, 1, 2 }, new [] {0.1, 0.2, 0.3 },"Custom/Int32", "Custom/Double")]
    [TestCase(new [] { 0.1, 1.2, 2.3 }, new [] {0.1f, 0.2f, 0.3f },"Custom/Double", "Custom/Single")]
    [TestCase(new [] { "String", "Test", "Hello" }, new [] {0.1f, 0.2f, 0.3f },"Custom/String", "Custom/Single")]
    public async Task ArrayColumnTypeCollisionTest(Array arr1, Array arr2, string name1, string name2)
    {
        string path = Path.GetTempFileName();
        
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