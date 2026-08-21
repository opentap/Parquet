using NUnit.Framework;
using OpenTap.Plugins.Parquet;
using OpenTap.Plugins.Parquet.Core;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Parquet.Tests;

public class FragmentTests
{
    // Wrap single-valued dictionaries into the List-based shape that Fragment.AddRows now expects.
    private static Dictionary<string, List<IConvertible>> Values(Dictionary<string, IConvertible> values)
    {
        return values.ToDictionary(kvp => kvp.Key, kvp => new List<IConvertible> { kvp.Value });
    }

    private static Dictionary<string, List<Array>> Arrays(Dictionary<string, Array> arrayValues)
    {
        return arrayValues.ToDictionary(kvp => kvp.Key, kvp => new List<Array> { kvp.Value });
    }

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
        frag.AddRows(Values(new Dictionary<string, IConvertible>()), Arrays(new Dictionary<string, Array>()));
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
        frag.AddRows(Values(new Dictionary<string, IConvertible>()
        {
            { "ResultName", resultName },
            { "Guid", guid },
            { "Parent", parent },
            { "StepId", stepId }
        }), Arrays(new Dictionary<string, Array>()));
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
        frag.AddRows(Values(new Dictionary<string, IConvertible>()
        {
            { name, value },
        }), Arrays(new Dictionary<string, Array>()));
        frag.Dispose();

        Assert.True(System.IO.File.Exists(path));

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", name];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
        Assert.That(reader.Count, Is.EqualTo(1));
        object?[] values = [null, null, null, null, value];
        Assert.That(reader.ReadRow(0), Is.EquivalentTo(values));
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
        frag.AddRows(Values(new Dictionary<string, IConvertible>()), Arrays(new Dictionary<string, Array>()
        {
            { name, expected },
        }));
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
        frag.AddRows(Values(new Dictionary<string, IConvertible>()
        {
            { "Guid", guid1 },
        }), Arrays(new Dictionary<string, Array>()
        {
            { "Result/data", Enumerable.Range(0, 50).ToArray() }
        }));
        frag.AddRows(Values(new Dictionary<string, IConvertible>()
        {
            { "Guid", guid2 },
        }), Arrays(new Dictionary<string, Array>()
        {
            { "Result/data", Enumerable.Range(50, 50).ToArray() }
        }));

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
        frag.AddRows(Values(new Dictionary<string, IConvertible>()), Arrays(results));
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

    #region Duplication & collision

    /// A single Fragment.AddRows call: scalar values and array values, each of which may contain
    /// several entries that share the same name (that is what triggers duplication/collision).
    public sealed class AddRowsCall
    {
        private readonly List<(string name, IConvertible value)> _scalars = new();
        private readonly List<(string name, Array value)> _arrays = new();

        public AddRowsCall Scalar(string name, IConvertible value)
        {
            _scalars.Add((name, value));
            return this;
        }

        public AddRowsCall Array(string name, Array value)
        {
            _arrays.Add((name, value));
            return this;
        }

        public Dictionary<string, List<IConvertible>> ScalarValues() => Group(_scalars);
        public Dictionary<string, List<Array>> ArrayValues() => Group(_arrays);

        // Group by name so entries that share a name end up in the same list (the duplicate case).
        private static Dictionary<string, List<T>> Group<T>(IEnumerable<(string name, T value)> entries)
        {
            return entries
                .GroupBy(e => e.name, e => e.value)
                .ToDictionary(g => g.Key, g => g.ToList());
        }
    }

    /// The expected contents of one physical column after all rows have been written.
    /// <paramref name="Values"/> is aligned to the full set of rows in the file (nulls for gaps).
    public sealed record ExpectedColumn(string Name, object?[] Values);

    /// One end-to-end duplication/collision scenario, self describing so failures explain themselves.
    public sealed class CollisionCase
    {
        public required string Description { get; init; }
        public required List<AddRowsCall> Calls { get; init; }
        public required List<ExpectedColumn> Columns { get; init; }
        public Dictionary<string, string> Mappings { get; init; } = new();

        public override string ToString() => Description;
    }

    private static readonly string[] DefaultFields = ["ResultName", "Guid", "Parent", "StepId"];

    public static IEnumerable<CollisionCase> CollisionCases()
    {
        // --- Scalar type collisions across separate calls (was ColumnTypeCollisionTest) ---
        yield return new CollisionCase
        {
            Description = "Scalar type collision: string then double under 'Column' across two calls " +
                          "-> first keeps 'Column', second becomes 'Column/1'",
            Calls =
            [
                new AddRowsCall().Scalar("Column", "0.1f"),
                new AddRowsCall().Scalar("Column", 0.1),
            ],
            Columns =
            [
                new ExpectedColumn("Column", ["0.1f", null]),
                new ExpectedColumn("Column/1", [null, 0.1]),
            ],
            Mappings = new() { ["Column/1"] = "Column" },
        };
        yield return new CollisionCase
        {
            Description = "Scalar type collision: double then string under 'Column' across two calls " +
                          "-> first keeps 'Column', second becomes 'Column/1'",
            Calls =
            [
                new AddRowsCall().Scalar("Column", 0.1),
                new AddRowsCall().Scalar("Column", "0.1f"),
            ],
            Columns =
            [
                new ExpectedColumn("Column", [0.1, null]),
                new ExpectedColumn("Column/1", [null, "0.1f"]),
            ],
            Mappings = new() { ["Column/1"] = "Column" },
        };

        // --- Array type collisions across separate calls ---
        foreach (CollisionCase arrayCase in ArrayTypeCollisionCases())
        {
            yield return arrayCase;
        }

        // --- Same-type duplicates written within a single call ---
        yield return new CollisionCase
        {
            Description = "Two same-typed arrays named 'a' in one call -> 'a' and 'a/1', data kept separate",
            Calls =
            [
                new AddRowsCall()
                    .Array("a", Enumerable.Range(0, 3).ToArray())
                    .Array("a", Enumerable.Range(100, 3).ToArray()),
            ],
            Columns =
            [
                new ExpectedColumn("a", [0, 1, 2]),
                new ExpectedColumn("a/1", [100, 101, 102]),
            ],
            Mappings = new() { ["a/1"] = "a" },
        };
        yield return new CollisionCase
        {
            Description = "Three same-typed arrays named 'a' in one call -> sequential indices 'a', 'a/1', 'a/2'",
            Calls =
            [
                new AddRowsCall()
                    .Array("a", Enumerable.Range(0, 3).ToArray())
                    .Array("a", Enumerable.Range(100, 3).ToArray())
                    .Array("a", Enumerable.Range(200, 3).ToArray()),
            ],
            Columns =
            [
                new ExpectedColumn("a", [0, 1, 2]),
                new ExpectedColumn("a/1", [100, 101, 102]),
                new ExpectedColumn("a/2", [200, 201, 202]),
            ],
            Mappings = new() { ["a/1"] = "a", ["a/2"] = "a" },
        };
        yield return new CollisionCase
        {
            Description = "Two same-typed scalars named 'p' in one call -> 'p' and 'p/1'",
            Calls =
            [
                new AddRowsCall().Scalar("p", 1).Scalar("p", 2),
            ],
            Columns =
            [
                new ExpectedColumn("p", [1]),
                new ExpectedColumn("p/1", [2]),
            ],
            Mappings = new() { ["p/1"] = "p" },
        };

        // --- A scalar and an array sharing a name in one call must not claim the same column ---
        yield return new CollisionCase
        {
            Description = "Scalar and array share name 'shared' in one call -> distinct columns, " +
                          "scalar broadcast across the array's rows",
            Calls =
            [
                new AddRowsCall()
                    .Scalar("shared", 7)
                    .Array("shared", Enumerable.Range(0, 3).ToArray()),
            ],
            // Column order follows creation order: scalars are claimed before arrays in AddRows.
            Columns =
            [
                new ExpectedColumn("shared", [7, 7, 7]),
                new ExpectedColumn("shared/1", [0, 1, 2]),
            ],
            Mappings = new() { ["shared/1"] = "shared" },
        };

        // --- Duplicate name accumulating across two separate calls reuses existing columns ---
        yield return new CollisionCase
        {
            Description = "Same-typed duplicates split in call 1 ('a','a/1'); call 2 reuses both columns",
            Calls =
            [
                new AddRowsCall()
                    .Array("a", new[] { 0, 1 })
                    .Array("a", new[] { 10, 11 }),
                new AddRowsCall()
                    .Array("a", new[] { 2, 3 })
                    .Array("a", new[] { 12, 13 }),
            ],
            Columns =
            [
                new ExpectedColumn("a", [0, 1, 2, 3]),
                new ExpectedColumn("a/1", [10, 11, 12, 13]),
            ],
            Mappings = new() { ["a/1"] = "a" },
        };
    }

    private static string ElementTypeName(Array array)
    {
        Type element = array.GetType().GetElementType()!;
        return (Nullable.GetUnderlyingType(element) ?? element).Name;
    }

    // How a column reads back: string-typed columns stringify their values, everything else is verbatim.
    private static object?[] Expected(Array array)
    {
        bool toString = Fragment.ShouldConvertToString(array.GetType().GetElementType()!);
        return array.Cast<object?>().Select(v => toString ? v?.ToString() : v).ToArray();
    }

    // A column that holds no data for `leading` rows, then the values, then nulls to reach `total`.
    private static object?[] Padded(int leading, object?[] values, int total)
    {
        object?[] column = new object?[total];
        Array.Copy(values, 0, column, leading, values.Length);
        return column;
    }

    private static IEnumerable<CollisionCase> ArrayTypeCollisionCases()
    {
        (Array first, Array second)[] pairs =
        [
            (new float?[] { 0.1f, 0.2f, 0.3f, null }, new[] { "1", "2" }),
            (new[] { 0, 1, 2 }, new[] { 0.1f, 0.2f, 0.3f }),
            (new[] { 0, 1, 2 }, new[] { 0.1, 0.2, 0.3 }),
            (new[] { 0.1, 1.2, 2.3 }, new[] { 0.1f, 0.2f, 0.3f }),
            (new[] { 0.1, 1.2, 2.3 }, new object[] { 0.1, 1.2, 2.3 }),
            (new[] { "String", "Test", "Hello" }, new[] { 0.1f, 0.2f, 0.3f }),
        ];

        foreach ((Array first, Array second) in pairs)
        {
            int total = first.Length + second.Length;
            yield return new CollisionCase
            {
                Description = $"Array type collision: {ElementTypeName(first)}[] then {ElementTypeName(second)}[] " +
                              "under 'Custom' -> 'Custom' and 'Custom/1'",
                Calls =
                [
                    new AddRowsCall().Array("Custom", first),
                    new AddRowsCall().Array("Custom", second),
                ],
                Columns =
                [
                    new ExpectedColumn("Custom", Padded(0, Expected(first), total)),
                    new ExpectedColumn("Custom/1", Padded(first.Length, Expected(second), total)),
                ],
                Mappings = new() { ["Custom/1"] = "Custom" },
            };
        }
    }

    [TestCaseSource(nameof(CollisionCases))]
    public async Task DuplicationAndCollisionTest(CollisionCase testCase)
    {
        string path = Path.GetTempFileName();

        var frag = new Fragment(path, new Options());
        foreach (AddRowsCall call in testCase.Calls)
        {
            Assert.That(frag.AddRows(call.ScalarValues(), call.ArrayValues()), Is.True,
                $"[{testCase.Description}] AddRows returned false (did not fit in the fragment).");
        }
        frag.Dispose();

        Assert.That(System.IO.File.Exists(path), Is.True,
            $"[{testCase.Description}] the parquet file was not created.");

        var reader = await Reader.CreateAsync(path);

        string[] expectedFields = DefaultFields.Concat(testCase.Columns.Select(c => c.Name)).ToArray();
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(expectedFields),
            $"[{testCase.Description}] the produced column names did not match. " +
            "Duplicate/incompatible names should keep the first column plain and index the rest (name/1, name/2, ...).");

        long expectedRowCount = testCase.Columns.Max(c => c.Values.Length);
        Assert.That(reader.Count, Is.EqualTo(expectedRowCount),
            $"[{testCase.Description}] the row count did not match.");

        foreach (ExpectedColumn column in testCase.Columns)
        {
            for (int row = 0; row < column.Values.Length; row++)
            {
                Assert.That(reader.ReadCell(row, column.Name), Is.EqualTo(column.Values[row]),
                    $"[{testCase.Description}] column '{column.Name}' row {row} had the wrong value. " +
                    "This usually means duplicate values were mixed into the same physical column.");
            }
        }

        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(JsonSerializer.Serialize(testCase.Mappings)),
            $"[{testCase.Description}] the name mappings metadata did not match. " +
            "Every indexed column must map back to its shared display name; the plain first column must not.");
    }

    // Duplicate arrays whose length spans several row groups: the split columns must stay aligned and
    // ordered even when WriteCache flushes partway through a single AddRows call.
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(7)]
    public async Task DuplicateArraysAcrossRowGroupBoundaryTest(int rowGroupSize)
    {
        string path = Path.GetTempFileName();

        int[] first = Enumerable.Range(0, 10).ToArray();
        int[] second = Enumerable.Range(100, 10).ToArray();

        var frag = new Fragment(path, new Options { RowGroupSize = rowGroupSize });
        frag.AddRows(new Dictionary<string, List<IConvertible>>(), new Dictionary<string, List<Array>>
        {
            { "a", [first, second] },
        });
        frag.Dispose();

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", "a", "a/1"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields),
            $"rowGroupSize={rowGroupSize}: duplicate arrays should split into 'a' and 'a/1'.");
        Assert.That(reader.Count, Is.EqualTo(10), $"rowGroupSize={rowGroupSize}: unexpected row count.");
        for (int i = 0; i < 10; i++)
        {
            Assert.That(reader.ReadCell(i, "a"), Is.EqualTo(first[i]),
                $"rowGroupSize={rowGroupSize}: 'a' row {i} misaligned across row-group boundary.");
            Assert.That(reader.ReadCell(i, "a/1"), Is.EqualTo(second[i]),
                $"rowGroupSize={rowGroupSize}: 'a/1' row {i} misaligned across row-group boundary.");
        }

        Assert.That(reader.CustomMetadata["Mappings"], Is.EqualTo(JsonSerializer.Serialize(new Dictionary<string, string> { ["a/1"] = "a" })),
            $"rowGroupSize={rowGroupSize}: mapping metadata should map 'a/1' back to 'a'.");
    }

    // A tricky ambiguous case: three real "a" columns AND a real "a/1" column in the same call.
    // The three 'a's want names a, a/1, a/2; the real 'a/1' also wants a/1. This must stay lossless
    // and, crucially, resolve to the correct *display* names on read-back: three columns should
    // display as "a" and one should display as "a/1" - exactly what was published. The physical
    // (unique) names are an internal detail and may be order-dependent.
    [Test]
    public async Task ThreeRealColumnsCollidingWithRealSplitNameTest()
    {
        string path = Path.GetTempFileName();

        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, List<IConvertible>>(), new Dictionary<string, List<Array>>
        {
            { "a", [new[] { 0 }, new[] { 1 }, new[] { 2 }] },
            { "a/1", [new[] { 99 }] },
        });
        frag.Dispose();

        var reader = await Reader.CreateAsync(path);
        var physicalNames = reader.Schema.Fields.Select(f => f.Name)
            .Where(n => n == "a" || n.StartsWith("a/"))
            .ToList();

        // Every published value survives in its own column - no loss, no mixing.
        Assert.That(physicalNames.Count, Is.EqualTo(4), "all four published columns must exist.");
        Assert.That(physicalNames.Distinct().Count(), Is.EqualTo(4), "physical column names must be unique.");
        var storedValues = physicalNames.Select(n => reader.ReadCell(0, n)).ToList();
        Assert.That(storedValues, Is.EquivalentTo(new object?[] { 0, 1, 2, 99 }),
            "all four values must survive in separate columns without mixing.");

        // Resolve each physical column to the display name it maps back to (identity when unmapped),
        // and assert the display-name multiset matches what was published: three "a" and one "a/1".
        var mappings = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(reader.CustomMetadata["Mappings"])!;
        var displayNames = physicalNames
            .Select(n => mappings.TryGetValue(n, out string? display) ? display : n)
            .ToList();
        Assert.That(displayNames.Count(d => d == "a"), Is.EqualTo(3),
            "three published 'a' columns must resolve back to display name 'a'.");
        Assert.That(displayNames.Count(d => d == "a/1"), Is.EqualTo(1),
            "the one published 'a/1' column must resolve back to display name 'a/1'.");
    }

    // Duplicates and type collisions under the same name must all use the one "/N" suffix scheme, and
    // every renamed physical column must be recorded in the mapping metadata (no silent drops).
    [Test]
    public async Task ConsistentSuffixSchemeAndCompleteMappingsTest()
    {
        string path = Path.GetTempFileName();

        // "a" gets: an int (plain), a duplicate int (needs a new name), and a string (type collision,
        // also needs a new name). All three extra columns should follow the same "/N" scheme.
        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, List<IConvertible>>(), new Dictionary<string, List<Array>>
        {
            { "a", [new[] { 0 }, new[] { 1 }, new[] { "text" }] },
        });
        frag.Dispose();

        var reader = await Reader.CreateAsync(path);
        string[] fields = ["ResultName", "Guid", "Parent", "StepId", "a", "a/1", "a/2"];
        Assert.That(reader.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields),
            "all duplicates/collisions under 'a' should use the sequential '/N' suffix scheme.");
        Assert.That(reader.ReadCell(0, "a"), Is.EqualTo(0));
        Assert.That(reader.ReadCell(0, "a/1"), Is.EqualTo(1));
        Assert.That(reader.ReadCell(0, "a/2"), Is.EqualTo("text"));

        // Every renamed column (UniqueName != Name) must appear in the mapping table.
        var mappings = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(reader.CustomMetadata["Mappings"])!;
        Assert.That(mappings, Is.EquivalentTo(new Dictionary<string, string>
        {
            ["a/1"] = "a",
            ["a/2"] = "a",
        }), "both generated columns must map back to the shared display name 'a'.");
    }

    // A generated split name can clash with a real, differently-named column. FindUniqueName must
    // then fall back to a numeric suffix so that no two physical columns ever share a name and every
    // value still lands in its own column. (Which physical name ends up mapped depends on claim order;
    // the invariants below hold regardless.)
    [Test]
    public async Task GeneratedSplitNameClashesWithRealColumnTest()
    {
        string path = Path.GetTempFileName();

        // "a" appears twice (the second needs a generated name), and a real column also asks for "a/1".
        var frag = new Fragment(path, new Options());
        frag.AddRows(new Dictionary<string, List<IConvertible>>(), new Dictionary<string, List<Array>>
        {
            { "a", [new[] { 0 }, new[] { 1 }] },
            { "a/1", [new[] { 2 }] },
        });
        frag.Dispose();

        var reader = await Reader.CreateAsync(path);
        var names = reader.Schema.Fields.Select(f => f.Name).ToList();

        // Core invariant: physical names are unique and every one of the three values got its own column.
        Assert.That(names.Distinct().Count(), Is.EqualTo(names.Count), "no two physical columns may share a name.");
        Assert.That(names, Does.Contain("a"), "the first 'a' keeps the plain name.");
        Assert.That(names.Count(n => n.StartsWith("a/")), Is.EqualTo(2),
            "the duplicate 'a' and the real 'a/1' must occupy two distinct 'a/...'-named columns.");

        // Every published value is present exactly once across the three columns (no data mixing/loss).
        var storedValues = names.Where(n => n == "a" || n.StartsWith("a/"))
            .Select(n => reader.ReadCell(0, n))
            .ToList();
        Assert.That(storedValues, Is.EquivalentTo(new object?[] { 0, 1, 2 }),
            "all three values must survive in separate columns without mixing.");

        // The two duplicate columns of display-name 'a' map back to 'a'; the real 'a/1' column maps to 'a/1'
        // (an identity mapping, so it is simply absent from the mapping table).
        var mappings = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(reader.CustomMetadata["Mappings"])!;
        Assert.That(mappings.Values.Count(v => v == "a"), Is.EqualTo(1),
            "exactly one generated column should map back to the shared display name 'a'.");
    }

    // A fragment only stops accepting new columns once a row group has been flushed (CanEdit == false).
    // With RowGroupSize == 1 the first write flushes immediately, so any later call that needs a new
    // column (a brand new name, or a type collision) must return false to trigger a new fragment.
    [Test]
    public void AddRowsReturnsFalseWhenNewColumnNeededAfterFlush()
    {
        string path = Path.GetTempFileName();

        var frag = new Fragment(path, new Options { RowGroupSize = 1 });
        bool first = frag.AddRows(new Dictionary<string, List<IConvertible>>
        {
            { "a", [1] },
        }, new Dictionary<string, List<Array>>());
        Assert.That(first, Is.True, "the first write establishes the schema and should succeed.");

        // Same schema -> reuses the existing column, still fits even after the flush.
        bool sameSchema = frag.AddRows(new Dictionary<string, List<IConvertible>>
        {
            { "a", [2] },
        }, new Dictionary<string, List<Array>>());
        Assert.That(sameSchema, Is.True, "re-using the existing column should still fit after a flush.");

        // A brand new column cannot be created once the writer exists -> must signal false.
        bool newColumn = frag.AddRows(new Dictionary<string, List<IConvertible>>
        {
            { "b", [3] },
        }, new Dictionary<string, List<Array>>());
        Assert.That(newColumn, Is.False, "a new column after flushing cannot fit and must return false.");

        // A type collision on an existing name also needs a new column -> also false.
        bool typeCollision = frag.AddRows(new Dictionary<string, List<IConvertible>>
        {
            { "a", ["text"] },
        }, new Dictionary<string, List<Array>>());
        Assert.That(typeCollision, Is.False, "a type collision needing a new column after flushing must return false.");

        frag.Dispose();
    }

    #endregion
}