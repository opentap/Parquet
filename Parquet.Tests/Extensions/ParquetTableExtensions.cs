using NUnit.Framework;
using Parquet.Rows;

namespace Parquet.Tests.Extensions;

public static class ParquetTableExtensions
{
    public static void AssertSchema(this Table table, params string[] fields)
    {
        Assert.That(table.Schema.Fields.Select(f => f.Name), Is.EquivalentTo(fields));
    }

    public static void AssertRows(this Table table, int rows, Action<Row> row)
    {
        Assert.That(table.Count, Is.EqualTo(rows));
        for (int i = 0; i < rows; i++)
        {
            row(table[i]);
        }
    }
    
    public static void AssertRows(this Table table, int rows, Action<Row, int> row)
    {
        Assert.That(table.Count, Is.EqualTo(rows));
        for (int i = 0; i < rows; i++)
        {
            row(table[i], i);
        }
    }
    
    public static void AssertValues(this Row row, params object?[] values)
    {
        Assert.That(row, Is.EquivalentTo(values));
    }
    
    public static void AssertContains(this Row row, params object?[] values)
    {
        Assert.That(row, Is.SupersetOf(values));
    }
}