using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Tests;

public class Reader : IDisposable
{
    private readonly ParquetReader _reader;
    private int _rowGroup = 0;
    private long _rowOffset;

    public Reader(ParquetReader reader)
    {
        _reader = reader;
    }

    public ParquetSchema Schema => _reader.Schema;
    public int RowGroupCount => _reader.RowGroupCount;
    public long Count => _reader.RowGroups.Sum(r => r.RowCount);
    public Dictionary<string, string> CustomMetadata => _reader.CustomMetadata;
    
    public static async Task<Reader> CreateAsync(string path)
    {
        ParquetReader reader = await ParquetReader.CreateAsync(path);
        
        return new Reader(reader);
    }

    public IEnumerable<object?> ReadRow(long readRow)
    {
        return ReadRowRec(readRow).Select(t => t.value);
    }

    public object? ReadCell(long row, string name)
    {
        return ReadRowRec(row).First(c => c.name == name).value;
    }
    
    private IEnumerable<(string name, object? value)> ReadRowRec(long readRow)
    {
        long row = readRow - _rowOffset;
        using var rowGroup = _reader.OpenRowGroupReader(_rowGroup);
        if (row >= rowGroup.RowCount)
        {
            _rowGroup += 1;
            _rowOffset += rowGroup.RowCount;
            foreach (var r in ReadRowRec(readRow))
            {
                yield return r;
            }

            yield break;
        }

        foreach (var field in Schema.DataFields)
        {
            var column = rowGroup.ReadColumnAsync(field).Result;
            yield return (field.Name, column.Data.Length <= row ? null : column.Data.GetValue(row));
        }
    }

    public void Dispose()
    {
        _reader.Dispose();
    }
}