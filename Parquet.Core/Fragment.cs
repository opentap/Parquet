using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using OpenTap.Plugins.Parquet.Core.Extensions;
using Parquet.Data;
using Parquet;

namespace OpenTap.Plugins.Parquet;

internal sealed class Fragment : IDisposable
{
    private class ColumnData
    {
        public Array Data { get; }
        public int Count { get; set; } = 0;
        public DataField Field { get; }
        public Type ParquetType { get; }

        public ColumnData(string name, Type type, int size, int existingCacheSize)
        {
            Data = Array.CreateInstance(type.AsNullable(), size);
            Field = new DataField(name, type, true);
            Count = existingCacheSize;
            ParquetType = type;
        }
    }

    private readonly Options _options;
    private readonly Stream _stream;
    private ParquetWriter? _writer;
    private ParquetSchema? _schema;
    private int _cacheSize;
    private readonly List<DataField> _fields;
    private readonly Dictionary<string, ColumnData> _cache;

    private int RowGroupSize => _options.RowGroupSize;

    public Fragment(string path,  Options options)
    {
        Path = path;
        _options = options;
        string? dirPath = System.IO.Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dirPath) && !Directory.Exists(dirPath))
        {
            Directory.CreateDirectory(dirPath);
        }
        _stream = File.Open(Path, FileMode.Create, FileAccess.Write);
        _fields = new();
        _cache = new();
        AddColumn("ResultName", typeof(string));
        AddColumn("Guid", typeof(string));
        AddColumn("Parent", typeof(string));
        AddColumn("StepId", typeof(string));
    }
    
    public string Path { get; }

    public bool AddRows(Dictionary<string, IConvertible> values,
        Dictionary<string, Array> arrayValues)
    {
        if (!FitsInCache(values.Select(kvp => (kvp.Key, kvp.Value.GetType()))) ||
            !FitsInCache(arrayValues.Select(kvp => (kvp.Key, kvp.Value.GetType().GetElementType()))))
        {
            return false;
        }
        
        int resultCount = Math.Max(1, arrayValues.Any() ? arrayValues.Max(d => d.Value.Length) : 1);
        int startIndex = 0;
        while (startIndex < resultCount)
        {
            int count = Math.Min(RowGroupSize - _cacheSize, resultCount - startIndex);

            foreach (KeyValuePair<string,ColumnData> kvp in _cache)
            {
                string name = kvp.Key;
                ColumnData column = kvp.Value;

                if (arrayValues.TryGetValue(name, out Array? valueArr))
                {
                    AddToColumn(column, valueArr, startIndex, count);
                }
                else if (values.TryGetValue(name, out IConvertible value))
                {
                    AddToColumn(column, value, count);
                }
                else
                {
                    AddToColumn(column, null, count);
                }
            }
            
            _cacheSize += count;
            startIndex += count;
            if (_cacheSize >= RowGroupSize)
            {
                WriteCache();
            }
        }
        return true;
    }

    private bool FitsInCache(IEnumerable<(string, Type)> fields)
    {
        foreach ((string name, Type type) in fields)
        {
            if (_cache.ContainsKey(name))
            {
                continue;
            }
            
            if (_writer is null)
            {
                AddColumn(name, type);
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    private void AddColumn(string name, Type type)
    {
        // Warning: This function should only be called if the writer is not null, otherwise we will get an error, next time writing the cache.
        ColumnData data = new ColumnData(name, GetParquetType(type), RowGroupSize, _cacheSize);
        _cache.Add(name, data);
        _fields.Add(data.Field);
    }

    private void AddToColumn(ColumnData column, IConvertible? value, int count){
        if (column.ParquetType == typeof(string) && value?.GetType() != typeof(string)){
            value = value?.ToString(CultureInfo.InvariantCulture);
        }

        for (int i = 0; i < count; i++)
        {
            column.Data.SetValue(value, _cacheSize + i);
        }
        column.Count += count;
    }

    private void AddToColumn(ColumnData column, Array values, int startIndex, int count){
        values = values.Cast<object?>()
            .Skip(startIndex)
            .Concat(Enumerable.Repeat<object?>(null, Math.Max(count + startIndex - values.Length, 0)))
            .Take(count)
            .ToArray();
        Array.Copy(values, 0, column.Data, column.Count, count);
        column.Count += count;
    }

    private void WriteCache()
    {
        if (_writer is null || _schema is null)
        {
            _schema = new ParquetSchema(_fields);
            _writer = ParquetWriter.CreateAsync(_schema, _stream, _options.ParquetOptions).Result;
            _writer.CompressionMethod = _options.CompressionMethod;
            _writer.CompressionLevel = _options.CompressionLevel;
        }
        using ParquetRowGroupWriter rowGroupWriter = _writer.CreateRowGroup();
        for (var i = 0; i < _schema.DataFields.Length; i++)
        {
            ColumnData data = _cache[_schema.DataFields[i].Name];
            data.Count = 0;
            Array arr = data.Data;
            if (_cacheSize != RowGroupSize){
                arr = Array.CreateInstance(_schema.DataFields[i].ClrNullableIfHasNullsType, _cacheSize);
                Array.Copy(data.Data, arr, _cacheSize);
            }
            DataColumn column = new DataColumn(_schema.DataFields[i], arr);
            rowGroupWriter.WriteColumnAsync(column).Wait();
        }
        _cacheSize = 0;
    }

    // Merge another parquet fragment into this fragment.
    public void MergeWith(Fragment other)
    {
        WriteCache();
        Dictionary<string, DataColumn> emptyColumns = new();

        using ParquetReader reader = ParquetReader.CreateAsync(other.Path).Result;
        for (int i = 0; i < reader.RowGroupCount; i++)
        {
            Dictionary<string, DataColumn> columns =
                reader.ReadEntireRowGroupAsync(i).Result.ToDictionary(c => c.Field.Name, c => c);
            using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(i);

            using ParquetRowGroupWriter writer = _writer!.CreateRowGroup();
            foreach (DataField field in _fields)
            {
                DataColumn column = GetColumn(columns, field, groupReader);
                writer.WriteColumnAsync(column).Wait();
            }
        }

        // TODO: We should benchmark if all of this logic actually makes merging faster.
        DataColumn GetColumn(Dictionary<string, DataColumn> columns, DataField field, ParquetRowGroupReader groupReader)
        {
            // Try to get column from other reader.
            if (columns.TryGetValue(field.Name, out DataColumn? column))
            {
                return column;
            }

            // Create new DataColumn of correct size.
            if (groupReader.RowCount != RowGroupSize)
            {
                return new DataColumn(field,
                    Array.CreateInstance(field.ClrNullableIfHasNullsType, groupReader.RowCount));
            }

            // If size is correct we can use a cached empty column.
            if (emptyColumns.TryGetValue(field.Name, out column))
            {
                return column;
            }

            // No cached empty column found, so create new column.
            column = new DataColumn(field,
                Array.CreateInstance(field.ClrNullableIfHasNullsType, RowGroupSize));
            emptyColumns.Add(field.Name, column);

            return column;
        }
    }

    public void Dispose()
    {
        WriteCache();
        _writer?.Dispose();
        _stream.Flush();
        _stream.Dispose();
    }
    
    private static Type GetParquetType(Type type)
    {
        if (type.IsEnum)
        {
            return typeof(string);
        }

        return type;
    }
}
