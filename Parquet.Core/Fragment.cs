using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using OpenTap.Plugins.Parquet.Core.Extensions;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ColumnKey = (string name, System.Type type);

namespace OpenTap.Plugins.Parquet.Core;

internal sealed class Fragment : IDisposable
{
    private class ColumnData
    {
        private DataField? _field;
        
        public Array Data { get; }
        public int Count { get; set; } = 0;

        public DataField Field => _field ??= new DataField(UniqueName, ParquetType, true);
        public Type ParquetType { get; }
        public Type Type { get; }
        public string Name { get; }
        public string UniqueName { get; private set; }

        public ColumnData(string uniqueName, string name, Type type, int size, int existingCacheSize)
        {
            Data = Array.CreateInstance(type.AsNullable(), size);
            Count = existingCacheSize;
            _field = null;
            ParquetType = GetParquetType(type);
            Type = type;
            UniqueName = uniqueName;
            Name = name;
        }

        public bool TrySetName(string name)
        {
            if (_field is not null)
            {
                return false;
            }

            UniqueName = name;
            return true;
        }
    }

    private readonly Options _options;
    private readonly Stream _stream;
    private ParquetWriter? _writer;
    private ParquetSchema? _schema;
    private int _cacheSize;
    private readonly List<ColumnData> _columns;
    private readonly HashSet<string> _uniqueColumnNames = new();
    private readonly Dictionary<string, Dictionary<Type, ColumnData>> _cache;
    private readonly Dictionary<string, string> _metadata;

    private int RowGroupSize => _options.RowGroupSize;

    public Fragment(string path, Options options)
    {
        Path = path;
        _options = options;
        string? dirPath = System.IO.Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dirPath) && !Directory.Exists(dirPath))
        {
            Directory.CreateDirectory(dirPath);
        }
        _stream = File.Open(Path, FileMode.Create, FileAccess.Write);
        _columns = new();
        _cache = new();
        _metadata = new();
        AddColumn("ResultName", typeof(string));
        AddColumn("Guid", typeof(string));
        AddColumn("Parent", typeof(string));
        AddColumn("StepId", typeof(string));
    }

    public Fragment(Fragment fragment, string path)
    {
        Path = path;
        _options = fragment._options;
        string? dirPath = System.IO.Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dirPath) && !Directory.Exists(dirPath))
        {
            Directory.CreateDirectory(dirPath);
        }
        _stream = File.Open(Path, FileMode.Create, FileAccess.Write);
        _columns = fragment._columns;
        _cache = fragment._cache;
        _metadata = fragment._metadata;
    }

    public bool CanEdit => _writer is null;
    
    public void SetMetadata(string key, string value)
    {
        _metadata[key] = value;
    }

    private void UpdateMappings()
    {
        Dictionary<string, string> mappings = _cache
            .Values
            .Where(d => d.Count > 1)
            .SelectMany(d => d.Values)
            .ToDictionary(cd => cd.UniqueName, cd => cd.Name);
        SetMetadata("Mappings", JsonSerializer.Serialize(mappings));
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

            foreach (ColumnData column in _columns)
            {
                if (arrayValues.TryGetValue(column.Name, out Array? valueArr) && column.Type.IsAssignableFrom(valueArr.GetType().GetElementType()))
                {
                    AddToColumn(column, valueArr, startIndex, count);
                }
                else if (values.TryGetValue(column.Name, out IConvertible value))
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

    private bool FitsInCache(IEnumerable<ColumnKey> fields)
    {
        // TODO: Test this function, rename- and add-column.
        foreach ((string name, Type type) in fields)
        {
            if (!FitsInCache(name, type))
            {
                return false;
            }
        }

        return true;
    }

    private bool FitsInCache(string name, Type type)
    {
        if (!_cache.TryGetValue(name, out Dictionary<Type, ColumnData>? typeCache))
        {
            return AddColumn(name, type) is not null;
        }

        if (!typeCache.TryGetValue(type, out _))
        {
            if (typeCache.Count == 1)
            {
                ColumnData data = typeCache.Values.First();
                data.TrySetName(FindUniqueName(data.Name + "/" + data.Type.Name));
            }
            bool val = AddColumn(name, type, FindUniqueName(name + "/" + type.Name)) is not null;
            UpdateMappings();
            return val;
        }

        return true;
    }

    private ColumnData? AddColumn(string name, Type type, string? uniqueName = null)
    {
        if (!CanEdit)
        {
            return null;
        }

        if (uniqueName == null)
        {
            uniqueName = FindUniqueName(name);
        }

        if (!_cache.TryGetValue(name, out Dictionary<Type, ColumnData>? typeCache))
        {
            typeCache = new();
            _cache[name] = typeCache;
        }
        ColumnData data = new ColumnData(uniqueName, name, GetParquetType(type), RowGroupSize, _cacheSize);
        typeCache.Add(type, data);
        _columns.Add(data);
        return data;
    }

    private string FindUniqueName(string name)
    {
        string str = name;
        int attempt = 0;
        while (_uniqueColumnNames.Contains(str))
        {
            str = name + attempt;
            attempt += 1;
    
            if (attempt == int.MaxValue)
            {
                throw new Exception("Too many columns in parquet file.");
            }
        }
    
        _uniqueColumnNames.Add(name);
    
        return str;
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
        EnsureWriterExists();
        using ParquetRowGroupWriter rowGroupWriter = _writer!.CreateRowGroup();
        for (var i = 0; i < _schema!.DataFields.Length; i++)
        {
            ColumnData data = _columns[i];
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

    private void EnsureWriterExists()
    {
        if (CanEdit)
        {
            _schema = new ParquetSchema(_columns.Select(cd => cd.Field));
            _writer = ParquetWriter.CreateAsync(_schema, _stream, _options.ParquetOptions).Result;
            _writer.CompressionMethod = _options.CompressionMethod;
            _writer.CompressionLevel = _options.CompressionLevel;
            _writer.CustomMetadata = _metadata;
        }
    }

    // Merge another parquet fragment into this fragment.
    public void MergeWith(Fragment other)
    {
        EnsureWriterExists();
        Dictionary<string, DataColumn> emptyColumns = new();

        using ParquetReader reader = ParquetReader.CreateAsync(other.Path).Result;
        for (int i = 0; i < reader.RowGroupCount; i++)
        {
            Dictionary<string, DataColumn> columns =
                reader.ReadEntireRowGroupAsync(i).Result.ToDictionary(c => c.Field.Name, c => c);
            using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(i);

            using ParquetRowGroupWriter writer = _writer!.CreateRowGroup();
            foreach (ColumnData cd in _columns)
            {
                DataField field = cd.Field;
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
