using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.Extensions;
using Parquet;
using System.IO.Compression;

namespace OpenTap.Plugins.Parquet;

internal sealed class ParquetFragment : IDisposable
{
    private record ColumnData
    {
        public Array Data { get; }
        public int Count { get; set; } = 0;
        public DataField Field { get; }

        public ColumnData(string name, Type type, int size, int existingCacheSize)
        {
            Data = Array.CreateInstance(type.AsNullable(), size);
            Field = new DataField(name, type, true);
            Count = existingCacheSize;
        }
    }
    
    private readonly string _path;
    private readonly ParquetResult.Options _options;
    private readonly int _nestedLevel;
    private readonly int _rowgroupSize;
    private readonly Stream _stream;
    private ParquetWriter? _writer;
    private ParquetSchema? _schema;
    private int _cacheSize;
    private readonly List<DataField> _fields;
    private readonly Dictionary<string, ColumnData> _cache;

    public ParquetFragment(string path,  ParquetResult.Options options)
    {
        _path = path;
        _options = options;
        _nestedLevel = 0;
        _rowgroupSize = _options.RowGroupSize;
        string? dirPath = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dirPath) && !Directory.Exists(dirPath))
        {
            Directory.CreateDirectory(dirPath);
        }
        _stream = File.Open(_path + _nestedLevel + ".tmp", FileMode.Create, FileAccess.Write);
        _fields = new();
        _cache = new();
    }

    public ParquetFragment(ParquetFragment fragment)
    {
        _path = fragment._path;
        _nestedLevel = fragment._nestedLevel + 1;
        _rowgroupSize = fragment._rowgroupSize;
        _options = fragment._options;
        _stream = File.Open(_path + _nestedLevel + ".tmp", FileMode.Create, FileAccess.Write);
        _cacheSize = fragment._cacheSize;
        _fields = fragment._fields;
        _cache = fragment._cache;
    }

    public bool AddRows(string? resultName, Guid? guid, Guid? parentId, Guid? stepId,
        Dictionary<string, IConvertible>? plan,
        Dictionary<string, IConvertible>? step,
        Dictionary<string, Array>? results)
    {
        int resultCount = results?.Values.Max(d => d.Length) ?? 1;
        int startIndex = 0;
        while (startIndex < resultCount)
        {
            bool fitsInCache = true;
            int count = Math.Min(_rowgroupSize - _cacheSize, resultCount - startIndex);

            fitsInCache &= AddToColumn("ResultName", typeof(string), resultName, count);
            fitsInCache &= AddToColumn("Guid", typeof(Guid), guid, count);
            fitsInCache &= AddToColumn("Parent", typeof(Guid), parentId, count);
            fitsInCache &= AddToColumn("StepId", typeof(Guid), stepId, count);
            
            if (plan is not null)
                foreach (var item in plan)
                {
                    fitsInCache &= AddToColumn("Plan/" + item.Key, item.Value.GetType(), item.Value, count);
                }
            if (step is not null)
                foreach (var item in step)
                {
                    fitsInCache &= AddToColumn("Step/" + item.Key, item.Value.GetType(), item.Value, count);
                }
            if (results is not null)
                foreach(var item in results)
                {
                    fitsInCache &= AddToColumn("Results/" + item.Key, item.Value, startIndex, item.Value.Length);
                }

            foreach (var item in _cache)
            {
                if (item.Value.Count < _cacheSize + count)
                {
                    fitsInCache &= AddToColumn(item.Key, item.Value.Field.ClrType, null, _cacheSize - item.Value.Count);
                }
            }
            
            _cacheSize += count;
            startIndex += count;
            if (!fitsInCache && _writer is not null)
            {
                return false;
            }
            
            if (_cacheSize >= _rowgroupSize)
            {
                WriteCache();
            }
        }
        return true;
    }

    private bool AddToColumn(string name, Array values, int startIndex, int count){
        Type type = values.GetType().GetElementType()!;
        bool fitsInCache = GetOrCreateColumn(name, type, out ColumnData data, out Type columnType);

        Array.Copy(values.Cast<object?>().Skip(startIndex).Take(count).ToArray(), 0, data.Data, data.Count, count);
        data.Count += count;

        return fitsInCache;
    }

    private bool AddToColumn(string name, Type type, object? value, int count){
        bool fitsInCache = GetOrCreateColumn(name, type, out ColumnData data, out Type columnType);

        if (columnType == typeof(string) && type != typeof(string)){
            value = value?.ToString();
        }

        for (int i = 0; i < count; i++)
        {
            data.Data.SetValue(value, _cacheSize + i);
        }
        data.Count += count;

        return fitsInCache;
    }

    private bool GetOrCreateColumn(string name, Type type, out ColumnData data, out Type parquetType)
    {
        parquetType = GetParquetType(type);
        if (!_cache.TryGetValue(name, out data))
        {
            data = new ColumnData(name, parquetType, _rowgroupSize, _cacheSize);
            _cache.Add(name, data);
            _fields.Add(data.Field);
            return false;
        }

        return true;
    }

    public void WriteCache()
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
            if (_cacheSize != _rowgroupSize){
                arr = Array.CreateInstance(_schema.DataFields[i].ClrNullableIfHasNullsType, _cacheSize);
                Array.Copy(data.Data, arr, _cacheSize);
            }
            DataColumn column = new DataColumn(_schema.DataFields[i], arr);
            rowGroupWriter.WriteColumnAsync(column).Wait();
        }
        _cacheSize = 0;
    }

    public void Dispose(IEnumerable<ParquetFragment>? fragments)
    {
        if (fragments == null)
        {
            fragments = new List<ParquetFragment>();
        }
        
        if (_writer is null || _schema is null)
        {
            _schema = new ParquetSchema(_fields);
            _writer = ParquetWriter.CreateAsync(_schema, _stream, _options.ParquetOptions).Result;
            _writer.CompressionMethod = _options.CompressionMethod;
            _writer.CompressionLevel = _options.CompressionLevel;
        }
        Dictionary<string, DataColumn> emptyColumns = new();
        foreach (ParquetFragment fragment in fragments)
        {
            if (_path != fragment._path || _fields != fragment._fields || _cache != fragment._cache)
            {
                throw new InvalidOperationException(
                    "Cannot merge fragments since they dont originate from the same base fragment.");
            }

            ParquetReader reader = ParquetReader.CreateAsync(fragment._path + fragment._nestedLevel + ".tmp").Result;
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                Dictionary<string, DataColumn> columns =
                    reader.ReadEntireRowGroupAsync(i).Result.ToDictionary(c => c.Field.Name, c => c);
                using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(i);

                using ParquetRowGroupWriter writer = _writer!.CreateRowGroup();
                foreach (DataField field in _fields)
                {
                    if (!columns.TryGetValue(field.Name, out DataColumn? column))
                    {
                        if (!emptyColumns.TryGetValue(field.Name, out column))
                        {
                            if (groupReader.RowCount == _rowgroupSize)
                            {
                                column = new DataColumn(field,
                                    Array.CreateInstance(field.ClrNullableIfHasNullsType, _rowgroupSize));
                                emptyColumns.Add(field.Name, column);
                            }
                            else
                            {
                                column = new DataColumn(field,
                                    Array.CreateInstance(field.ClrNullableIfHasNullsType, groupReader.RowCount));
                            }
                        }
                    }

                    writer.WriteColumnAsync(column).Wait();
                }
            }
            reader.Dispose();
            File.Delete(fragment._path + fragment._nestedLevel + ".tmp");
        }
        Dispose();
        // TODO: This could cause error in cases where the old file is open in another program.
        if (File.Exists(_path))
        {
            File.Delete(_path);
        }
        File.Move(_path + _nestedLevel + ".tmp", _path);
    }

    public void Dispose()
    {
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
