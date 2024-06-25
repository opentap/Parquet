using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Parquet.Data;
using Parquet.Extensions;
using Parquet;
using ColumnData = (System.Array Data, Parquet.Schema.DataField Field);
using System.IO.Compression;

namespace OpenTap.Plugins.Parquet;

internal sealed class ParquetFragment : IDisposable
{
    private readonly string _path;
    private readonly int _rowgroupSize;
    private readonly CompressionMethod _method;
    private readonly CompressionLevel _level;
    private readonly Stream _stream;
    private ParquetWriter? _writer;
    private ParquetSchema? _schema;
    private int _cacheSize;
    private readonly List<DataField> _fields;
    private readonly Dictionary<string, ColumnData> _cache;

    public ParquetFragment(string path, int rowgroupSize, CompressionMethod method, CompressionLevel level)
    {
        _path = path;
        _rowgroupSize = rowgroupSize;
        _method = method;
        _level = level;
        string? dirPath = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dirPath) && !Directory.Exists(dirPath))
        {
            Directory.CreateDirectory(dirPath);
        }
        _stream = System.IO.File.Open(_path, FileMode.Create, FileAccess.Write);
        _fields = new();
        _cache = new();
    }

    public ParquetFragment(ParquetFragment fragment)
    {
        _path = fragment._path + ".tmp";
        _rowgroupSize = fragment._rowgroupSize;
        _method = fragment._method;
        _level = fragment._level;
        _stream = System.IO.File.Open(_path, FileMode.Create, FileAccess.Write);
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
        bool fitsInCache = true;
        while (resultCount > 0)
        {
            int count = Math.Min(_rowgroupSize - _cacheSize, resultCount);

            fitsInCache |= AddToColumn("ResultName", typeof(string), resultName, count);
            fitsInCache |= AddToColumn("Guid", typeof(Guid), guid, count);
            fitsInCache |= AddToColumn("Parent", typeof(Guid), parentId, count);
            fitsInCache |= AddToColumn("StepId", typeof(Guid), stepId, count);
            
            if (plan is not null)
                foreach (var item in plan)
                {
                    fitsInCache |= AddToColumn("Plan/" + item.Key, item.Value.GetType(), item.Value, count);
                }
            if (step is not null)
                foreach (var item in step)
                {
                    fitsInCache |= AddToColumn("Step/" + item.Key, item.Value.GetType(), item.Value, count);
                }
            if (results is not null)
                foreach(var item in results)
                {
                    fitsInCache |= AddToColumn("Results/" + item.Key, item.Value, startIndex, count);
                }

            _cacheSize += count;
            resultCount -= count;
            foreach (var item in _cache)
            {
                if (item.Value.Data.Length < _cacheSize)
                {
                    fitsInCache |= AddToColumn(item.Key, item.Value.Field.ClrType, null, count);
                }
            }

            if (!fitsInCache && _writer is not null)
            {
                return false;
            }
            
            if (_cacheSize >= _rowgroupSize)
            {
                WriteCache();
                startIndex += count;
            }
        }
        return true;
    }

    private bool AddToColumn(string name, Array values, int startIndex, int count){
        Type type = values.GetType().GetElementType();
        bool fitsInCache = GetOrCreateColumn(name, type, out ColumnData data, out Type columnType);

        Array.Copy(values.Cast<object?>().Skip(startIndex).ToArray(), startIndex, data.Data, _cacheSize, count);

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

        return fitsInCache;
    }

    private bool GetOrCreateColumn(string name, Type type, out ColumnData data, out Type parquetType)
    {
        parquetType = GetParquetType(type);
        if (!_cache.TryGetValue(name, out data))
        {
            DataField field = new DataField(name, parquetType, true);
            data = (Array.CreateInstance(parquetType.AsNullable(), _rowgroupSize), field);
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
            _writer = ParquetWriter.CreateAsync(_schema, _stream).Result;
            _writer.CompressionMethod = _method;
            _writer.CompressionLevel = _level;
        }
        ParquetRowGroupWriter rowGroupWriter = _writer.CreateRowGroup();
        for (var i = 0; i < _schema.DataFields.Length; i++)
        {
            ColumnData data = _cache[_schema.DataFields[i].Name];
            Array arr = data.Data;
            if (_cacheSize != _rowgroupSize){
                arr = Array.CreateInstance(_schema.DataFields[i].ClrNullableIfHasNullsType, _cacheSize);
                Array.Copy(data.Data, arr, _cacheSize);
            }
            DataColumn column = new DataColumn(_schema.DataFields[i], arr);
            rowGroupWriter.WriteColumnAsync(column).Wait();
        }
        _cacheSize = 0;
        rowGroupWriter.Dispose();
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
