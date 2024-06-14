using Parquet.Schema;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Extensions;
using ColumnKey = (string Name, System.Type Type);
using ColumnData = (System.Collections.ArrayList Data, Parquet.Schema.DataField Field);

namespace Parquet.ResultListener;

internal sealed class ParquetFragment : IDisposable
{
    private readonly string _path;
    private readonly Stream _stream;
    private ParquetWriter? _writer;
    private ParquetSchema? _schema;
    private int _cacheSize;
    private readonly List<DataField> _fields;
    private readonly Dictionary<string, ColumnData> _cache;

    public ParquetFragment(string path)
    {
        _path = path;
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
        _stream = System.IO.File.Open(_path, FileMode.Create, FileAccess.Write);
        _cacheSize = fragment._cacheSize;
        _fields = fragment._fields.ToList();
        _cache = fragment._cache;
    }

    public bool AddRows(string? resultName, Guid? guid, Guid? parentId, Guid? stepId,
        Dictionary<string, IConvertible>? plan,
        Dictionary<string, IConvertible>? step,
        Dictionary<string, Array>? results)
    {
        int count = results?.Values.Max(d => d.Length) ?? 1;
        bool fitsInCache = true;

        AddToCache("ResultName", typeof(string), Enumerable.Repeat(resultName, count).ToArray());
        AddToCache("Guid", typeof(Guid), Enumerable.Repeat(guid, count).ToArray());
        AddToCache("Parent", typeof(Guid), Enumerable.Repeat(parentId, count).ToArray());
        AddToCache("StepId", typeof(Guid), Enumerable.Repeat(stepId, count).ToArray());

        if (plan is not null)
            foreach (var item in plan)
            {
                AddToCache("Plan/" + item.Key, item.Value.GetType(), Enumerable.Repeat(item.Value, count).ToArray());
            }
        if (step is not null)
            foreach (var item in step)
            {
                AddToCache("Step/" + item.Key, item.Value.GetType(), Enumerable.Repeat(item.Value, count).ToArray());
            }
        if (results is not null)
            foreach(var item in results)
            {
                AddToCache("Results/" + item.Key, item.Value.GetValue(0).GetType(), item.Value);
            }

        _cacheSize += count;
        foreach (var item in _cache)
        {
            if (item.Value.Item1.Count < _cacheSize)
            {
                AddToCache(item.Key, item.Value.Field.ClrType, Enumerable.Repeat<object?>(null, count).ToArray());
            }
        }

        if ((!fitsInCache && _writer is not null))
        {
            return false;
        }
        
        if (_cacheSize >= 1000)
        {
            WriteCache();
        }

        return true;
        

        void AddToCache(string name, Type type, Array values)
        {
            Type parquetType = GetParquetType(type);
            if (!_cache.TryGetValue(name, out ColumnData data))
            {
                fitsInCache = false;
                data = (new ArrayList(), new DataField(name, parquetType, true));
                data.Data.AddRange(Enumerable.Repeat<object?>(null, _cacheSize).ToArray());
                _cache.Add(name, data);
                _fields.Add(data.Field);
            }
            data.Data.AddRange(values);
        }
    }

    public void WriteCache()
    {
        if (_writer is null || _schema is null)
        {
            _schema = new ParquetSchema(_fields);
            _writer = ParquetWriter.CreateAsync(_schema, _stream).Result;
        }

        ParquetRowGroupWriter rowGroupWriter = _writer.CreateRowGroup();
        foreach (var field in _schema.DataFields)
        {
            DataColumn column;
            ArrayList data = _cache[field.Name].Data;
            column = new DataColumn(field, data.ConvertList(field.ClrNullableIfHasNullsType));
            rowGroupWriter.WriteColumnAsync(column).Wait();
            data.Clear();
        }
        rowGroupWriter.Dispose();

        _cacheSize = 0;
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
