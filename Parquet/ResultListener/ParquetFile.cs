using Parquet;
using Parquet.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace OpenTap.Plugins.Parquet
{
    internal sealed class ParquetFile : IDisposable
    {
        private Dictionary<DataField, ArrayList> _cachedData = new Dictionary<DataField, ArrayList>();
        private readonly Schema _schema;
        private readonly Stream _stream;
        private readonly ParquetWriter _writer;
        private int _rowCount = 0;

        public string Path { get; }

        internal ParquetFile(SchemaBuilder schema, string path)
        {
            // If the file already exists we should merge the new file with the old data first.
            bool merge = File.Exists(path);
            if (merge)
            {
                File.Move(path, path + ".tmp");
            }

            _schema = schema.ToSchema();
            _stream = File.OpenWrite(path);
            _writer = new ParquetWriter(_schema, _stream);
            _cachedData = schema.GetDataFields().ToDictionary(field => field, field => new ArrayList());
            Path = path;

            if (merge)
            {
                using Stream stream = File.OpenRead(path + ".tmp");
                using ParquetReader reader = new ParquetReader(stream);
                schema.Union(reader.Schema);
                _schema = schema.ToSchema();


            }
        }

        internal void OnlyParameters(TestPlanRun planRun)
        {
            Dictionary<string, IConvertible> parameters = GetParameters(planRun);

            foreach (DataField field in _schema.GetDataFields())
            {
                ArrayList column = _cachedData[field];
                switch (SchemaBuilder.GetColumnType(field, out string name))
                {
                    case ColumnType.Plan:
                        column.Add(parameters[name]);
                        break;
                    case ColumnType.Step:
                        column.Add(null);
                        break;
                    case ColumnType.Result:
                        column.Add(null);
                        break;
                    case ColumnType.Guid:
                        column.Add(planRun.Id);
                        break;
                    case ColumnType.Parent:
                        column.Add(null);
                        break;
                        // TODO: Default?
                }
            }
            _rowCount += 1;
            if (_rowCount > 500)
            {
                WriteCache();
            }
        }

        internal void OnlyParameters(TestStepRun stepRun)
        {
            Dictionary<string, IConvertible> parameters = GetParameters(stepRun);

            foreach (DataField field in _schema.GetDataFields())
            {
                ArrayList column = _cachedData[field];
                switch (SchemaBuilder.GetColumnType(field, out string name))
                {
                    case ColumnType.Plan:
                        column.Add(null);
                        break;
                    case ColumnType.Step:
                        column.Add(parameters[name]);
                        break;
                    case ColumnType.Result:
                        column.Add(null);
                        break;
                    case ColumnType.Guid:
                        column.Add(stepRun.Id);
                        break;
                    case ColumnType.Parent:
                        column.Add(stepRun.Parent);
                        break;
                        // TODO: Default?
                }
            }
            _rowCount += 1;
            if (_rowCount > 500)
            {
                WriteCache();
            }
        }

        internal void Results(TestStepRun stepRun, ResultTable table)
        {
            Dictionary<string, IConvertible> parameters = GetParameters(stepRun);
            Dictionary<string, Array> results = table.Columns.ToDictionary(c => c.Name, c => c.Data);
            int count = table.Columns.Max(c => c.Data.Length);

            foreach (DataField field in _schema.GetDataFields())
            {
                ArrayList column = _cachedData[field];
                switch (SchemaBuilder.GetColumnType(field, out string name))
                {
                    case ColumnType.Plan:
                        column.AddRange(Enumerable.Repeat<object?>(null, count).ToArray());
                        break;
                    case ColumnType.Step:
                        column.AddRange(Enumerable.Repeat(parameters[name], count).ToArray());
                        break;
                    case ColumnType.Result:
                        column.AddRange(results[name]);
                        break;
                    case ColumnType.Guid:
                        column.AddRange(Enumerable.Repeat(stepRun.Id, count).ToArray());
                        break;
                    case ColumnType.Parent:
                        column.AddRange(Enumerable.Repeat(stepRun.Parent, count).ToArray());
                        break;
                }
            }
            _rowCount += count;
            if (_rowCount > 500)
            {
                WriteCache();
            }
        }

        private void AddRows(Dictionary<string, IConvertible> planParameters, Dictionary<string, IConvertible> stepParameters, Dictionary<string, Array> results, Guid stepId, Guid parentId, int count)
        {
            foreach (DataField field in _schema.GetDataFields())
            {
                ArrayList column = _cachedData[field];
                switch (SchemaBuilder.GetColumnType(field, out string name))
                {
                    case ColumnType.Plan:
                        column.AddRange(Enumerable.Repeat<object?>(null, count).ToArray());
                        break;
                    case ColumnType.Step:
                        column.AddRange(Enumerable.Repeat(stepParameters[name], count).ToArray());
                        break;
                    case ColumnType.Result:
                        column.AddRange(results[name]);
                        break;
                    case ColumnType.Guid:
                        column.AddRange(Enumerable.Repeat(stepId, count).ToArray());
                        break;
                    case ColumnType.Parent:
                        column.AddRange(Enumerable.Repeat(stepRun.Parent, count).ToArray());
                        break;
                }
            }
            _rowCount += count;
            if (_rowCount > 500)
            {
                WriteCache();
            }
        }

        private void WriteCache()
        {
            _rowCount = 0;
            ParquetRowGroupWriter groupWriter = _writer.CreateRowGroup();
            foreach (KeyValuePair<DataField, ArrayList> kvp in _cachedData)
            {
                DataField field = kvp.Key;
                ArrayList list = kvp.Value;
                Array data = ConvertList(list, field.DataType);
                DataColumn column = new DataColumn(field, data);
                groupWriter.WriteColumn(column);
            }
            groupWriter.Dispose();
        }

        internal bool CanContain(SchemaBuilder schema)
        {
            // TODO: Check subset instead of equality.
            return _schema.Equals(schema);
        }

        public void Dispose()
        {
            WriteCache();
            _stream.Flush();
            _writer.Dispose();
            _stream.Dispose();
        }

        private static Array ConvertList(ArrayList list, DataType type)
        {
            switch (type)
            {
                case DataType.Boolean:
                    return list.ToArray(typeof(bool?));
                case DataType.Byte:
                    return list.ToArray(typeof(byte?));
                case DataType.SignedByte:
                    return list.ToArray(typeof(sbyte?));
                case DataType.UnsignedByte:
                    return list.ToArray(typeof(byte?));
                case DataType.Short:
                    return list.ToArray(typeof(short?));
                case DataType.UnsignedShort:
                    return list.ToArray(typeof(ushort?));
                case DataType.Int16:
                    return list.ToArray(typeof(short?));
                case DataType.UnsignedInt16:
                    return list.ToArray(typeof(ushort?));
                case DataType.Int32:
                    return list.ToArray(typeof(int?));
                case DataType.UnsignedInt32:
                    return list.ToArray(typeof(uint?));
                case DataType.Int64:
                    return list.ToArray(typeof(long?));
                case DataType.UnsignedInt64:
                    return list.ToArray(typeof(ulong?));
                case DataType.String:
                    return list.OfType<object?>().Select(o => o?.ToString()).ToArray();
                case DataType.Float:
                    return list.ToArray(typeof(float?));
                case DataType.Double:
                    return list.ToArray(typeof(double?));
                case DataType.Decimal:
                    return list.ToArray(typeof(decimal?));
                case DataType.TimeSpan:
                    return list.ToArray(typeof(TimeSpan?));
                case DataType.DateTimeOffset:
                    return list.OfType<IConvertible?>().Select<IConvertible?, DateTimeOffset?>(dt => dt is null ? null : new DateTimeOffset((DateTime)dt)).ToArray();
                case DataType.Unspecified:
                case DataType.Int96:
                case DataType.ByteArray:
                case DataType.Interval:
                default:
                    throw new Exception($"Could not create column of type {type}");
            }
        }

        private static Dictionary<string, IConvertible> GetParameters(TestRun planRun)
        {
            return planRun.Parameters
                            .ToDictionary(p => SchemaBuilder.GetValidParquetName(p.Group, p.Name), p => p.Value);
        }
    }
}
