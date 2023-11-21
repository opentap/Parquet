using Parquet;
using Parquet.Data;
using Parquet.Extensions;
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
            if (!File.Exists(path))
            {
                _schema = schema.ToSchema();
                _stream = File.OpenWrite(path);
                _writer = new ParquetWriter(_schema, _stream);
                _cachedData = schema.GetDataFields().ToDictionary(field => field, field => new ArrayList());
                Path = path;
            }
            else
            {
                File.Move(path, path + ".tmp");
                using Stream stream = File.OpenRead(path + ".tmp");
                using ParquetReader reader = new ParquetReader(stream);
                schema.Union(reader.Schema);
                _schema = schema.ToSchema();
                _stream = File.OpenWrite(path);
                _writer = new ParquetWriter(_schema, _stream);
                _cachedData = schema.GetDataFields().ToDictionary(field => field, field => new ArrayList());
                Path = path;

                HashSet<DataField> fields = reader.Schema.GetDataFields().ToHashSet();
                for (int i = 0; i < reader.RowGroupCount; i++)
                {
                    using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(i);
                    using ParquetRowGroupWriter groupWriter = _writer.CreateRowGroup();

                    foreach (DataField field in _schema.GetDataFields())
                    {
                        DataColumn column;
                        if (fields.Contains(field))
                        {
                            Array data = groupReader.ReadColumn(field).Data;
                            column = new DataColumn(field, data);
                        }
                        else
                        {
                            ArrayList arrayList = new ArrayList(Enumerable.Repeat<object?>(null, (int)groupReader.RowCount).ToArray());
                            Array data = ArrayListExtensions.ConvertList(arrayList, field.DataType);
                            column = new DataColumn(field, data);
                        }
                        groupWriter.WriteColumn(column);
                    }
                }
                reader.Dispose();
                stream.Flush();
                stream.Dispose();
                File.Delete(path + ".tmp");
            }
        }

        internal void OnlyParameters(TestPlanRun planRun)
        {
            Dictionary<string, IConvertible> parameters = TestRunExtensions.GetParameters(planRun);

            AddRows(parameters, null, null, null, planRun.Id, null);
        }

        internal void OnlyParameters(TestStepRun stepRun)
        {
            Dictionary<string, IConvertible> parameters = TestRunExtensions.GetParameters(stepRun);

            AddRows(null, parameters, null, null, stepRun.Id, stepRun.Parent);
        }

        internal void Results(TestStepRun stepRun, ResultTable table)
        {
            Dictionary<string, IConvertible> parameters = stepRun.GetParameters();
            Dictionary<string, Array> results = table.GetResults();

            AddRows(null, parameters, results, table.Name, stepRun.Id, stepRun.Parent);
        }

        private void AddRows(Dictionary<string, IConvertible>? planParameters,
            Dictionary<string, IConvertible>? stepParameters,
            Dictionary<string, Array>? results,
            string? resultName,
            Guid? stepId,
            Guid? parentId)
        {
            int count = results?.Values.Max(d => d.Length) ?? 1;
            foreach (DataField field in _schema.GetDataFields())
            {
                ArrayList column = _cachedData[field];
                switch (SchemaBuilder.GetFieldType(field, out string name))
                {
                    case FieldType.Plan:
                        column.AddRange(Enumerable.Repeat(planParameters?.GetValueOrDefault(name), count).ToArray());
                        break;
                    case FieldType.Step:
                        column.AddRange(Enumerable.Repeat(stepParameters?.GetValueOrDefault(name), count).ToArray());
                        break;
                    case FieldType.Result:
                        column.AddRange(results?.GetValueOrDefault(name) ?? Enumerable.Repeat<object?>(null, count).ToArray());
                        break;
                    case FieldType.ResultName:
                        column.AddRange(Enumerable.Repeat(resultName, count).ToArray());
                        break;
                    case FieldType.Guid:
                        column.AddRange(Enumerable.Repeat(stepId, count).ToArray());
                        break;
                    case FieldType.Parent:
                        column.AddRange(Enumerable.Repeat(parentId, count).ToArray());
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
            foreach (DataField field in _schema.GetDataFields())
            {
                ArrayList list = _cachedData[field];
                Array data = list.ConvertList(field.DataType);
                DataColumn column = new DataColumn(field, data);
                groupWriter.WriteColumn(column);
                list.Clear();
            }
            groupWriter.Dispose();
        }

        internal bool CanContain(SchemaBuilder schema)
        {
            return _schema.GetDataFields().ToHashSet().IsSubsetOf(schema.GetDataFields());
        }

        public void Dispose()
        {
            WriteCache();
            _stream.Flush();
            _writer.Dispose();
            _stream.Dispose();
        }
    }
}
