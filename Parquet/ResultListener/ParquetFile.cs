using Parquet;
using Parquet.Data;
using Parquet.Extensions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;

namespace OpenTap.Plugins.Parquet
{
    internal sealed class ParquetFileOptions
    {
        public bool CloseWriter { get; set; } = true;
        public bool CloseStream { get; set; } = true;
    }

    internal sealed class ParquetFile : IDisposable
    {
        private Dictionary<DataField, ArrayList> _dataCache = new Dictionary<DataField, ArrayList>();
        private readonly Stream _stream;
        private readonly ParquetWriter _writer;
        private int _rowCount = 0;
        private readonly Dictionary<string, string> _customMetadata;

        public ParquetFileOptions Options { get; }

        public string Path { get; } = string.Empty;

        public Schema Schema { get; }

        internal ParquetFile(Schema schema, Stream writeStream, ParquetFileOptions? options = null)
        {
            Schema = schema;
            _stream = writeStream;
            _writer = new ParquetWriter(Schema, _stream);
            _dataCache = schema.GetDataFields().ToDictionary(field => field, field => new ArrayList());
            Options = options ?? new ParquetFileOptions();

            string assemblyLocation = typeof(ParquetFile).Assembly.Location;
            FileVersionInfo versionInfo = FileVersionInfo.GetVersionInfo(assemblyLocation);
            _customMetadata = new Dictionary<string, string>()
            {
                {"SchemaVersion", new Version(1, 0, 0, 0).ToString() },
                {"ToolVersion", versionInfo.FileVersion },
                {"Time", DateTime.Now.ToString("O", CultureInfo.InvariantCulture) },
            };
            _writer.CustomMetadata = _customMetadata;
        }

        internal ParquetFile(Schema schema, string path, ParquetFileOptions? options = null) : this(schema, File.OpenWrite(path), options)
        {
            Path = path;
        }

        internal void AddMetadata(string key, string value)
        {
            _customMetadata.Add(key, value);
            _writer.CustomMetadata = _customMetadata;
        }

        internal void AddRows(Dictionary<string, IConvertible>? planParameters,
            Dictionary<string, IConvertible>? stepParameters,
            Dictionary<string, Array>? results,
            string? resultName,
            Guid? stepId,
            Guid? parentId)
        {
            int count = results?.Values.Max(d => d.Length) ?? 1;
            foreach (DataField field in Schema.GetDataFields())
            {
                ArrayList column = _dataCache[field];
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

        internal void AddRows(string path)
        {
            using Stream stream = File.OpenRead(path);
            AddRows(stream);
        }

        internal void AddRows(Stream stream)
        {
            using ParquetReader reader = new ParquetReader(stream);
            if (!CanContain(reader.Schema))
            {
                throw new Exception("Tried to add rows to parquet file that weren't compatible with that file.");
            }

            HashSet<DataField> fields = reader.Schema.GetDataFields().ToHashSet();
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using ParquetRowGroupReader groupReader = reader.OpenRowGroupReader(i);
                using ParquetRowGroupWriter groupWriter = _writer.CreateRowGroup();

                foreach (DataField field in Schema.GetDataFields())
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
        }

        private void WriteCache()
        {
            try
            {
                _rowCount = 0;
                using ParquetRowGroupWriter groupWriter = _writer.CreateRowGroup();
                foreach (DataField field in Schema.GetDataFields())
                {
                    ArrayList list = _dataCache[field];
                    Array data = list.ConvertList(field.DataType);
                    DataColumn column = new DataColumn(field, data);
                    groupWriter.WriteColumn(column);
                    list.Clear();
                }
            }
            catch (Exception ex)
            {
                ParquetResultListener.Log.Error(ex);
            }
        }

        internal bool CanContain(Schema schema)
        {
            return schema.GetDataFields().ToHashSet().IsSubsetOf(Schema.GetDataFields());
        }

        public void Dispose()
        {
            WriteCache();
            if (Options.CloseWriter)
            {
                _writer.Dispose();
            }
            if (Options.CloseStream)
            {
                _stream.Dispose();
            }
        }
    }
}
