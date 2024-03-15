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
    /// <summary>
    /// Options for creating a parquet file.
    /// </summary>
    public sealed class ParquetFileOptions
    {
        /// <summary>
        /// If true; the parquet writer will be closed when the file is disposed.
        /// </summary>
        public bool CloseWriter { get; set; } = true;
        /// <summary>
        /// If true; the underlying stream will be closed when the file is disposed.
        /// </summary>
        public bool CloseStream { get; set; } = true;
    }

    /// <summary>
    /// Represents a single parquet file on the disc.
    /// </summary>
    public sealed class ParquetFile : IDisposable
    {
        private Dictionary<DataField, ArrayList> _dataCache = new Dictionary<DataField, ArrayList>();
        private readonly Stream _stream;
        private readonly ParquetWriter _writer;
        private int _rowCount = 0;

        /// <summary>
        /// Gets the options of this parquet file.
        /// </summary>
        public ParquetFileOptions Options { get; }

        /// <summary>
        /// Gets the path of this parquet file on disc if any (if opened from a stream the path will be empty string).
        /// </summary>
        public string Path { get; } = string.Empty;

        /// <summary>
        /// Gets the schema of this parquet file, this is usefull to check if files can be stored within each other.
        /// </summary>
        public Schema Schema { get; }

        /// <summary>
        /// Create a new parquet file from a stream.
        /// </summary>
        /// <param name="schema">The schema of the parquet file.</param>
        /// <param name="writeStream">The stream to write the parquet file to.</param>
        /// <param name="options">The options of this parquet file, leave null for default.</param>
        public ParquetFile(Schema schema, Stream writeStream, ParquetFileOptions? options = null)
        {
            Schema = schema;
            _stream = writeStream;
            _writer = new ParquetWriter(Schema, _stream);
            _dataCache = schema.GetDataFields().ToDictionary(field => field, field => new ArrayList());
            Options = options ?? new ParquetFileOptions();
        }

        /// <summary>
        /// Create a new parquet file that writes to a path on the file system.
        /// </summary>
        /// <param name="schema">The schema of the parquet file.</param>
        /// <param name="path">The path to write the parquet file to.</param>
        /// <param name="options">The options of this parquet file, leave null for default.</param>
        public ParquetFile(Schema schema, string path, ParquetFileOptions? options = null) : this(schema, File.OpenWrite(path), options)
        {
            Path = path;
        }

        /// <summary>
        /// Add rows to the parquet file for each parameter in a testplan.
        /// </summary>
        /// <param name="planParameters">The parameters of the test plan.</param>
        /// <param name="guid">The guid of the plan run.</param>
        public void AddPlanParameters(Dictionary<string, IConvertible> planParameters, Guid guid)
        {
            AddRows(planParameters, null, null, null, guid, null);
        }

        /// <inheritdoc cref="AddPlanParameters(Dictionary{string, IConvertible}, Guid)"/>
        /// <param name="planRun">The plan to add the parameters of.</param>
        public void AddPlanParameters(TestPlanRun planRun)
        {
            AddPlanParameters(planRun.GetParameters(), planRun.Id);
        }

        /// <summary>
        /// Add rows to the parquet file for each parameter in a test step.
        /// </summary>
        /// <param name="stepParameters">The parameters of the test step.</param>
        /// <param name="guid">The guid of the step run.</param>
        /// <param name="parent">The guid of the step runs parent.</param>
        public void AddStepParameters(Dictionary<string, IConvertible> stepParameters, Guid guid, Guid parent)
        {
            AddRows(null, stepParameters, null, null, guid, parent);
        }

        /// <inheritdoc cref="AddStepParameters(Dictionary{string, IConvertible}, Guid, Guid)"/>
        /// <param name="step">The step to add the parameters of.</param>
        public void AddStepParameters(TestStepRun step)
        {
            AddStepParameters(step.GetParameters(), step.Id, step.Parent);
        }

        /// <summary>
        /// Add rows to the parquet file for each result in the step.
        /// </summary>
        /// <param name="stepParameters">The parameters of the test step.</param>
        /// <param name="results">The results of the step.</param>
        /// <param name="guid">The guid of the step run.</param>
        /// <param name="parent">The guid of the step runs parent.</param>
        public void AddStepResults(Dictionary<string, IConvertible> stepParameters, Dictionary<string, Array> results, string resultName, Guid guid, Guid parent)
        {
            AddRows(null, stepParameters, results, resultName, guid, parent);
        }

        /// <inheritdoc cref="AddStepResults(Dictionary{string, IConvertible}, Dictionary{string, Array}, string, Guid, Guid)"/>
        /// <param name="step">The step that generated the result table.</param>
        /// <param name="table">The result table to add.</param>
        public void AddStepResults(TestStepRun step, ResultTable table)
        {
            AddStepResults(step.GetParameters(), table.GetResults(), table.Name, step.Id, step.Parent);
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

        /// <summary>
        /// Merge this parquet file with another file, adding the rows from that file.
        /// </summary>
        /// <param name="path">The path to get the other parquet file from.</param>
        /// <exception cref="Exception">Throws exception if the file at path can't be merged with this file because of schema incompatibility.</exception>
        public void AddRows(string path)
        {
            using Stream stream = File.OpenRead(path);
            AddRows(stream);
        }

        /// <summary>
        /// Merge this parquet file with another file, adding the rows from that file.
        /// </summary>
        /// <param name="stream">The stream of the other parquet file.</param>
        /// <exception cref="Exception">Throws exception if the file at the stream can't be merged with this file because of schema incompatibility.</exception>
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

        /// <summary>
        /// Force the parquet file to write the current cache to the file system.
        /// </summary>
        public void WriteCache()
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

        /// <summary>
        /// Check if this parquet file can contain the given schema.
        /// </summary>
        /// <param name="schema">The other schema, checks if this file can contain this schema.</param>
        /// <returns>True; if this parquet file has the same fields with the same types as schema. Otherwise; returns false.</returns>
        public bool CanContain(Schema schema)
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
