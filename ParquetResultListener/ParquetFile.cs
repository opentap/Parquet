using OpenTap;
using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ParquetResultListener
{
    internal sealed class ParquetFile
    {
        private readonly string _path;
        private readonly Schema _schema;

        internal ParquetFile(string path, Schema schema)
        {
            _path = path;
            _schema = schema;
        }

        internal void PublishResult(ResultTable result)
        {
            using Stream stream = File.Open(_path, FileMode.OpenOrCreate);
            using ParquetWriter writer = new ParquetWriter(_schema, stream, append: false);
            using ParquetRowGroupWriter groupWriter = writer.CreateRowGroup();

            Dictionary<string, ResultColumn> columns = result.Columns.ToDictionary(c => c.Name, c => c);

            foreach (DataField field in _schema.Fields)
            {
                ResultColumn resultColumn = columns[field.Name];
                DataColumn parquetColumn = new DataColumn(field, resultColumn.Data);
                groupWriter.WriteColumn(parquetColumn);
            }
        }
    }
}