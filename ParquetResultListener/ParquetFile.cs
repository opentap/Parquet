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

        internal void PublishResult(TestStepRun stepRun, ResultTable result)
        {
            using Stream stream = File.Open(_path, FileMode.OpenOrCreate);
            using ParquetWriter writer = new ParquetWriter(_schema, stream, append: false);
            using ParquetRowGroupWriter groupWriter = writer.CreateRowGroup();

            Dictionary<string, ResultColumn> columns = result.Columns.ToDictionary(c => c.Name, c => c);
            Dictionary<string, ResultParameter> parameters = stepRun.Parameters.ToDictionary(p => p.Name, p => p);

            int rows = result.Rows;
            foreach (DataField field in _schema.Fields)
            {
                Array data;
                switch (SchemaBuilder.GetColumnType(field, out string fieldName))
                {
                    case ColumnType.Step:
                        data = CreateDataArray(parameters[fieldName].Value, rows, field.DataType);
                        break;
                    case ColumnType.Result:
                        data = columns[fieldName].Data;
                        break;
                    case ColumnType.Guid:
                        if (fieldName == "Guid")
                        {
                            data = Enumerable.Repeat(stepRun.Id.ToString(), rows).ToArray();
                        }
                        else if (fieldName == "Parent")
                        {
                            data = Enumerable.Repeat(stepRun.Parent.ToString(), rows).ToArray();
                        }
                        else
                        {
                            throw new FormatException("Schema is not of a format that can be filled by this instance.");
                        }
                        break;
                    default:
                        throw new FormatException("Schema is not of a format that can be filled by this instance.");
                }
                DataColumn parquetColumn = new DataColumn(field, data);
                groupWriter.WriteColumn(parquetColumn);
            }
        }

        private static Array CreateDataArray(IConvertible convertible, int rows, DataType type)
        {
            switch (type)
            {
                case DataType.Boolean:
                    return Enumerable.Repeat(Convert.ToBoolean(convertible), rows).ToArray();
                case DataType.Byte:
                    return Enumerable.Repeat(Convert.ToByte(convertible), rows).ToArray();
                case DataType.SignedByte:
                    return Enumerable.Repeat(Convert.ToSByte(convertible), rows).ToArray();
                case DataType.UnsignedByte:
                    return Enumerable.Repeat(Convert.ToByte(convertible), rows).ToArray();
                case DataType.Short:
                    return Enumerable.Repeat(Convert.ToInt16(convertible), rows).ToArray();
                case DataType.UnsignedShort:
                    return Enumerable.Repeat(Convert.ToUInt16(convertible), rows).ToArray();
                case DataType.Int16:
                    return Enumerable.Repeat(Convert.ToInt16(convertible), rows).ToArray();
                case DataType.UnsignedInt16:
                    return Enumerable.Repeat(Convert.ToUInt16(convertible), rows).ToArray();
                case DataType.Int32:
                    return Enumerable.Repeat(Convert.ToInt32(convertible), rows).ToArray();
                case DataType.Int64:
                    return Enumerable.Repeat(Convert.ToInt64(convertible), rows).ToArray();
                case DataType.String:
                    return Enumerable.Repeat(Convert.ToString(convertible), rows).ToArray();
                case DataType.Float:
                    return Enumerable.Repeat(Convert.ToSingle(convertible), rows).ToArray();
                case DataType.Double:
                    return Enumerable.Repeat(Convert.ToDouble(convertible), rows).ToArray();
                case DataType.Decimal:
                    return Enumerable.Repeat(Convert.ToDecimal(convertible), rows).ToArray();
                case DataType.DateTimeOffset:
                    return Enumerable.Repeat(new DateTimeOffset((DateTime)convertible), rows).ToArray();
                default:
                    throw new NotImplementedException();
            }
        }
    }
}