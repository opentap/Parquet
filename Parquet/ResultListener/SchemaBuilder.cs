using Parquet.Data;
using Parquet.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenTap.Plugins.Parquet
{
    internal enum FieldType
    {
        Plan,
        Step,
        Result,
        ResultName,
        Guid,
        Parent,
    }

    internal class SchemaBuilder
    {
        private readonly List<DataField> _fields;

        internal SchemaBuilder()
        {
            _fields = new List<DataField>()
            {
                CreateField(typeof(string), nameof(FieldType.ResultName)),
                CreateField(typeof(string), nameof(FieldType.Guid)),
                CreateField(typeof(string), nameof(FieldType.Parent)),
            };
        }

        internal void Union(Schema schema)
        {
            IEnumerable<DataField> fields = _fields.Union(schema.GetDataFields()).ToList();
            _fields.Clear();
            _fields.AddRange(fields);
        }

        internal void AddResults(ResultTable result)
        {
            foreach (ResultColumn? column in result.Columns)
            {
                if (column.Data is not null && column.Data.Length > 0)
                {
                    _fields.Add(CreateField(column.Data.GetValue(0).GetType(), nameof(FieldType.Result), column.Name));
                }
            }
        }

        internal Schema ToSchema()
        {
            return new Schema(_fields);
        }

        internal IEnumerable<DataField> GetDataFields()
        {
            foreach (DataField field in _fields)
            {
                yield return field;
            }
        }

        internal void AddParameters(FieldType group, TestRun run)
        {
            foreach (ResultParameter? parameter in run.Parameters)
            {
                _fields.Add(CreateField(parameter.Value.GetType(), group.ToString(), parameter.Group, parameter.Name));
            }
        }

        internal static string GetValidParquetName(params string[] path)
        {
            return string.Join("/", path.Where(s => !string.IsNullOrWhiteSpace(s))).Replace(".", " ").Replace(",", " ");
        }

        private static DataField CreateField(Type type, params string[] path)
        {
            // Enums not directly supported.
            if (type.IsEnum)
            {
                type = typeof(string);
            }

            return new DataField(GetValidParquetName(path), type.AsNullable());
        }

        internal static FieldType GetFieldType(DataField field, out string name)
        {
            string[] pathParts = field.Name.Split('/');
            name = GetValidParquetName(pathParts.Skip(1).ToArray());
            switch (pathParts[0])
            {
                case nameof(FieldType.Plan):
                    return FieldType.Plan;
                case nameof(FieldType.Step):
                    return FieldType.Step;
                case nameof(FieldType.Result):
                    return FieldType.Result;
                default:
                    if (field.Name == nameof(FieldType.ResultName))
                    {
                        return FieldType.ResultName;
                    }
                    if (field.Name == nameof(FieldType.Guid))
                    {
                        return FieldType.Guid;
                    }
                    if (field.Name == nameof(FieldType.Parent))
                    {
                        return FieldType.Parent;
                    }
                    throw new ArgumentException("Field was not created by the schema builder.", nameof(field));
            }
        }
    }
}
