using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenTap.Plugins.Parquet
{
    internal enum ColumnType
    {
        Plan,
        Step,
        Result,
        Guid,
        Parent,
    }

    internal class SchemaBuilder
    {
        private const string Plan = "Plan";
        private const string Step = "Step";
        private const string Result = "Result";
        private const string Guid = "Guid";
        private const string Parent = "Parent";

        private readonly List<DataField> _fields;

        internal SchemaBuilder()
        {
            _fields = new List<DataField>()
            {
                CreateField(typeof(string), Guid),
                CreateField(typeof(string), Parent),
            };
        }

        internal SchemaBuilder AddResultFields(TestStepRun run, ResultTable result)
        {
            AddParameters(Step, run);

            foreach (ResultColumn? column in result.Columns)
            {
                if (column.Data is not null && column.Data.Length > 0)
                {
                    _fields.Add(CreateField(column.Data.GetValue(0).GetType(), Result, column.Name));
                }
            }
            return this;
        }

        internal SchemaBuilder AddStepParameters(TestStepRun run)
        {
            AddParameters(Step, run);
            return this;
        }

        internal SchemaBuilder AddPlanParameters(TestPlanRun run)
        {
            AddParameters(Plan, run);
            return this;
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

        private void AddParameters(string group, TestRun run)
        {
            foreach (ResultParameter? parameter in run.Parameters)
            {
                _fields.Add(CreateField(parameter.Value.GetType(), group, parameter.Group, parameter.Name));
            }
        }

        private static DataField CreateField(Type type, params string[] path)
        {
            // Enums not directly supported.
            if (type.IsEnum)
            {
                type = typeof(string);
            }

            return new DataField(GetValidParquetName(path), type.GetNullableType());
        }

        internal static string GetValidParquetName(params string[] path)
        {
            return string.Join("/", path).Replace(".", ",");
        }

        internal static ColumnType GetColumnType(DataField field, out string name)
        {
            string[] pathParts = field.Name.Split('/');
            name = GetValidParquetName(pathParts.Skip(1).ToArray());
            switch (pathParts[0])
            {
                case Plan:
                    return ColumnType.Plan;
                case Step:
                    return ColumnType.Step;
                case Result:
                    return ColumnType.Result;
                default:
                    if (field.Name == Guid)
                    {
                        return ColumnType.Guid;
                    }
                    if (field.Name == Parent)
                    {
                        return ColumnType.Parent;
                    }
                    throw new ArgumentException("Field was not created by the schema builder.", nameof(field));
            }
        }

        public override bool Equals(object? obj)
        {
            return obj is SchemaBuilder schema &&
                   _fields.SequenceEqual(schema._fields);
        }
    }
}
