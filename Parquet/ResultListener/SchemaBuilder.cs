using OpenTap;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Protocol;

namespace Parquet.ResultListener
{
    internal enum ColumnType
    {
        Plan,
        Step,
        Result,
        Guid,
        Parent,
    }

    internal static class SchemaBuilder
    {
        private const string Plan = "Plan";
        private const string Step = "Step";
        private const string Result = "Result";
        private const string Guid = "Guid/Guid";
        private const string Parent = "Guid/Parent";

        private static DataField GetField(Type type, params string[] path)
        {
            // Enums not directly supported.
            if (type.IsEnum)
            {
                type = typeof(string);
            }

            return new DataField(GetValidParquetName(path), type.GetNullableType());
        }
        internal static Schema FromTestStepRun(TestStepRun run)
        {
            List<DataField> fields = new List<DataField>()
            {
                GetField(typeof(string), Guid),
                GetField(typeof(string), Parent),
            };


            AddParameters(fields, Step, run);

            return new Schema(fields);
        }

        internal static Schema FromTestPlanRun(TestPlanRun run)
        {
            List<DataField> fields = new List<DataField>()
            {
                GetField(typeof(string), Guid),
            };

            AddParameters(fields, Plan, run);

            return new Schema(fields);
        }

        internal static Schema FromResult(TestStepRun run, ResultTable result)
        {
            List<DataField> fields = new List<DataField>()
            {
                GetField(typeof(string), Guid),
                GetField(typeof(string), Parent),
            };

            AddParameters(fields, Step, run);

            foreach (ResultColumn? column in result.Columns)
            {
                if (column.Data is not null && column.Data.Length > 0)
                {
                    fields.Add(GetField(column.Data.GetValue(0).GetType(), Result, column.Name));
                }
            }
            return new Schema(fields);
        }

        private static void AddParameters(List<DataField> fields, string group, TestRun run)
        {
            foreach (ResultParameter? parameter in run.Parameters)
            {
                fields.Add(GetField(parameter.Value.GetType(), group, parameter.Group, parameter.Name));
            }
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
                        return ColumnType.Plan;
                    }
                    throw new ArgumentException("Field was not created by the schema builder.", nameof(field));
            }
        }
    }
}
