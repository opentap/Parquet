using Parquet.Data;
using Parquet.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenTap.Plugins.Parquet
{
    /// <summary>
    /// An enum specifying the type of field stored in a column.
    /// </summary>
    public enum FieldType
    {
        Plan,
        Step,
        Result,
        ResultName,
        Guid,
        Parent,
    }

    /// <summary>
    /// Can build a schema for a <see cref="ParquetFile"/>.
    /// </summary>
    public class SchemaBuilder
    {
        private readonly List<DataField> _fields;

        /// <summary>
        /// Create a new schemabuilder with non optional collumns.
        /// </summary>
        public SchemaBuilder()
        {
            _fields = new List<DataField>()
            {
                CreateField(typeof(string), nameof(FieldType.ResultName)),
                CreateField(typeof(string), nameof(FieldType.Guid)),
                CreateField(typeof(string), nameof(FieldType.Parent)),
            };
        }

        /// <summary>
        /// Union this schema with another already built schema. Adding the fields of the other schema to this schema.
        /// </summary>
        /// <param name="schema">The other schema to union with this one.</param>
        public void Union(Schema schema)
        {
            IEnumerable<DataField> fields = _fields.Union(schema.GetDataFields()).ToList();
            _fields.Clear();
            _fields.AddRange(fields);
        }

        /// <summary>
        /// Add result columns to this schema builder.
        /// </summary>
        /// <param name="result">The result table to get the columns from.</param>
        public void AddResults(ResultTable result)
        {
            foreach (ResultColumn? column in result.Columns)
            {
                if (column.Data is not null && column.Data.Length > 0)
                {
                    _fields.Add(CreateField(column.Data.GetValue(0).GetType(), nameof(FieldType.Result), column.Name));
                }
            }
        }

        /// <summary>
        /// Add step parameters as columns to the schema.
        /// </summary>
        /// <param name="parameters">The parameters of the step.</param>
        public void AddStepParameters(IEnumerable<(IConvertible value, string group, string name)> parameters)
        {
            _fields.AddRange(parameters.Select(p => CreateField(p.value.GetType(), FieldType.Step.ToString(), p.group, p.name)));
        }

        /// <inheritdoc cref="AddStepParameters(IEnumerable{ValueTuple{IConvertible, string, string}})"/>
        public void AddStepParameters(params (IConvertible value, string group, string name)[] parameters)
        {
            AddStepParameters(parameters);
        }

        /// <inheritdoc cref="AddStepParameters(IEnumerable{ValueTuple{IConvertible, string, string}})"/>
        /// <param name="stepRun">The step to add parameters from.</param>
        public void AddStepParameters(TestStepRun stepRun)
        {
            AddStepParameters(stepRun.Parameters.Select(p => (p.Value, p.Group, p.Name)));
        }

        /// <summary>
        /// Add plan parameters as columns to the schema.
        /// </summary>
        /// <param name="parameters">The parameters of the plan.</param>
        public void AddPlanParameters(IEnumerable<(IConvertible value, string group, string name)> parameters)
        {
            _fields.AddRange(parameters.Select(p => CreateField(p.value.GetType(), FieldType.Plan.ToString(), p.group, p.name)));
        }

        /// <inheritdoc cref="AddPlanParameters(IEnumerable{ValueTuple{IConvertible, string, string}})"/>
        public void AddPlanParameters(params (IConvertible value, string group, string name)[] parameters)
        {
            AddPlanParameters(parameters);
        }

        /// <inheritdoc cref="AddPlanParameters(IEnumerable{ValueTuple{IConvertible, string, string}})"/>
        /// <param name="planRun">The plan to add parameters from.</param>
        public void AddPlanParameters(TestPlanRun planRun)
        {
            AddPlanParameters(planRun.Parameters.Select(p => (p.Value, p.Group, p.Name)));
        }

        /// <summary>
        /// Builds the schema of this schemabuilder, and returns the resulting schema.
        /// </summary>
        /// <returns>The schema containing all fields added to this schemabuilder.</returns>
        public Schema ToSchema()
        {
            return new Schema(_fields);
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
