using Parquet.Data;
using System;
using System.Collections.Generic;

namespace ParquetResultListener
{
    internal enum ColumnType
    {
        Plan,
        Step,
        Result,
        Guid,
    }

    internal sealed class SchemaBuilder
    {
        private const string Plan = "Plan";
        private const string Step = "Step";
        private const string Result = "Result";
        private const string Guid = "Guid";

        private readonly List<DataField> _fields = new List<DataField>();

        internal SchemaBuilder()
        {
        }

        private void Append(Type type, params string[] path)
        {
            // Enums not directly supported.
            if (type.IsEnum)
            {
                type = typeof(string);
            }

            _fields.Add(new DataField(string.Join("/", path), type));
        }
        private void Append<T>(params string[] path)
        {
            Append(typeof(T), path);
        }
        internal bool TryAppendResult(Array data, string name)
        {
            if (data is not null && data.Length > 0)
            {
                Append(data.GetValue(0).GetType(), Result, name);
                return true;
            }
            return false;
        }
        internal void AppendStep(object value, string name)
        {
            Append(value.GetType(), Step, name);
        }
        internal void AppendGuid(string name)
        {
            Append<string>(Guid, name);
        }

        internal Schema Build()
        {
            return new Schema(_fields);
        }

        internal static ColumnType GetColumnType(DataField field, out string name)
        {
            string[] pathParts = field.Name.Split('/');
            name = pathParts[1];
            switch (pathParts[0])
            {
                case Plan:
                    return ColumnType.Plan;
                case Step:
                    return ColumnType.Step;
                case Result:
                    return ColumnType.Result;
                case Guid:
                    return ColumnType.Guid;
                default:
                    throw new ArgumentException("Field was not created by the schema builder.", nameof(field));
            }
        }
    }
}
