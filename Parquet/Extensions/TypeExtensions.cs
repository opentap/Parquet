using System;

namespace Parquet.Extensions
{
    internal static class TypeExtensions
    {
        // https://stackoverflow.com/questions/108104/how-do-i-convert-a-system-type-to-its-nullable-version
        internal static Type GetNullableType(this Type type)
        {
            // Use Nullable.GetUnderlyingType() to remove the Nullable<T> wrapper if type is already nullable.
            type = Nullable.GetUnderlyingType(type) ?? type; // avoid type becoming null
            if (type.IsValueType)
                return typeof(Nullable<>).MakeGenericType(type);
            else
                return type;
        }
    }
}
