using System;

namespace OpenTap.Plugins.Parquet.Core.Extensions;

internal static class TypeExtensions
{
    // https://stackoverflow.com/questions/108104/how-do-i-convert-a-system-type-to-its-nullable-version
    internal static Type AsNullable(this Type type)
    {
        // Use Nullable.GetUnderlyingType() to remove the Nullable<T> wrapper if type is already nullable.
        type = GetNullableUnderlyingType(type);
        if (type.IsValueType)
            return typeof(Nullable<>).MakeGenericType(type);
        else
            return type;
    }

    internal static Type GetNullableUnderlyingType(this Type type)
    {
        return Nullable.GetUnderlyingType(type) ?? type;
    }
}