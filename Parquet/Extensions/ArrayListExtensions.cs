using Parquet.Data;
using System;
using System.Collections;
using System.Linq;

namespace Parquet.Extensions
{
    internal static class ArrayListExtensions
    {

        internal static Array ConvertList(this ArrayList list, Type type)
        {
            if (type == typeof(string))
            {
                return list.Cast<object?>().Select(o => o?.ToString()).ToArray();
            }

            if (type == typeof(DateTimeOffset))
            {
                return list.OfType<IConvertible?>().Select<IConvertible?, DateTimeOffset?>(dt => dt is null ? null : new DateTimeOffset((DateTime)dt)).ToArray();
            }

            return list.ToArray(type.AsNullable());
        }
    }
}