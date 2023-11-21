using Parquet.Data;
using System;
using System.Collections;
using System.Linq;

namespace Parquet.Extensions
{
    internal static class ArrayListExtensions
    {

        internal static Array ConvertList(this ArrayList list, DataType type)
        {
            switch (type)
            {
                case DataType.Boolean:
                    return list.ToArray(typeof(bool?));
                case DataType.Byte:
                    return list.ToArray(typeof(byte?));
                case DataType.SignedByte:
                    return list.ToArray(typeof(sbyte?));
                case DataType.UnsignedByte:
                    return list.ToArray(typeof(byte?));
                case DataType.Short:
                    return list.ToArray(typeof(short?));
                case DataType.UnsignedShort:
                    return list.ToArray(typeof(ushort?));
                case DataType.Int16:
                    return list.ToArray(typeof(short?));
                case DataType.UnsignedInt16:
                    return list.ToArray(typeof(ushort?));
                case DataType.Int32:
                    return list.ToArray(typeof(int?));
                case DataType.UnsignedInt32:
                    return list.ToArray(typeof(uint?));
                case DataType.Int64:
                    return list.ToArray(typeof(long?));
                case DataType.UnsignedInt64:
                    return list.ToArray(typeof(ulong?));
                case DataType.String:
                    return list.Cast<object?>().Select(o => o?.ToString()).ToArray();
                case DataType.Float:
                    return list.ToArray(typeof(float?));
                case DataType.Double:
                    return list.ToArray(typeof(double?));
                case DataType.Decimal:
                    return list.ToArray(typeof(decimal?));
                case DataType.TimeSpan:
                    return list.ToArray(typeof(TimeSpan?));
                case DataType.DateTimeOffset:
                    return list.OfType<IConvertible?>().Select<IConvertible?, DateTimeOffset?>(dt => dt is null ? null : new DateTimeOffset((DateTime)dt)).ToArray();
                case DataType.Unspecified:
                case DataType.Int96:
                case DataType.ByteArray:
                case DataType.Interval:
                default:
                    throw new Exception($"Could not create column of type {type}");
            }
        }
    }
}