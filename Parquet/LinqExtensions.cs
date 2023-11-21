using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet
{
    internal static class LinqExtensions
    {
        internal static HashSet<T> ToHashSet<T>(this IEnumerable<T> enumerable)
        {
            HashSet<T> hashSet = new HashSet<T>();
            foreach (T item in enumerable)
            {
                hashSet.Add(item);
            }
            return hashSet;
        }
    }
}
