using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenTap.Plugins.Parquet.Core.Extensions;

internal static class LookupExtensions
{
    internal static Dictionary<TKey, List<TValue>> ToDictLookup<TIn, TKey, TValue>(this IEnumerable<TIn> lookup, Func<TIn, TKey> keySelector, Func<TIn, IEnumerable<TValue>> valueSelector) where TKey : notnull
    {
        return lookup.ToDictionary(keySelector, i => valueSelector(i).ToList());
    }

    internal static void Add<TKey, TValue>(this Dictionary<TKey, List<TValue>> dict, TKey key, TValue value) where TKey : notnull
    {
        if (!dict.TryGetValue(key, out List<TValue>? list))
        {
            list = new List<TValue>();
            dict[key] = list;
        }
        list.Add(value);
    }
}