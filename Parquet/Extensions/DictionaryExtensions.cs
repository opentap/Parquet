using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Extensions
{
    internal static class DictionaryExtensions
    {
        internal static TValue? GetValueOrDefault<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey value)
        {
            return dict.TryGetValue(value, out TValue? valueOrNull) ? valueOrNull : default;
        }
    }
}
