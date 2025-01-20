using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenTap.Plugins.Parquet.Extensions
{
    internal static class ResultTableExtensions
    {

        internal static Dictionary<string, Array> GetResults(this ResultTable table)
        {
            return table.Columns.ToDictionary(c => c.Name, c => c.Data);
        }
    }
}