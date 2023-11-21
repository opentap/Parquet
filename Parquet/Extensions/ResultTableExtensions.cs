using System;
using System.Collections.Generic;
using System.Linq;
using OpenTap;

namespace Parquet.Extensions
{
    internal static class ResultTableExtensions
    {

        internal static Dictionary<string, Array> GetResults(this ResultTable table)
        {
            return table.Columns.ToDictionary(c => c.Name, c => c.Data);
        }
    }
}