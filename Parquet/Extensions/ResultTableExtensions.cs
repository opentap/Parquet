using System;
using System.Linq;

namespace OpenTap.Plugins.Parquet.Extensions;

internal static class ResultTableExtensions
{

    internal static ILookup<string, Array> GetResults(this ResultTable table)
    {
        return table.Columns.ToLookup(c => c.Name, c => c.Data);
    }
}