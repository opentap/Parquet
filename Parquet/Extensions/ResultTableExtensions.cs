using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenTap.Plugins.Parquet.Extensions;

internal static class ResultTableExtensions
{
    /// <summary>
    /// Turns the columns of a <see cref="ResultTable"/> into a lookup keyed by column name.
    /// Columns that share the same name are grouped together; disambiguation and name mapping
    /// are handled by <see cref="OpenTap.Plugins.Parquet.Core.ParquetFile"/> when the row is written.
    /// </summary>
    internal static ILookup<string, Array> GetResults(this ResultTable table)
    {
        return table.Columns.ToLookup(c => c.Name, c => c.Data);
    }
}