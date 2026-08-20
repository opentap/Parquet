using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenTap.Plugins.Parquet.Extensions;

internal static class ResultTableExtensions
{
    /// <summary>
    /// Turns the columns of a <see cref="ResultTable"/> into a dictionary keyed by column name.
    /// Columns that share the same name are de-duplicated by suffixing the name with "/1", "/2", ...
    /// so that they can be written as separate parquet columns. The returned <paramref name="nameMapping"/>
    /// maps each de-duplicated (unique) key back to the original display name so the caller can register
    /// the mapping on the file.
    /// </summary>
    internal static Dictionary<string, Array> GetResults(this ResultTable table, out Dictionary<string, string> nameMapping)
    {
        Dictionary<string, Array> results = new();
        nameMapping = new();
        Dictionary<string, int> seen = new();
        foreach (ResultColumn column in table.Columns)
        {
            string name = column.Name;
            if (seen.TryGetValue(name, out int count))
            {
                count += 1;
                seen[name] = count;
                string uniqueName = name + "/" + count;
                results[uniqueName] = column.Data;
                nameMapping[uniqueName] = name;
            }
            else
            {
                seen[name] = 0;
                results[name] = column.Data;
            }
        }

        return results;
    }
}