using System;
using System.Collections.Generic;
using System.Linq;
using OpenTap;

namespace Parquet.Extensions
{
    /// <summary>
    /// Extensions to result tables.
    /// </summary>
    public static class ResultTableExtensions
    {

        /// <summary>
        /// Get results out of a ResultTable.
        /// </summary>
        /// <param name="table">The result table to get parameters from.</param>
        /// <returns>A dictionary containing all columns of the result table.</returns>
        public static Dictionary<string, Array> GetResults(this ResultTable table)
        {
            return table.Columns.ToDictionary(c => c.Name, c => c.Data);
        }
    }
}