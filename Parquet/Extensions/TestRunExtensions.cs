using System;
using System.Collections.Generic;
using System.Linq;
using OpenTap;
using OpenTap.Plugins.Parquet;

namespace Parquet.Extensions
{
    /// <summary>
    /// Extensions to test run classes.
    /// </summary>
    public static class TestRunExtensions
    {

        /// <summary>
        /// Get parameters in a dictionary for a <see cref="ParquetFile"/>.
        /// </summary>
        /// <param name="run">The run to get the parameters from.</param>
        /// <returns>A dictionary of parameter names and values.</returns>
        public static Dictionary<string, IConvertible> GetParameters(this TestRun run)
        {
            return run.Parameters
                            .ToDictionary(p => SchemaBuilder.GetValidParquetName(p.Group, p.Name), p => p.Value);
        }
    }
}