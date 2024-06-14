using System;
using System.Collections.Generic;
using System.Linq;
using OpenTap;
using OpenTap.Plugins.Parquet;

namespace Parquet.Extensions
{
    internal static class TestRunExtensions
    {

        internal static Dictionary<string, IConvertible> GetParameters(this TestRun run)
        {
            return run.Parameters
                            .ToDictionary(p => p.Group + "/" + p.Name, p => p.Value);
        }
    }
}