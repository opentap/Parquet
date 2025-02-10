using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenTap.Plugins.Parquet.Extensions;

internal static class TestRunExtensions
{

    internal static Dictionary<string, IConvertible> GetParameters(this TestRun run)
    {
        return run.Parameters
            .ToDictionary(CreateName, p => p.Value);

        static string CreateName(ResultParameter parameter)
        {
            return string.IsNullOrWhiteSpace(parameter.Group)
                ? parameter.Name
                : string.Join("/", parameter.Group, parameter.Name);
        }
    }
}