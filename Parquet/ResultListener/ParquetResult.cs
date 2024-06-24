using OpenTap;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Parquet.Extensions;

namespace OpenTap.Plugins.Parquet;

public sealed class ParquetResult : IDisposable
{
    private readonly List<ParquetFragment> _fragments;

    public ParquetResult(string path)
    {
        _fragments = new List<ParquetFragment>()
        {
            new ParquetFragment(path),
        };
    }

    private ParquetFragment CurrentFragment => _fragments[_fragments.Count - 1];

    public void AddResultRow(TestStepRun run, ResultTable table)
    {
        AddResultRow(table.Name, run.Id, run.Parent, run.TestStepId, run.GetParameters(), table.GetResults());
    }
    
    public void AddResultRow(string resultName, Guid runId, Guid parentId, Guid stepId, Dictionary<string, IConvertible> parameters, Dictionary<string, Array> results)
    {
        if (!CurrentFragment.AddRows(
                resultName,
                runId,
                parentId,
                stepId,
                null,
                parameters,
                results))
        {
            CurrentFragment.Dispose();
            _fragments.Add( new ParquetFragment(CurrentFragment));
        }
    }
    
    public void AddStepRow(TestStepRun run)
    {
        AddStepRow(run.Id, run.Parent, run.TestStepId, run.GetParameters());
    }

    public void AddStepRow(Guid runId, Guid parentId, Guid stepId, Dictionary<string, IConvertible> parameters)
    {
        if (!CurrentFragment.AddRows(
                null,
                runId,
                parentId,
                stepId,
                null,
                parameters,
                null))
        {
            CurrentFragment.Dispose();
            _fragments.Add( new ParquetFragment(CurrentFragment));
        }
    }

    public void AddPlanRow(TestPlanRun plan)
    {
        AddPlanRow(plan.Id, plan.GetParameters());
    }

    public void AddPlanRow(Guid planId, Dictionary<string, IConvertible> parameters)
    {
        if (!CurrentFragment.AddRows(
                null,
                planId,
                null,
                null,
                parameters,
                null,
                null))
        {
            CurrentFragment.Dispose();
            _fragments.Add( new ParquetFragment(CurrentFragment));
        }
    }

    public void Dispose()
    {
        CurrentFragment.WriteCache();
        CurrentFragment.Dispose();
    }
}
