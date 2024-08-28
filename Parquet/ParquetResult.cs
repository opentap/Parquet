using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Extensions;
using System.Linq;

namespace OpenTap.Plugins.Parquet;

public sealed class ParquetResult : IDisposable
{
    private readonly Options? _options;
    private readonly List<ParquetFragment> _fragments;

    public ParquetResult(string path, Options? options = null)
    {
        _options = options;
        Path = path;
        _fragments = [];
        AddFragment();
    }
    
    public string Path { get; }
    
    private ParquetFragment CurrentFragment => _fragments[_fragments.Count - 1];

    private void AddFragment()
    {
        if (_fragments.Count > 0)
        {
            CurrentFragment.Dispose();
        }

        _fragments.Add(new($"{Path}-{_fragments.Count}.tmp", _options ?? new Options()));
    }

    public void AddResultRow(TestStepRun run, ResultTable table)
    {
        AddResultRow(table.Name, run.Id.ToString(), run.Parent.ToString(), run.TestStepId.ToString(), run.GetParameters(), table.GetResults());
    }
    
    public void AddResultRow(string resultName, string runId, string parentId, string stepId, Dictionary<string, IConvertible> parameters, Dictionary<string, Array> results)
    {
        parameters = parameters.ToDictionary(kvp => "Step/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("ResultName", resultName);
        parameters.Add("Guid", runId);
        parameters.Add("ResultName", parentId);
        parameters.Add("ResultName", stepId);
        if (!CurrentFragment.AddRows(parameters, results))
        {
            CurrentFragment.Dispose();
            AddFragment();
        }
    }
    
    public void AddStepRow(TestStepRun run)
    {
        AddStepRow(run.Id.ToString(), run.Parent.ToString(), run.TestStepId.ToString(), run.GetParameters());
    }

    public void AddStepRow(string runId, string parentId, string stepId, Dictionary<string, IConvertible> parameters)
    {
        parameters = parameters.ToDictionary(kvp => "Step/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("Guid", runId);
        parameters.Add("ResultName", parentId);
        parameters.Add("ResultName", stepId);
        if (!CurrentFragment.AddRows(parameters, new Dictionary<string, Array>()))
        {
            CurrentFragment.Dispose();
            AddFragment();
        }
    }

    public void AddPlanRow(TestPlanRun plan)
    {
        AddPlanRow(plan.Id.ToString(), plan.GetParameters());
    }

    public void AddPlanRow(string planId, Dictionary<string, IConvertible> parameters)
    {
        parameters = parameters.ToDictionary(kvp => "Plan/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("Guid", planId);
        if (!CurrentFragment.AddRows(parameters, new Dictionary<string, Array>()))
        {
            CurrentFragment.Dispose();
            AddFragment();
        }
    }
    
    public void Dispose()
    {
        foreach (ParquetFragment fragment in _fragments.TakeWhile(f => f != CurrentFragment))
        {
            CurrentFragment.MergeWith(fragment);
        }
        CurrentFragment.Dispose();
        if (File.Exists(Path))
        {
            File.Delete(Path);
        }
        File.Move(CurrentFragment.Path, Path);
    }
}