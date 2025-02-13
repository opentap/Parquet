using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace OpenTap.Plugins.Parquet.Core;

public sealed class ParquetResult : IDisposable
{
    private readonly Options? _options;
    private readonly List<Fragment> _fragments;

    public ParquetResult(string path, Options? options = null)
    {
        _options = options;
        Path = path;
        _fragments = [];
        AddFragment();
    }
    
    public string Path { get; }

    internal int FragmentCount => _fragments.Count;
    
    private Fragment CurrentFragment => _fragments[_fragments.Count - 1];

    private void AddFragment()
    {
        string path = $"{Path}-{_fragments.Count}.tmp";
        if (FragmentCount == 0)
        {
            _fragments.Add(new($"{Path}-{_fragments.Count}.tmp", _options ?? new Options()));
            return;
        }
        CurrentFragment.Dispose();
        _fragments.Add(new (CurrentFragment, path));
    }
    
    public void AddResultRow(string resultName, string runId, string parentId, string stepId, Dictionary<string, IConvertible> parameters, Dictionary<string, Array> results)
    {
        parameters = parameters.ToDictionary(kvp => "Step/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("ResultName", resultName);
        parameters.Add("Guid", runId);
        parameters.Add("Parent", parentId);
        parameters.Add("StepId", stepId);
        results = results.ToDictionary(kvp => "Result/" + kvp.Key, kvp => kvp.Value);
        while (!CurrentFragment.AddRows(parameters, results))
        {
            AddFragment();
        }
    }
    

    public void AddStepRow(string runId, string parentId, string stepId, Dictionary<string, IConvertible> parameters)
    {
        parameters = parameters.ToDictionary(kvp => "Step/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("Guid", runId);
        parameters.Add("Parent", parentId);
        parameters.Add("StepId", stepId);
        while (!CurrentFragment.AddRows(parameters, new Dictionary<string, Array>()))
        {
            AddFragment();
        }
    }

    public void AddPlanRow(string planId, Dictionary<string, IConvertible> parameters)
    {
        parameters = parameters.ToDictionary(kvp => "Plan/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("Guid", planId);
        while (!CurrentFragment.AddRows(parameters, new Dictionary<string, Array>()))
        {
            AddFragment();
        }
    }
    
    public void Dispose()
    {
        if (!CurrentFragment.CanEdit)
        {
            AddFragment();
        }
        
        foreach (Fragment fragment in _fragments.TakeWhile(f => f != CurrentFragment))
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