using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OpenTap.Plugins.Parquet.Core.Extensions;

namespace OpenTap.Plugins.Parquet.Core;

/// A parquet result is a single parquet file.
/// It will write and manage multiple fragments and make sure they are being managed properly to ensure schema compliance.
public sealed class ParquetFile : IDisposable
{
    private readonly Options? _options;
    private readonly List<Fragment> _fragments;

    /// <summary>
    /// Create a new parquet result.
    /// </summary>
    /// <param name="path">The final path to the file once it is done being written.</param>
    /// <param name="options">Options for the underlying parquet writer.</param>
    public ParquetFile(string path, Options? options = null)
    {
        _options = options;
        Path = path;
        _fragments = [];
        AddFragment();
    }
    
    /// <summary>
    /// Gets the path of the parquet file.
    /// </summary>
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
    
    /// <summary>
    /// Add a result row to the file.
    /// </summary>
    /// <param name="resultName">The name of the results.</param>
    /// <param name="runId">The id of the step run that created the results.</param>
    /// <param name="parentId">The id of the parent to the step run that created the results.</param>
    /// <param name="stepId">The id of the test step within the test plan.</param>
    /// <param name="parameters">A dictionary containing the parameters of the step, to look them up by their name.</param>
    /// <param name="results">A dictionary containing the results of the step, to look them up by their column names.</param>
    public void AddResultRow(string resultName, string runId, string parentId, string stepId, Dictionary<string, IConvertible> parameters, Dictionary<string, Array> results)
    {
        AddResultRow(resultName, runId, parentId, stepId, parameters.ToLookup(kvp => kvp.Key, kvp => kvp.Value), results.ToLookup(kvp => kvp.Key, kvp => kvp.Value));
    }

    public void AddResultRow(string resultName, string runId, string parentId, string stepId,
        ILookup<string, IConvertible> parameters, ILookup<string, Array> results)
    {
        var parametersDict = parameters.ToDictLookup(g => "Step/" + g.Key, g => g);
        parametersDict.Add("ResultName", resultName);
        parametersDict.Add("Guid", runId);
        parametersDict.Add("Parent", parentId);
        parametersDict.Add("StepId", stepId);
        var resultsDict = results.ToDictLookup(g => "Result/" + g.Key, g => g);
        while (!CurrentFragment.AddRows(parametersDict, resultsDict))
        {
            AddFragment();
        }
    }
    
    /// <summary>
    /// Add a step row without results to the file.
    /// </summary>
    /// <param name="runId">The id of the step run.</param>
    /// <param name="parentId">The id of the parent to the step run.</param>
    /// <param name="stepId">The id of the test step within the test plan.</param>
    /// <param name="parameters">A dictionary containing the parameters of the step, to look them up by their name.</param>
    public void AddStepRow(string runId, string parentId, string stepId, Dictionary<string, IConvertible> parameters)
    {
        AddStepRow(runId, parentId, stepId, parameters.ToLookup(kvp => kvp.Key, kvp => kvp.Value));
    }

    public void AddStepRow(string runId, string parentId, string stepId, ILookup<string, IConvertible> parameters)
    {
        var parametersDict = parameters.ToDictLookup(g => "Step/" + g.Key, g => g);
        parametersDict.Add("Guid", runId);
        parametersDict.Add("Parent", parentId);
        parametersDict.Add("StepId", stepId);
        while (!CurrentFragment.AddRows(parametersDict, new Dictionary<string, List<Array>>()))
        {
            AddFragment();
        }
    }

    /// <summary>
    /// Add a plan row to the file.
    /// </summary>
    /// <param name="planId">The id of the plan run.</param>
    /// <param name="parameters">A dictionary containing the parameters of the step, to look them up by their name.</param>
    public void AddPlanRow(string planId, Dictionary<string, IConvertible> parameters)
    {
        AddPlanRow(planId, parameters.ToLookup(kvp => kvp.Key, kvp => kvp.Value));
    }

    public void AddPlanRow(string planId, ILookup<string, IConvertible> parameters)
    {
        var parametersDict = parameters.ToDictLookup(g => "Plan/" + g.Key, g => g);
        parametersDict.Add("Guid", planId);
        while (!CurrentFragment.AddRows(parametersDict, new Dictionary<string, List<Array>>()))
        {
            AddFragment();
        }
    }
    
    public void Dispose()
    {
        if (!CurrentFragment.CanEdit && _fragments.Count > 1)
        {
            AddFragment();
        }
        
        foreach (Fragment fragment in _fragments.TakeWhile(f => f != CurrentFragment))
        {
            CurrentFragment.MergeWith(fragment);
            File.Delete(fragment.Path);
        }
        CurrentFragment.Dispose();
        
        if (File.Exists(Path))
        {
            File.Delete(Path);
        }
        File.Move(CurrentFragment.Path, Path);
    }
}