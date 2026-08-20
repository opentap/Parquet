using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace OpenTap.Plugins.Parquet.Core;

/// A parquet result is a single parquet file.
/// It will write and manage multiple fragments and make sure they are being managed properly to ensure schema compliance.
public sealed class ParquetFile : IDisposable
{
    private const string ResultPrefix = "Result/";

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
        parameters = parameters.ToDictionary(kvp => "Step/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("ResultName", resultName);
        parameters.Add("Guid", runId);
        parameters.Add("Parent", parentId);
        parameters.Add("StepId", stepId);
        results = results.ToDictionary(kvp => ResultPrefix + kvp.Key, kvp => kvp.Value);
        while (!CurrentFragment.AddRows(parameters, results))
        {
            AddFragment();
        }
    }

    /// <summary>
    /// Add a result row to the file where several result arrays may share the same name.
    /// Columns sharing a name are written as separate parquet columns (the first keeps the name,
    /// subsequent ones are suffixed with "/1", "/2", ...) and a name mapping is registered so that
    /// every one of them maps back to the shared display name in the file's metadata.
    /// </summary>
    /// <param name="resultName">The name of the results.</param>
    /// <param name="runId">The id of the step run that created the results.</param>
    /// <param name="parentId">The id of the parent to the step run that created the results.</param>
    /// <param name="stepId">The id of the test step within the test plan.</param>
    /// <param name="parameters">A dictionary containing the parameters of the step, to look them up by their name.</param>
    /// <param name="results">A lookup of result arrays grouped by column name. Multiple arrays may share a name.</param>
    public void AddResultRow(string resultName, string runId, string parentId, string stepId, Dictionary<string, IConvertible> parameters, ILookup<string, Array> results)
    {
        Dictionary<string, Array> flattened = new();
        List<KeyValuePair<string, string>> mappings = new();
        foreach (IGrouping<string, Array> group in results)
        {
            int index = 0;
            foreach (Array data in group)
            {
                string key = index == 0 ? group.Key : $"{group.Key}/{index}";
                flattened[key] = data;
                if (index > 0)
                {
                    mappings.Add(new KeyValuePair<string, string>(ResultPrefix + key, ResultPrefix + group.Key));
                }
                index += 1;
            }
        }

        AddResultRow(resultName, runId, parentId, stepId, parameters, flattened);
        foreach (KeyValuePair<string, string> mapping in mappings)
        {
            AddNameMapping(mapping.Key, mapping.Value);
        }
    }

    /// <summary>
    /// Register a mapping from a column's unique name to the display name it should map back to.
    /// This lets two separately written columns (e.g. "Result/a" and "Result/a/1") both map to the
    /// same display name (e.g. "Result/a") in the file's name mapping metadata.
    /// The caller is responsible for de-duplicating the column names before writing them and for
    /// supplying the corresponding mapping here.
    /// </summary>
    /// <param name="uniqueName">The unique (physical) name of the column that was written.</param>
    /// <param name="name">The display name the column should map to.</param>
    public void AddNameMapping(string uniqueName, string name)
    {
        CurrentFragment.AddNameMapping(uniqueName, name);
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
        parameters = parameters.ToDictionary(kvp => "Step/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("Guid", runId);
        parameters.Add("Parent", parentId);
        parameters.Add("StepId", stepId);
        while (!CurrentFragment.AddRows(parameters, new Dictionary<string, Array>()))
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
        parameters = parameters.ToDictionary(kvp => "Plan/" + kvp.Key, kvp => kvp.Value);
        parameters.Add("Guid", planId);
        while (!CurrentFragment.AddRows(parameters, new Dictionary<string, Array>()))
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