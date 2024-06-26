using System;
using System.Collections.Generic;
using Parquet.Extensions;
using Parquet;
using System.IO.Compression;
using System.Linq;

namespace OpenTap.Plugins.Parquet;

public sealed class ParquetResult : IDisposable
{

    private readonly List<ParquetFragment> _oldFragments;
    private ParquetFragment _currentFragment;

    public sealed class Options
    {
        public int RowGroupSize { get; set; } = 10_000;
        public CompressionMethod CompressionMethod { get; set; }= CompressionMethod.Snappy;
        public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;
        public ParquetOptions ParquetOptions { get; set; } = new ParquetOptions();
    }

    public ParquetResult(string path, Options? options = null)
    {
        Path = path;
        _oldFragments = [];
        _currentFragment = new(path, options ?? new Options(){ParquetOptions = { UseDeltaBinaryPackedEncoding = false }});
    }
    
    public string Path { get; }
    
    private ParquetFragment CurrentFragment {
        get => _currentFragment;
        set
        {
            _oldFragments.Add(_currentFragment);
            _currentFragment = value;
        }
    }

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
            CurrentFragment = new ParquetFragment(CurrentFragment);
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
            CurrentFragment = new ParquetFragment(CurrentFragment);
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
            CurrentFragment = new ParquetFragment(CurrentFragment);
        }
    }

    public void Dispose()
    {
        CurrentFragment.WriteCache();
        CurrentFragment.Dispose(_oldFragments);
    }
}
