using Parquet;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using OpenTap.Plugins.Parquet.Extensions;

namespace OpenTap.Plugins.Parquet;

[Display("Parquet", "Save results in a Parquet file", "Database")]
public sealed class ParquetResultListener : ResultListener
{
    internal new static TraceSource Log { get; } = OpenTap.Log.CreateSource("Parquet");

    private readonly Dictionary<Guid, TestPlanRun> _guidToPlanRuns = new();
    private readonly Dictionary<Guid, TestStepRun> _guidToStepRuns = new();
    private readonly HashSet<Guid> _hasWrittenParameters = [];
    private readonly Dictionary<string, ParquetResult> _results = new();

    [Display("File path", "The file path of the parquet file(s). Can use <ResultType> to have one file per result type.", Order: 0)]
    [FilePath(FilePathAttribute.BehaviorChoice.Save)]
    public MacroString FilePath { get; set; } = new() { Text = "Results/<TestPlanName>.<Date>/<ResultType>.parquet"};

    [Display("Delete on publish", "If true the files will be removed when published as artifacts.", Order: 1)]
    public bool DeleteOnPublish { get; set; } = false;

    [Display("Method", "The compression method to use when writing the file.", "Compression", 2, false)]
    public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

    [Display("Level", "The compression level to use when writing the file.", "Compression", 3, false)]
    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;
        
    [Display("Rowgroup size", "The ideal size of each row group measured in rows. Each Row Group size should roughly fit with one memory page for ideal performance.", "Encoding", Order:4, Collapsed: true)]
    public int RowGroupSize { get; set; } = 10_000;

    [Display("Use dictionary encoding",
        "Whether to use dictionary encoding for string columns. Other column types are not supported.",
        "Encoding", 5, true)]
    public bool UseDictionaryEncoding { get; set; } = true;

    [Display("Dictionary encoding threshold",
        "String dictionary uniqueness threshold, which is a value from 0 (no unique values) to 1 (all values are unique) indicating when string dictionary encoding is applied. Uniqueness factor needs to be less or equal than this threshold.",
        "Encoding", 6, true)]
    [EnabledIf("UseDictionaryEncoding", true)]
    public double DictionaryEncodingThreshold { get; set; }= 0.8;

    [Display("Use delta binary packed encoding",
        "When set, the default encoding for INT32 and INT64 is 'delta binary packed', otherwise it's reverted to 'plain'. You should only set this to true if your readers understand it.",
        "Encoding", 7, true)]
    public bool UseDeltaBinaryPackedEncoding { get; set; } = true;
        
    public ParquetResultListener()
    {
        Name = "Parquet";
    }

    public override void OnTestPlanRunStart(TestPlanRun planRun)
    {
        base.OnTestPlanRunStart(planRun);

        _guidToPlanRuns[planRun.Id] = planRun;
        GetFile(planRun).AddPlanRow(planRun);
    }

    public override void OnTestPlanRunCompleted(TestPlanRun planRun, Stream logStream)
    {
        base.OnTestPlanRunCompleted(planRun, logStream);
            
        foreach (ParquetResult parquetResult in _results.Values)
        {
            parquetResult.Dispose();
            planRun.PublishArtifactAsync(parquetResult.Path);
            if (DeleteOnPublish)
            {
                File.Delete(parquetResult.Path);
            }
        }
        _results.Clear();

        _guidToPlanRuns.Clear();
        _guidToStepRuns.Clear();
    }

    public override void OnTestStepRunStart(TestStepRun stepRun)
    {
        _guidToStepRuns[stepRun.Id] = stepRun;
        base.OnTestStepRunStart(stepRun);
    }

    public override void OnTestStepRunCompleted(TestStepRun stepRun)
    {
        base.OnTestStepRunCompleted(stepRun);

        if (!_hasWrittenParameters.Contains(stepRun.Id))
        {
            TestPlanRun planRun = GetPlanRun(stepRun);
                
            GetFile(planRun).AddStepRow(stepRun);
            _hasWrittenParameters.Add(stepRun.Id);
        }
    }

    public override void OnResultPublished(Guid stepRunId, ResultTable result)
    {
        base.OnResultPublished(stepRunId, result);
        TestStepRun stepRun = _guidToStepRuns[stepRunId];
        TestPlanRun planRun = GetPlanRun(stepRun);

        GetFile(planRun, result.Name).AddResultRow(stepRun, result);

        _hasWrittenParameters.Add(stepRunId);
    }

    private ParquetResult GetFile(TestPlanRun planRun, string resultType = "Plan")
    {
        string path = FilePath.Expand(planRun, planRun.StartTime, "./", new Dictionary<string, object>
        {
            { "ResultType", resultType }
        });

        if (!_results.TryGetValue(path, out ParquetResult? result))
        {
            result = new ParquetResult(path, new Options()
            {
                RowGroupSize = RowGroupSize,
                CompressionMethod = CompressionMethod,
                CompressionLevel = CompressionLevel,
                ParquetOptions =
                {
                    UseDictionaryEncoding = UseDictionaryEncoding,
                    DictionaryEncodingThreshold = DictionaryEncodingThreshold,
                    UseDeltaBinaryPackedEncoding = UseDeltaBinaryPackedEncoding,
                },
            });
            _results.Add(path, result);
        }

        return result;
    }

    private TestPlanRun GetPlanRun(TestStepRun run)
    {
        TestPlanRun? planRun;
        while (!_guidToPlanRuns.TryGetValue(run.Parent, out planRun))
        {
            run = _guidToStepRuns[run.Parent];
        }
        return planRun;
    }
}