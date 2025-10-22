using System;
using System.Diagnostics;
using System.IO;

namespace OpenTap.Plugins.Parquet;

public class MemoryLogger : ResultListener
{
    public long MaxUsage = 0;
    private Stopwatch _stopwatch = new Stopwatch();

    public MemoryLogger()
    {
        Name = "MemoryLogger";
    }
    
    public override void OnResultPublished(Guid stepRunId, ResultTable result)
    {
        UpdateMaxUsage();
        base.OnResultPublished(stepRunId, result);
        UpdateMaxUsage();
    }

    public override void OnTestStepRunCompleted(TestStepRun stepRun)
    {
        UpdateMaxUsage();
        base.OnTestStepRunCompleted(stepRun);
        UpdateMaxUsage();
    }

    public override void OnTestPlanRunCompleted(TestPlanRun planRun, Stream logStream)
    {
        UpdateMaxUsage();
        base.OnTestPlanRunCompleted(planRun, logStream);
        UpdateMaxUsage();
    }

    public override void OnTestPlanRunStart(TestPlanRun planRun)
    {
        UpdateMaxUsage();
        base.OnTestPlanRunStart(planRun);
        UpdateMaxUsage();
    }

    public override void OnTestStepRunStart(TestStepRun stepRun)
    {
        UpdateMaxUsage();
        base.OnTestStepRunStart(stepRun);
        UpdateMaxUsage();
    }

    public override void Open()
    {
        _stopwatch.Start();
        UpdateMaxUsage();
        base.Open();
        UpdateMaxUsage();
    }

    public override void Close()
    {
        UpdateMaxUsage();
        base.Close();
        UpdateMaxUsage();
        _stopwatch.Stop();
        Log.Info($"Total time: {_stopwatch.ElapsedMilliseconds}ms");
        Log.Info($"Max memory usage: {MaxUsage}b");
        File.AppendAllText("Results.csv", $"{string.Join(" ", Environment.GetCommandLineArgs())}, {_stopwatch.ElapsedMilliseconds}ms, {MaxUsage}b\n");
    }

    public void UpdateMaxUsage()
    {
        MaxUsage = Math.Max(MaxUsage, GC.GetTotalMemory(false));
    }
}