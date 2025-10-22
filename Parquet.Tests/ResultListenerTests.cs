using NUnit.Framework;
using OpenTap;
using OpenTap.Plugins.BasicSteps;
using OpenTap.Plugins.Parquet;

namespace Parquet.Tests;

public class ResultStep(string resultName, ResultColumn[] columns, ResultParameter[] parameters)
    : TestStep
{
    public override void Run()
    {
        foreach (ResultParameter resultParameter in parameters)
        {
            Results.AddParameter(resultParameter);
        }

        Results.Publish(new ResultTable(resultName, columns));
    }
}

internal class ResultListenerTests
{
    [Test]
    public void OutputParquetFilesTest()
    {
        string path = $"Tests/{nameof(ResultListenerTests)}/{nameof(OutputParquetFilesTest)}.parquet";
        
        TestPlan plan = new TestPlan();
        ParquetResultListener resultListener = new ParquetResultListener()
        {
            FilePath = { Text = path },
        };
        var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
        result.WaitForResults();
        var artifacts = result.Artifacts.ToList();
        Assert.That(artifacts.Count, Is.EqualTo(1));
        Assert.That(artifacts[0], Is.EqualTo($"{nameof(OutputParquetFilesTest)}.parquet"));

        Assert.That(System.IO.File.Exists(path), Is.True);
    }

    [Test]
    public async Task OutputResultsTest()
    {
        string path = $"Tests/{nameof(ResultListenerTests)}/{nameof(OutputResultsTest)}.parquet";
        
        TestPlan plan = new TestPlan();
        ResultStep step = new ResultStep("Test",
            [new ResultColumn("Column1", Enumerable.Range(0, 50).ToArray())],
            []
        );
        plan.ChildTestSteps.Add(step);
        ParquetResultListener resultListener = new ParquetResultListener()
        {
            FilePath = { Text = path },
        };
        var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
        result.WaitForResults();

        Assert.That(System.IO.File.Exists(path), Is.True);
        
        var reader = await Reader.CreateAsync(path);

        Assert.That(reader.Count, Is.EqualTo(51));
        for (int i = 0; i < 50; i++)
        {
            Assert.That(reader.ReadCell(i+1, "StepId"), Is.EqualTo(step.Id.ToString()));
            Assert.That(reader.ReadCell(i+1, "Result/Column1"), Is.EqualTo(i));
        }
    }

    [Test]
    public async Task OutputResultsAndParametersTest()
    {
        string path = $"Tests/{nameof(ResultListenerTests)}/{nameof(OutputResultsAndParametersTest)}.parquet";
        
        TestPlan plan = new TestPlan();
        ResultStep step = new ResultStep("Test",
            [new ResultColumn("Column1", Enumerable.Range(0, 50).ToArray())],
            [new ResultParameter("Group", "Parameter", 5)]
        );
        plan.ChildTestSteps.Add(step);
        ParquetResultListener resultListener = new ParquetResultListener()
        {
            FilePath = { Text = path },
        };
        var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
        result.WaitForResults();

        Assert.That(System.IO.File.Exists(path), Is.True);
        
        var reader = await Reader.CreateAsync(path);

        Assert.That(reader.Count, Is.EqualTo(51));
        var fields = reader.Schema.DataFields
            .Select((f, index) => (f.Name, index)).ToDictionary(t => t.Name, t => t.index);
        for (int i = 0; i < 50; i++)
        {
            Assert.That(reader.ReadCell(i+1, "StepId"), Is.EqualTo(step.Id.ToString()));
            Assert.That(reader.ReadCell(i+1, "Result/Column1"), Is.EqualTo(i));
            Assert.That(reader.ReadCell(i+1, "Step/Group/Parameter"), Is.EqualTo(5));
        }
    }
    
    [Test]
    public async Task StepWithoutResultsTest()
    {
        string path = $"Tests/{nameof(ResultListenerTests)}/{nameof(StepWithoutResultsTest)}.parquet";
        
        TestPlan plan = new TestPlan();
        var step = new DelayStep();
        plan.ChildTestSteps.Add(step);
        ParquetResultListener resultListener = new ParquetResultListener()
        {
            FilePath = { Text = path },
        };
        var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
        result.WaitForResults();

        Assert.That(System.IO.File.Exists(path), Is.True);
        
        var reader = await Reader.CreateAsync(path);

        Assert.That(reader.Count, Is.EqualTo(2));
        Assert.That(reader.Schema.DataFields.Select(f => f.Name), Does.Contain("Step/Duration"));
        Assert.That(reader.Schema.DataFields.Any(f => f.Name.StartsWith("Result/")), Is.EqualTo(false));
    }

    [Test]
    public async Task OverridesOldFiles()
    {
        string path = $"Tests/{nameof(ResultListenerTests)}/{nameof(OverridesOldFiles)}.parquet";
        
        TestPlan plan = new TestPlan();
        ParquetResultListener resultListener = new ParquetResultListener()
        {
            FilePath = { Text = path },
        };
        var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
        result.WaitForResults();
        Assert.That(System.IO.File.Exists(path), Is.True);
            
        result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
        result.WaitForResults();
        Assert.That(System.IO.File.Exists(path), Is.True);

        var reader = await Reader.CreateAsync(path);

        Assert.That(reader.Count, Is.EqualTo(1));
        object?[] values = [result.Id.ToString()];
        Assert.That(reader.ReadRow(0), Is.SupersetOf(values));
    }
}