using NUnit.Framework;
using OpenTap;
using OpenTap.Plugins.Parquet;

namespace Parquet.Tests;

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

        var table = await ParquetReader.ReadTableFromFileAsync(path);

        Assert.That(table.Count, Is.EqualTo(1));
        object?[] values = [result.Id.ToString()];
        Assert.That(table[0], Is.SupersetOf(values));
    }
}