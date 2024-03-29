﻿using NUnit.Framework;
using OpenTap;
using OpenTap.Plugins.Parquet;

namespace Parquet.Tests
{
    internal class ResultListenerTests
    {
        [Test]
        public void OutputParquetFilesTest()
        {
            TestPlan plan = new TestPlan();
            ParquetResultListener resultListener = new ParquetResultListener();
            resultListener.DeleteOnPublish = false;
            resultListener.FilePath.Text = $"Results/Tests/{nameof(OutputParquetFilesTest)}.parquet";
            var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
            result.WaitForResults();
            var artifacts = result.Artifacts.ToList();
            Assert.That(artifacts.Count, Is.EqualTo(1));
            Assert.That(artifacts[0], Is.EqualTo($"{nameof(OutputParquetFilesTest)}.parquet"));

            Assert.That(System.IO.File.Exists($"Results/Tests/{nameof(OutputParquetFilesTest)}.parquet"), Is.True);
        }

        [Test]
        public void DoesntMergeWithOldFiles()
        {
            TestPlan plan = new TestPlan();
            ParquetResultListener resultListener = new ParquetResultListener();
            resultListener.DeleteOnPublish = false;
            resultListener.FilePath.Text = $"Results/Tests/{nameof(DoesntMergeWithOldFiles)}.parquet";
            var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
            result.WaitForResults();
            Assert.That(System.IO.File.Exists($"Results/Tests/{nameof(DoesntMergeWithOldFiles)}.parquet"), Is.True);

            result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
            result.WaitForResults();
            Assert.That(System.IO.File.Exists($"Results/Tests/{nameof(DoesntMergeWithOldFiles)}.parquet"), Is.True);

            using Stream stream = System.IO.File.OpenRead($"Results/Tests/{nameof(DoesntMergeWithOldFiles)}.parquet");
            using ParquetReader reader = new ParquetReader(stream);
            Assert.That(reader.RowGroupCount, Is.EqualTo(1));
            var rowgroup = reader.ReadEntireRowGroup(0);
            Assert.That(rowgroup.Any(c => c.Data.GetValue(0)?.Equals(result.Id.ToString()) ?? false), Is.True);
        }
    }
}
