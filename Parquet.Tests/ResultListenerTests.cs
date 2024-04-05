using NUnit.Framework;
using OpenTap;
using OpenTap.Plugins.Parquet;
using Parquet.Data;

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

        [Test]
        public void TestColumnsExist()
        {
            var plan = new TestPlan();
            var step = new MyTestStep();
            plan.Steps.Add(step);
            var resultListener = new ParquetResultListener();
            resultListener.DeleteOnPublish = false;
            resultListener.FilePath.Text = $"Results/Tests/{nameof(TestColumnsExist)}.parquet";

            var result = plan.Execute(new ResultListener[] {resultListener}, Array.Empty<ResultParameter>());
            result.WaitForResults();
            Assert.That(System.IO.File.Exists($"Results/Tests/{nameof(TestColumnsExist)}.parquet"), Is.True);

            using Stream stream = System.IO.File.OpenRead($"Results/Tests/{nameof(TestColumnsExist)}.parquet");
            using ParquetReader reader = new ParquetReader(stream);
            Assert.That(reader.RowGroupCount, Is.EqualTo(2));
            Assert.That(reader.Schema.Fields, Does.Contain(new DataField("ResultName", typeof(string))));
            Assert.That(reader.Schema.Fields, Does.Contain(new DataField("Guid", typeof(string))));
            Assert.That(reader.Schema.Fields, Does.Contain(new DataField("Parent", typeof(string))));
            Assert.That(reader.Schema.Fields, Does.Contain(new DataField("StepId", typeof(string))));
            var rowGroup = reader.ReadEntireRowGroup(0);
            Assert.That(rowGroup[3].Data, Does.Contain(step.Id.ToString()));
        }

        internal class MyTestStep : TestStep
        {
            public override void Run()
            {

            }
        }
    }
}
