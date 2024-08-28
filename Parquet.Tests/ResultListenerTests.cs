using NUnit.Framework;
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
            ParquetResultListener resultListener = new ParquetResultListener()
            {
                DeleteOnPublish = false,
                FilePath = { Text = $"Tests/{nameof(ResultListenerTests)}/{nameof(OutputParquetFilesTest)}.parquet" },
            };
            var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
            result.WaitForResults();
            var artifacts = result.Artifacts.ToList();
            Assert.That(artifacts.Count, Is.EqualTo(1));
            Assert.That(artifacts[0], Is.EqualTo($"{nameof(OutputParquetFilesTest)}.parquet"));

            Assert.That(System.IO.File.Exists($"Tests/{nameof(ResultListenerTests)}/{nameof(OutputParquetFilesTest)}.parquet"), Is.True);
        }

        [Test]
        public async Task OverridesOldFiles()
        {
            TestPlan plan = new TestPlan();
            ParquetResultListener resultListener = new ParquetResultListener()
            {
                DeleteOnPublish = false,
                FilePath = { Text = $"Tests/{nameof(ResultListenerTests)}/{nameof(OverridesOldFiles)}.parquet" },
            };
            var result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
            result.WaitForResults();
            Assert.That(System.IO.File.Exists($"Tests/{nameof(ResultListenerTests)}/{nameof(OverridesOldFiles)}.parquet"), Is.True);
            
            result = plan.Execute(new ResultListener[] { resultListener }, Array.Empty<ResultParameter>());
            result.WaitForResults();
            Assert.That(System.IO.File.Exists($"Tests/{nameof(ResultListenerTests)}/{nameof(OverridesOldFiles)}.parquet"), Is.True);

            using Stream stream = System.IO.File.OpenRead($"Tests/{nameof(ResultListenerTests)}/{nameof(OverridesOldFiles)}.parquet");
            using ParquetReader reader = await ParquetReader.CreateAsync(stream);
            
            Assert.That(reader.RowGroupCount, Is.EqualTo(1));
            var group = await reader.ReadEntireRowGroupAsync(0);
            using var groupReader = reader.OpenRowGroupReader(0);
            Assert.That(groupReader.RowCount, Is.EqualTo(1));
            Assert.That(group[1].Data.GetValue(0), Is.EqualTo(result.Id.ToString()));
        }
    }
}
