using NUnit.Framework;
using OpenTap;
using OpenTap.Plugins.BasicSteps;
using OpenTap.Plugins.Parquet.Core.Extensions;
using OpenTap.Plugins.Parquet.Extensions;
using Parquet.Extensions;

namespace Parquet.Tests
{
    internal class ExtensionTests
    {
        [TestCase(typeof(int), typeof(int?))]
        [TestCase(typeof(int?), typeof(int?))]
        [TestCase(typeof(string), typeof(string))]
        public void TypeAsNullableTest(Type type, Type expected)
        {
            var actual = type.AsNullable();
            Assert.That(actual, Is.EqualTo(expected));
        }

        [Test]
        public void ResultTableGetResultsTest()
        {
            var table = new ResultTable("test", new ResultColumn[]
            {
                new ResultColumn("Col1", Enumerable.Range(0, 10).ToArray()),
                new ResultColumn("Col2", Enumerable.Repeat(0, 10).ToArray()),
                new ResultColumn("Col3", Enumerable.Range(0, 10).Reverse().ToArray()),
            });

            var results = table.GetResults();
            
            Assert.That(results["Col1"], Is.EquivalentTo(Enumerable.Range(0, 10)));
            Assert.That(results["Col2"], Is.EquivalentTo(Enumerable.Repeat(0, 10).ToArray()));
            Assert.That(results["Col3"], Is.EquivalentTo(Enumerable.Range(0, 10).Reverse().ToArray()));
        }

        [Test]
        public void TestRunGetParametersTest()
        {
            var testStepRun = new TestStepRun(new DelayStep(), Guid.NewGuid(), new[]
            {
                new ResultParameter("Group1", "Param1", 0),
                new ResultParameter("Group1", "Param2", 3.14),
                new ResultParameter("Group1", "Param3", 3.14f),
                new ResultParameter("Group1", "Param4", "value"),
                new ResultParameter("Group2", "Param1", 0),
                new ResultParameter("Group2", "Param2", 3.14),
                new ResultParameter("Group2", "Param3", 3.14f),
                new ResultParameter("Group2", "Param4", "value"),
            });

            var parameters = testStepRun.GetParameters();
            
            Assert.That(parameters["Group1/Param1"], Is.EqualTo(0));
            Assert.That(parameters["Group1/Param2"], Is.EqualTo(3.14));
            Assert.That(parameters["Group1/Param3"], Is.EqualTo(3.14f));
            Assert.That(parameters["Group1/Param4"], Is.EqualTo("value"));
            Assert.That(parameters["Group2/Param1"], Is.EqualTo(0));
            Assert.That(parameters["Group2/Param2"], Is.EqualTo(3.14));
            Assert.That(parameters["Group2/Param3"], Is.EqualTo(3.14f));
            Assert.That(parameters["Group2/Param4"], Is.EqualTo("value"));
        }
    }
}
