using NUnit.Framework;
using OpenTap;
using OpenTap.Plugins.Parquet;
using Parquet.Data;

namespace Parquet.Tests
{
    public enum MyEnum
    {
        Zero, One, Two
    };

    public class ParameterTestRun : TestRun
    {

        public ParameterTestRun()
        {
            Parameters.Add(new ResultParameter("Integer", 1));
            Parameters.Add(new ResultParameter("Float", 0.5f));
            Parameters.Add(new ResultParameter("Enum", MyEnum.One));
        }
    }

    public class SchemaBuilderTests
    {
        [Test]
        public void CreateEmptySchemaTest()
        {
            Schema actual = CreateEmptySchema();

            var expected = new Schema(new Field[]
            {
                new DataField("ResultName", typeof(string)),
                new DataField("Guid", typeof(string)),
                new DataField("Parent", typeof(string)),
            });

            Assert.That(actual, Is.EqualTo(expected));
        }

        private static Schema CreateEmptySchema()
        {
            var builder = new SchemaBuilder();
            var actual = builder.ToSchema();
            return actual;
        }

        [Test]
        public void CreateStepSchemaTest()
        {
            Schema actual = CreateStepSchema();

            var expected = new Schema(new Field[]
            {
                new DataField("ResultName", typeof(string)),
                new DataField("Guid", typeof(string)),
                new DataField("Parent", typeof(string)),
                new DataField("Step/Integer", typeof(int?)),
                new DataField("Step/Float", typeof(float?)),
                new DataField("Step/Enum", typeof(string)),
            });

            Assert.That(actual, Is.EqualTo(expected));
        }

        private static Schema CreateStepSchema()
        {
            var builder = new SchemaBuilder();
            var run = new ParameterTestRun();
            builder.AddParameters(FieldType.Step, run);
            var actual = builder.ToSchema();
            return actual;
        }

        [Test]
        public void CreatePlanSchemaTest()
        {
            Schema actual = CreatePlanSchema();

            var expected = new Schema(new Field[]
            {
                new DataField("ResultName", typeof(string)),
                new DataField("Guid", typeof(string)),
                new DataField("Parent", typeof(string)),
                new DataField("Plan/Integer", typeof(int?)),
                new DataField("Plan/Float", typeof(float?)),
                new DataField("Plan/Enum", typeof(string)),
            });

            Assert.That(actual, Is.EqualTo(expected));
        }

        private static Schema CreatePlanSchema()
        {
            var builder = new SchemaBuilder();
            var run = new ParameterTestRun();
            builder.AddParameters(FieldType.Plan, run);
            var actual = builder.ToSchema();
            return actual;
        }

        [Test]
        public void CreateResultSchemaTest()
        {
            Schema actual = CreateResultSchema();

            var expected = new Schema(new Field[]
            {
                new DataField("ResultName", typeof(string)),
                new DataField("Guid", typeof(string)),
                new DataField("Parent", typeof(string)),
                new DataField("Result/Result1", typeof(int?)),
                new DataField("Result/StringResults", typeof(string)),
            });

            Assert.That(actual, Is.EqualTo(expected));
        }

        private static Schema CreateResultSchema()
        {
            var builder = new SchemaBuilder();
            var columns = new ResultColumn[] {
                new ResultColumn("Result1", new int[] { 0 }),
                new ResultColumn("StringResults", new string[] { "0" }),
            };
            var table = new ResultTable("Results", columns);
            builder.AddResults(table);
            var actual = builder.ToSchema();
            return actual;
        }

        [Test]
        public void CreateComplexSchemaTest()
        {
            Schema actual = CreateComplexSchema();

            var expected = new Schema(new Field[] {
                new DataField("ResultName", typeof(string)),
                new DataField("Guid", typeof(string)),
                new DataField("Parent", typeof(string)),
                new DataField("Plan/Integer", typeof(int?)),
                new DataField("Plan/Float", typeof(float?)),
                new DataField("Plan/Enum", typeof(string)),
                new DataField("Step/Integer", typeof(int?)),
                new DataField("Step/Float", typeof(float?)),
                new DataField("Step/Enum", typeof(string)),
                new DataField("Result/Result1", typeof(int?)),
                new DataField("Result/StringResults", typeof(string)),
            });

            Assert.That(actual, Is.EqualTo(expected));
        }

        private static Schema CreateComplexSchema()
        {
            var builder = new SchemaBuilder();
            builder.Union(CreateEmptySchema());
            builder.Union(CreatePlanSchema());
            builder.Union(CreateStepSchema());
            builder.Union(CreateResultSchema());
            var actual = builder.ToSchema();
            return actual;
        }

        [TestCase("ResultName", "", FieldType.ResultName)]
        [TestCase("Guid", "", FieldType.Guid)]
        [TestCase("Parent", "", FieldType.Parent)]
        [TestCase("Plan/Float", "Float", FieldType.Plan)]
        [TestCase("Step/String", "String", FieldType.Step)]
        [TestCase("Result/Integer", "Integer", FieldType.Result)]
        [TestCase("Result/With/Multiple/Groups", "With/Multiple/Groups", FieldType.Result)]
        public void GetFieldTypeTest(string name, string expectedName, int expectedType)
        {
            var actual = new DataField(name, typeof(string));

            var actualType = SchemaBuilder.GetFieldType(actual, out var actualName);

            Assert.That((int)actualType, Is.EqualTo(expectedType));
            Assert.That(actualName, Is.EqualTo(expectedName));
        }

        [TestCase("Test", "Test")]
        [TestCase("Result/Hello", "Result", "Hello")]
        [TestCase("Result/Hello", "Result/Hello")]
        [TestCase("Result Hello", "Result.Hello")]
        [TestCase("Result Hello", "Result,Hello")]
        public void ValidParquetNameTest(string expected, params string[] name)
        {
            var actual = SchemaBuilder.GetValidParquetName(name);
            Assert.That(actual, Is.EqualTo(expected));
        }
    }
}