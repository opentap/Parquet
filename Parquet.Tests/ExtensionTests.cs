using NUnit.Framework;
using Parquet.Extensions;

namespace Parquet.Tests
{
    internal class ExtensionTests
    {
        [Test]
        public void ToHashsetTest()
        {
            var arr = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0 };
            var expected = Enumerable.ToHashSet(arr);
            var actual = LinqExtensions.ToHashSet(arr);
            Assert.That(actual, Is.EqualTo(expected));
        }

        [TestCase(typeof(int), typeof(int?))]
        [TestCase(typeof(int?), typeof(int?))]
        [TestCase(typeof(string), typeof(string))]
        public void TypeAsNullableTest(Type type, Type expected)
        {
            var actual = type.AsNullable();
            Assert.That(actual, Is.EqualTo(expected));
        }

        [TestCase(1, 1)]
        [TestCase(100, 0)]
        [TestCase(100, 90, 90)]
        public void DictionaryGetValueOrDefaultTest(int key, int expected, int def = default)
        {
            var dict = new Dictionary<int, int>()
                { {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}, {0, 0} };
            int actual = dict.GetValueOrDefault(key, def);
            Assert.That(actual, Is.EqualTo(expected));
        }
    }
}
