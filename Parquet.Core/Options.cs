using System.IO.Compression;
using Parquet;

namespace OpenTap.Plugins.Parquet.Core;

public sealed class Options
{
    public int RowGroupSize { get; set; } = 10_000;
    public CompressionMethod CompressionMethod { get; set; }= CompressionMethod.Snappy;
    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;

    public ParquetOptions ParquetOptions { get; set; } = new ParquetOptions() { UseDeltaBinaryPackedEncoding = false };
}