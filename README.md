# Parquet
This is a package for OpenTAP and features a way to read and write parquet results to the OpenTAP engine.

## Installation
Write `tap package install Parquet` to install the newest release of the Parquet plugin.

## Writing results
Add the result listener called `Parquet` to your test plan.
These are the options available on the result listener at this moment.

| Group | Name | Description | Default value | Values |
|-|-|-|-|-|
| | File path* | The file path of the parquet file(s). Can use `\<ResultType>` to have one file per result type. Further documentation can be found [here](https://doc.opentap.io/Developer%20Guide/Appendix%20A/#result-listeners) | `Results/\<TestPlanName>.\<Date>/<ResultType>.parquet` | `String` |
| | Delete on publish  | If true the files will be removed when published as artifacts. | `false` | `false` `true` |
| Compression | Method** | The compression method to use when writing the file. | `Snappy` | `None` `Snappy` `Gzip` `Lzo` `Brotli` `LZ4` `Zstd` `Lz4Raw` |
| Compression | Level** | The compression level to use when writing the file. | `Optimal` | `Optimal` `Fastest` `NoCompression` |
| Encoding | Rowgroup size | The ideal size of each row group measured in rows. Each Row Group size should roughly fit with one memory page for ideal performance. | 10_000 | `Int` |
| Encoding | Use dictionary encoding** | Whether to use dictionary encoding for columns if data meets ParquetOptions.DictionaryEncodingThreshold The following CLR types are currently supported: string, DateTime, decimal, byte, short, ushort, int, uint, long, ulong, float, double | `true` | `false` `true` |
| Encoding | Dictionary encoding threshold** | Dictionary uniqueness threshold, which is a value from 0 (no unique values) to 1 (all values are unique) indicating when dictionary encoding is applied. Uniqueness factor needs to be less or equal than this threshold | `0.8` | `Float` |
| Encoding | Use delta binary packed encoding** | When set, the default encoding for INT32 and INT64 is 'delta binary packed', otherwise it's reverted to 'plain'. You should only set this to true if your readers understand it. | `true` | `false` `true` |

> \* This can severely impact performance as the result listener has to merge parquet files after writing them. If you use `\<ResultType>` to create several smaller files you will get the best performance.<br>
\*\* This category is for exposed settings from the internal library. They are not recommended to be changed unless you know what you are doing. As it might result in files that cannot be read by all tools.

Depending on what you have specified within the file path, a single test plan could result in one or more parquet files. All steps will allways generate at least one row within a parquet file, and the test plan will always create one and only one row.

All the options on all steps and the test plan are saved in seperate columns, each of those columns having groups that are split by '.'.

## Reading results
This is yet to be implemented. But will work using the result store from OpenTAP.

## Contributing
Any contributions are welcome. If you want to fix something simply fork the repo and create a pull request, you can also open an issue if you don't feel comfortable editing the code. Although your issue might not be prioritised.

### Team
This project was last maintained by
Frederik (@frederikja163)