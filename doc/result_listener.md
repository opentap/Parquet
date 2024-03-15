# Result listener
Add the result listener called `Parquet` to your test plan.
These are the options available on the result listener at this moment.

| Name | Description | Default value |
|-|-|-|
| File path*  | The file path of the parquet file(s). Can use \<ResultType> to have one file per result type. Further documentation can be found [here](https://doc.opentap.io/Developer%20Guide/Appendix%20A/#result-listeners) | `Results/\<TestPlanName>.\<Date>.parquet` |
| Delete on publish  | If true the files will be removed when published as artifacts. | `false` |

> File path* note: This can severely impact performance as the result listener has to merge parquet files after writing them. The more you can seperate the results into seperate files by using \<ResultType> the better the result listener will perform.

Depending on what you have specified within the file path, a single test plan could result in one or more parquet files. All steps will allways generate at least one row within a parquet file, and the test plan will always create one and only one row.

## Output format
The output is in [.parquet](https://parquet.apache.org/) format. For now there is only one recognized format, but in the future there might be added more. The current format can be seen [here](formats/1.0.0.0.md)
