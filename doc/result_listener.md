# Result listener
The parquet result listener allows listening to events from OpenTAP and writing the data from these events into .parquet files. The data added consists of TestStep results, TestStep parameters and Plan parameters.

To use the result listener follow the steps below:
1. Install the Parquet tappackage `tap package install Parquet`
2. Create/open a test plan in your favorite IDE
3. Add a parquet result listener to you result listener settings. It can be found under the name "Database/Parquet"
4. (Optional) configure the result listener so it outputs where/how you like.
5. Run the test plan

Now you should see a file appear at the given path, this file can be opened using TAD and in the future a resultstore will be added so it can be viewed in OpenTAP editors too.

## Options
These are the options available on the result listener at this moment.

| Name | Description | Default value |
|-|-|-|
| File path*  | The file path of the parquet file(s). Can use \<ResultType> to have one file per result type. Further documentation can be found [here](https://doc.opentap.io/Developer%20Guide/Appendix%20A/#result-listeners) | `Results/\<TestPlanName>.\<Date>.parquet` |
| Delete on publish  | If true the files will be removed when published as artifacts. | `false` |

> File path* note: This can severely impact performance as the result listener has to merge parquet files after writing them. The more you can seperate the results into seperate files by using \<ResultType> the better the result listener will perform.

Depending on what you have specified within the file path, a single test plan could result in one or more parquet files. All steps will allways generate at least one row within a parquet file, and the test plan will always create one and only one row.

## Output format
The output is in [.parquet](https://parquet.apache.org/) format. For now there is only one recognized format, but in the future there might be added more. The current format can be seen [here](formats/1.0.0.0.md)
