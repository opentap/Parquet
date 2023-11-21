# Parquet
This is a package for OpenTAP and features a way to read and write parquet results to the OpenTAP engine.

## Installation
Write `tap package install Parquet` to install the newest release of the Parquet plugin.
If you are using OpenTAP in a runner it might already be installed if you are using the latest version.

## Writing results
Add the result listener called `Parquet` to your test plan.
These are the options available on the result listener at this moment.

| Name | Description | Default value |
|-|-|-|
| File path*  | The file path of the parquet file(s). Can use \<ResultType> to have one file per result type. Further documentation can be found here: https://doc.opentap.io/Developer%20Guide/Appendix%20A/#result-listeners | Results/\<TestPlanName>.\<Date>.parquet |

> File path* note: This can severely impact performance as the result listener has to merge parquet files after writing them. The more you can seperate the results into seperate files by using \<ResultType> the better the result listener will perform.

Depending on what you have specified within the file path, a single test plan could result in one or more parquet files. All steps will allways generate at least one row within a parquet file, and the test plan will always create one and only one row.

All the options on all steps and the test plan are saved in seperate columns, each of those columns having groups that are split by '.'.
> Note: The result listener uses the parquet files it reads as storage while running. Deleting the resulting parquet files might cause issues, so tread lightly.

## Reading results
This is yet to be implemented. But will work using the result store from OpenTAP.

## Contributing
Any contributions are welcome. If you want to fix something simply fork the repo and create a pull request, you can also open an issue if you don't feel comfortable editing the code. Although your issue might not be prioritised.

### Team
This project is currently maintained by
Frederik (@frederikja163)