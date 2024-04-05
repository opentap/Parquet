# Performance
Writing date: 22/03/2024

I have spent the last little while looking at the performance of writing parquet results. Throughout this time i found some interesting results, i will go over these results here along with a conclusion about what to do moving forward.

All data of these benchmarks can be found in [ParquetBenchmarks.xlsx](ParquetBenchmarks.xlsx). The code was tested on the benchmarks branch.

## Related issues
[https://github.com/opentap/Parquet/issues/41](https://github.com/opentap/Parquet/issues/41)

## Motivation
We would expect a binary format like .parquet to be very easy to write to. It should also be compact and fast. Since it is column based we also expect sparse data to be really efficient, but these are all assumptions. So it makes sense to test and see if our assumptions are true when stress testing.

Some questions we want answered:
1. How much overhead compared to parquetdotnet?
2. Why do we write as many bytes as we do?
3. How slow are we compared to other result listeners?

Answering these questions could help us understand how to improve performance and written file size.

## How much overhead compared to parquetdotnet?
To figure out over overhead compared to parquetdotnet we need to benchmark against different versions of parquetdotnet.
### V3.10.0
Version 3.10.0 is the one used in the library, this will give us a good idea of how much overhead we have in the result listener.











|Groups|	Rows|	Columns|	Size|	Type|	Time|	Time (ns)| Log(time) |
|-|-|-|-|-|-|-|-|
|100|	1000|	10|	 4.128.707| 	Int32|	457.050ms|	 457.050.300 |8,7|
|1000|	1000|	10|	 41.289.708| 	Int32|	2.636s|	 1.636.523.700| 9,2|
|10000|	1000|	10|	 413.033.525| 	Int32|	12.749s|	 11.749.740.800| 10,1|
|1000|	100|	10|	 5.284.596| 	Int32|	326.794ms|	 326.794.800 |8,5|
|1000|	1000|	10|	 41.289.708| 	Int32|	1.175s|	 1.175.682.100| 9,1|
|1000|	10000|	10|	 401.394.475| 	Int32|	13.328s|	 13.328.590.300| 10,1|
|1000|	1000|	1|	 4.136.582| 	Int32|	165.293ms|	 165.293.700 |8,2|
|1000|	1000|	10|	 41.289.708| 	Int32|	1.151s|	 1.151.126.500| 9,1|
|1000|	1000|	100|	 413.035.876| 	Int32|	12.008s|	 12.008.597.300| 10,1|
|1000|	1000|	10|	 43.595.756| 	String|	2.199s|	 2.199.358.500| 9,3|

It seems from this data that the amount of time is roughly linear over how many cells are written. It also seems that changing the type of data doesnt have a huge impact on the amount of time it takes.

### V4.23.4
The newest version at the time of writing is 4.23.4 so this version was tested too, to see if finding a solution to updating the library could improve performance significantly.

|Groups|	Rows|	Columns|	Size|	Type|	Time|	Time (ns)|	Log(time)|
|-|-|-|-|-|-|-|-|
|100|	1000|	10|	 4.128.707| 	Int32|	70.919ms|	 70.919.600| 	7,9|
|1000|	1000|	10|	 41.289.708| 	Int32|	777.867ms|	 777.867.300| 	8,9|
|10000|	1000|	10|	 413.033.525| 	Int32|	7.332s|	 7.332.082.800| 	9,9|
|1000|	100|	10|	 5.284.596| 	Int32|	249.630ms|	 249.630.100| 	8,4|
|1000|	1000|	10|	 41.289.708| 	Int32|	727.901ms|	 727.901.800| 	8,9|
|1000|	10000|	10|	 401.394.475| 	Int32|	7.796s|	 6.796.458.300| 	9,8|
|1000|	1000|	1|	 4.136.582| 	Int32|	74.210ms|	 74.210.000| 	7,9|
|1000|	1000|	10|	 41.289.708| 	Int32|	705.855ms|	 705.855.200| 	8,8|
|1000|	1000|	100|	 413.035.876| 	Int32|	7.252s|	 7.252.047.000| 	9,9|
|1000|	1000|	10|	 43.595.756| 	String|	2.829s|	 1.829.949.900| 	9,3|

Again the time seems roughly linear with the amount of cells. In fact it is even more linear over the amount of cells than the previous version. There is also an improvement of about 2x on average in the amount of time taken. This could be an interesting way to gain performance, however there are problems with updating to this version, so for now it isnt viable to update. It might be viable in the future though when tap updates to dotnet instead of net framework/netstandard. Using the library async might also be where this performance comes from, if that is the case we could try using async on the old version.

### Result Listener
Time to see how much overhead the result listener has over raw parquetdotnet.

1 sine step has roughly 30 columns.

|Row groups|Sine rows|Time|Post process|
|-|-|-|-|
|1000|10000|53s|71.2s|
|10000|1000|63.4|83.6s|
|1000|1000|6.33s|8.67s|

From this it seems like there is again a roughly linear relation to the amount of cells. If we use raw parquet v3.10.0 to generate files with the same amount of cells we get roughly half the performance. It took 36.42s to create a file equivalent to the first row. This menas we could see a 2x improvement to performance by optimizing our use of the parquet writer. This is not a lot and has not really been deemed to be worth it.

### Conclusion
We dont have that much overhead compared to just using parquetdotnet. There are definitely some improvements to be had if possible to optimize the result listener, but it does not seem worth it as it would only move us from ~500m cells/min to ~1b cells/min. The same could be seen with moving to version 4.24.4, which could net us a 2x improvement. In total that means it is feasible to get up to ~2b cells/min. That is without changing the underlying way we are writing. Maybe if we write less data, or can optimize the schema somehow that would yield a bigger improvement?

## Why do we write as many bytes as we do?
If we cant get an easy win by optimizing something in the result listener, maybe we can optimize the amount of data we are writing somehow, changing the schema or compression could be candidates for this.

### Changing compression methods.
To understand why we write as many bytes as we do we started out trying to write with different compression levels and methods.
|| |
|-------------|---------|
| Repeat      |	1000    |
| Sine        |	10000   |

This means we are writing a total of 10_000_000 rows.

| Compression | level   | Compression method |	Plan run |	Post processing	Size |
|-------------|---------|--------------------|-----------|-----------------------|
| -1          |	None    | 48.2s              |	59.5s    |	1.93GB               |
| -1          |	Gzip    | 129s               |	182s     |	77.5MB               |
| 5           |	Gzip    | 125s               |	182s     |	77.5MB               |
| -1          |	Snappy  | 58.9s              |	62.0s    |	208MB                |
| 5           |	Snappy  | 52.8s              |	64.0s    |	208MB                |

From this it seems like the best option is snappy, and the compression level doesnt seem to have any effect. Gzip is too slow to warant only a 50% decrease in file size. No compression is a bit faster than snappy, but not enough to warrant a 10x increase in file size. Snappy is also the default in parquetdotnet.

### Data compaction
Maybe there is some performance to gain with how the data is compacted into a file. Parquet should be good with uniform data sets, so maybe more grouped data is better than interleaved or specled data?

First some definitions. By sparse data we mean data that contains a lot of null fields, grouped data means rows with similar data are grouped together, interleaved means every n-th row for small n's the row format is changed, finally specled data is the same as interleaved except for bigger n's. Specled is more grouped than interleaved.

The data for these runs can be found in the [attached excel file](ParquetBenchmarks.xlsx). They are left in there since not all the data was gathered before we were ready with the conclusions of this test.

It seems like as long as we use a reasonable data format it doesn't matter too much what we do. If we dont go into the extreme with specled data and keep the row groups roughly uniform it isn't a problem. These tests werent as conclusive as other tests, but the default behaviour of the result listener, is roughly what performs best for most use cases. Some practical examples were also taken that seem to follow some of the better patterns.

More data, better definitions and more rigorous testing could be useful here to educate people about how to produce plans that are better for large data sets. But ultimately it doesn't seem worth it to dwell too long on this for now.

### Using custom headers
Since it seems like everything just comes down to how many cells we write, is there a possible way to decrease the amount of cells and thereby data, while still having the same data available? We thought about if there was a good way to use custom metadata for repeated data. Since a lot of the data is parameters that wont change between different rows from the same testsstep. It might be helpful to store these as custom metadata instead of inside the rows.

Ultimately this is down to the individual usecases, so some users of opentap were asked to provide .parquet files generated by the result listener, these were then analyzed to see if repeat data could be added to the custom metadata headers. But this seems unlikely as the test plans were all filled with test steps that only generated 1-2 rows per step. This would not make it worth it at all to change this to metadata, since it would basically turn the parquet files into json files. Countarcting all positives of parquet.

### Conclusion
It doesnt seem like there is a whole lot to gain from updating the spec. We are writing the amount of data necessary and in the best way possible to get as much data written as possible. In the future it might be reasonable to take some of these methods up again to improve performance, if it becomes more critical. However for now performance is not a big enough issue to warrant investigating this further.

## How slow are we compared to other result listeners?
A testplan was run like the one for checking compression levels

|| |
|-------------|---------|
| Repeat      |	1000    |
| Sine        |	10000   |

It was ran on 4 different result listeners.

|Listener|	Time|
|-|-|
|Parquet|	87.4s|
|SQLite|	56.8s|
|PGSQL|	1153s|
|CSV|	224s|

It seems like parquet is roughly equivalent to the fastest one. So maybe the problem isn't even the format but maybe the hard drive just cant write much more data than this. Atleast we know now that parquet is almost as fast as it gets, even with the current implementation. We also know it is possible to go up to 4x faster, in case we need it in the future.

## Conclusion
In general it seems like there are some improvements we can get, but using the current schema we are limited to 4x the performance of what we are getting now. That would take us from ~500m cells/min to ~2b cells/min. While this means we could get improvements, it isnt deemed worth it right now, as it would take quite a lot of work, and it is a theoretical maximum. The general nature of the result listeners means we cant know for sure all the data follows a specific schema, that is bound to add some overhead.

This overhead from the result listener could maybe be updated by changing the schema. There are also other benefits that could be gained from understanding the impact of the schema on the data more. We could research more ways to do merging of files, how to do column layouts, how to compact the data in the file, and maybe explore other formats for repeated data like parameters. Maybe some of these could be benefits of switching to another file format than parquet too? Maybe a custom format inspired by parquet?

Finally we tested the performance of the parquet listener against other popular result listeners. Amongst these parquet were one of the fastest ones. This means with the 4x improvement potential we could be the go-to for fast result storing. Right now we are already close to this.