## Analysis
The log file used for this analysis is of size 200 MB. This is by no means a big data by today's standard, but it is sufficient to illustrate the map-reduce and the analysis using it.
The log messages in the file are of the type `WARN`, `INFO`, `DEBUG`, `ERROR`.
Each log message is of the format: `<timestamp> <context> <message type> <logging class> <log message>` <br />
In the analysis we are only interested in: `<timestamp>`, `<message type>`, `<log message>`

> :warning: the output files of all jobs are zipped and attached in the root directory

### Message type distribution
Given a log file, the task is to analyse the distribution of log message types across a predefined interval. This interval can be set in the `jobs.conf` file. The default interval is set to `5000 ms`.<br />
`DistributionMapper`: it takes input an auto-generated key and a line from log. It parses this log messages for relevant fields and maps the time stamp to a time interval. Output of this mapper is a timestamp interval key and a custom writable containing message type and message.<br />
`DistributionReducer`: it iterates over the custom writable and outputs timestamp interval as key and comma separated frequencies of `WARN`, `INFO`, `DEBUG`, `ERROR`<br />
The distributions' `csv` can be found [here](../aws-output/jobs_distribution)

### Message type count
This task is to count the number of log messages by message type. <br />
`MessageTypeMapper`: it takes input an auto-generated key and a line from log. It writes the log message type and count (`=1`) as output.
`MessageTypeReducer`: it iterates through the count values and sums them up. Since the keys are message types, we need not do additional handling for separating them.
The output of counts found [here](../aws-output/message_types) are:
```console
WARN	287851
INFO	1057178
DEBUG	150382
ERROR	15103
```

### Error type sort by frequency
This task involves evaluating the distribution of `ERROR` type log message and then sort them in descending order. Default interval is set to `5000 ms` and can be set in the `jobs.conf` file.
`ErrDistMapper`: it takes input an auto-generated key and a line from log. It parses the line and writes to the stream only if the message is of type `ERROR`. The key of the output stream is a time interval string in which the timestamp belongs.
`ErrDistReducer`: iterates and counts the number of messages within the timestamp. 
`ErrSortMapper`: here we use a custom writable with composite key (key and value send to this mapper is combined to form a composite key) which compares in decreasing order of the count of messages in an interval. Since the mapper writes the output in a sorted manner, we leverage this to write the output stream.
The maximum frequency of error messages : `14` . Output can be found [here](../aws-output/err_dist_sort)

### Maximum length pattern matching
In this task we search for the messages matching a given pattern. We finally write the longest messages by message type to the output. Pattern can be set in `jobs.conf`. <br />
The default pattern used for analysis is: `([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}`
`MaxMatchingMapper`: Writes to the output only if the given Text value matches the pattern
`MaxMatchingReducer`: Iterates over the values to find the max length message and writes to output.
With the given log file, following are the results:
```console
WARN,15:13:09.714, tp)*,RTO#qJMq#d|I@[!0_zbce2ce2cg1ae2be3ag2cg2E6jcg0ae3ce3T8obg1M9hF5kBfnqt&%q's#@cwMP?&h`H=zU, 93
INFO,09:40:50.412, 8G;,3m_T`G#H]&Yh:Ei1%fp''5`Z6wE5jcf0cg0ce1%<Bqz8fMm#{JWqMdoc_2N/|wf8], 69
DEBUG,12:55:03.885, tnCU:-{iS11ce0ce2ag3cg0M8qcg2E5mbg2L)0fx<x$G%=, 46
ERROR,10:05:52.729, 6e-=Zr1oae2Z5jae0J7wcg1af2?@ph%]75, 34
```
