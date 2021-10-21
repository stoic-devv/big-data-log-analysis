# Big data log analysis

## About

We use Hadoop framework to do big data analysis on a synthetically generated application log file. We do various types of search, count and filtering analysis, first on [Hortonworks Sandbox](https://www.cloudera.com/downloads/hortonworks-sandbox.html) and then on [AWS EMR](https://aws.amazon.com/emr/).

## Build and Run Configuration
1. This project was developed using `IntelliJ` and is written in `Scala`. We use `sbt` build tool for compiling, testing, and managing dependencies.
2. [RunJobs.scala](/src/main/scala/RunJobs.scala) is the main class for this application. This is the class responsible for running all the map-reduce jobs.
3. You can build this project using [sbt plugin](https://plugins.jetbrains.com/plugin/5007-sbt) for IntelliJ or clone and import this repository as an sbt project. Alternatively, you can download and [install](https://www.scala-sbt.org/download.html) the sbt build tool, and run from your terminal.
4. Sbt commands for this project:
`> compile` and `test` should run successfully with the following results
```console
[info] Run completed in 1 second, 950 milliseconds.
[info] Total number of tests run: 7
[info] Suites: completed 5, aborted 0
[info] Tests: succeeded 7, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

`> assembly` creates a fat executable jar that we'll be using in the Hortonworks sandbox as well as in AWS EMR.
```console
[info] Assembly up to date: <project-dir>\target\scala-3.0.2\big-data-log-analysis-assembly-0.1.jar
```
5. To run the jobs with hadoop execute the created jar using the following command:<br/>
```console
hadoop jar <path-to-jar> <job-number> <data-input-directory> <base-output-directory>
```

where `job-number` can be:<br/>
`0` for running distribution of log messages by message types<br/>
`1` for counting messages by message types<br/>
`2` for distribution of error messages and sorting them in decreasing order of frequency<br/>
`3` for maximum length of a log message string by message type matching a given pattern<br/>
`4` for running all the jobs mentioned above
6. The log file used for the analysis is attached [here](https://drive.google.com/file/d/1wxzwClaRdwsV1zd465-KUpSljOjAErSW/view?usp=sharing)


## Project wiki
1. [Project structure and design](design.md)
2. [Build and run project with Hortonworks Sandbox](/doc/build-run-config.md)
3. [Log file analysis](analysis.md)
4. [Deploying to AWS](aws.md)


## Improvements

### Functional Improvements
1. Explore custom partitioning before sorting for a more efficient map-reduce

### Technical Improvements
1. Convert the build and run configuration into a bash script.
2. Make the log parser more flexible and robust.