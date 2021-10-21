# Project Structure

We have the following project structure:

1. `doc`: where all the documentation resides. It also contains images and log files for each simulation for better readability and evaluation.
2. `aws-output`: output of the map-reduce jobs run on AWS EMR
   1. `err_dist_sort`: output distribution of error type log messages sorted in decreasing order of frequency
   2. `jobs_distribution`: output distribution of message types in the log file.
   3. `max_pattern_match`: output message searches matching a pattern and returns the one with maximum length per message type.
   4. `message_types`: output counts of number of messages per message type.
3. `src/main/resources`: is where our project configurations are present.
4. `src/main/scala` is where our code resides. In alphabetical order:
   1. `constants`: our project constants to avoid changing strings at multiple places.
   2. `entity`: entities for business logic. These also contain custom hadoop writables.
   3. `jobs`: where the map-reduce code resides. Each subpackage is a functionality we try to achieve using map-reduce and has job(s), mapper(s), reducer(s)
      1. `distribution`: distribution of message types in the log file.
      2. `errdistsort`: distribution of error type log messages sorted in decreasing order of frequency.
      3. `maxmatching`: searches messages matching a pattern and returns the one with maximum length per message type.
      4. `messagetypes`: counts the number of messages per message type.
   4. `utils`: this package provides helper methods to obtain/validate configuration and logger. It also provides a decorator to customize our simulation output.
   5. `RunJobs.scala`: main classes that calls jobs based upon passed arguments
5. `src/test/resources`: is where our configuration for test suites reside.
6. `src/test/scala`: is where our test suites are. This follows same package structure as `src/main/scala`.