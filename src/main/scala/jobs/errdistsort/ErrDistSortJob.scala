package jobs.errdistsort

import constants.{DistributionJobConstants, JobsConfigConstants}
import entity.CompositeKeyWritableEntity
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import utils.ObtainConfigReference

class ErrDistSortJob

object ErrDistSortJob:

  def runErrDistJob(inputPath: Path, outputPath: Path): Unit = {

    val errDistConf = new JobConf(this.getClass)
    errDistConf.setJobName("Distribution of messages by types")
    errDistConf.setJarByClass(this.getClass)

    errDistConf.setOutputKeyClass(classOf[Text])
    errDistConf.setOutputValueClass(classOf[Text])

    errDistConf.setMapperClass(classOf[ErrDistMapper])
    errDistConf.setReducerClass(classOf[ErrDistReducer])

    errDistConf.setMapOutputKeyClass(classOf[Text])
    errDistConf.setMapOutputValueClass(classOf[IntWritable])

    errDistConf.setInputFormat(classOf[TextInputFormat])
    errDistConf.set(DistributionJobConstants.MAPRED_SEPARATOR_PARAM, ",")
    errDistConf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])

    // set input and output dirs
    FileInputFormat.setInputPaths(errDistConf, inputPath)
    FileOutputFormat.setOutputPath(errDistConf, outputPath)

    // run job
    JobClient.runJob(errDistConf)

  }

  def runErrSortJob(inputPath: Path, outputPath: Path): Unit = {
    val errSortConf = new JobConf(this.getClass)
    errSortConf.setJobName("Sort by decreasing error messages")
    errSortConf.setJarByClass(this.getClass)

    errSortConf.setOutputKeyClass(classOf[Text])
    errSortConf.setOutputValueClass(classOf[NullWritable])

    errSortConf.setMapperClass(classOf[ErrSortMapper])
    //errSortConf.setReducerClass(classOf[ErrDistReducer])
    
    errSortConf.setMapOutputKeyClass(classOf[CompositeKeyWritableEntity])
    errSortConf.setMapOutputValueClass(classOf[NullWritable])

    errSortConf.setInputFormat(classOf[TextInputFormat])
    errSortConf.set(DistributionJobConstants.MAPRED_SEPARATOR_PARAM, ",")
    errSortConf.setOutputFormat(classOf[TextOutputFormat[CompositeKeyWritableEntity, NullWritable]])

    // set input and output dirs
    FileInputFormat.setInputPaths(errSortConf, inputPath)
    FileOutputFormat.setOutputPath(errSortConf, outputPath)

    // run job
    JobClient.runJob(errSortConf)
  }

  def apply(inputPath: Path): Unit = {

    val jobsConfig = ObtainConfigReference(JobsConfigConstants.FILE_NAME, JobsConfigConstants.OBJ_NAME) match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot locate job configuration")
    }

    val firstJobOutPath = new Path(jobsConfig.getString(JobsConfigConstants.BASE_OUTPUT_DIR) +
      jobsConfig.getString(JobsConfigConstants.ERRDIST_OUTPUT_DIR))

    // get error distribution job
    runErrDistJob(inputPath, firstJobOutPath)

    // sort the error distribution
    runErrSortJob(firstJobOutPath, new Path(jobsConfig.getString(JobsConfigConstants.BASE_OUTPUT_DIR) +
      jobsConfig.getString(JobsConfigConstants.ERRDISTSORT_OUTPUT_DIR)))

  }