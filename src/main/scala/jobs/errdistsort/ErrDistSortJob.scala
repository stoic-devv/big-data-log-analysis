package jobs.errdistsort

import constants.{DistributionJobConstants, JobsConfigConstants}
import entity.CompositeKeyWritableEntity
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import utils.ObtainConfigReference

/**
 * Sets config and runs the error distribution and sorting job
 **/
class ErrDistSortJob

object ErrDistSortJob:

  def runErrDistJob(inputPath: Path, outputPath: Path): Unit = {

    // set job name and jar
    val errDistConf = new JobConf(this.getClass)
    errDistConf.setJobName("Distribution of messages by types")
    errDistConf.setJarByClass(this.getClass)

    // set output key-value class
    errDistConf.setOutputKeyClass(classOf[Text])
    errDistConf.setOutputValueClass(classOf[Text])

    // set mapper and reducer class
    errDistConf.setMapperClass(classOf[ErrDistMapper])
    errDistConf.setReducerClass(classOf[ErrDistReducer])

    // set mapper key-value class
    errDistConf.setMapOutputKeyClass(classOf[Text])
    errDistConf.setMapOutputValueClass(classOf[IntWritable])

    // formatting
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
    
    // set job name and jar
    val errSortConf = new JobConf(this.getClass)
    errSortConf.setJobName("Sort by decreasing error messages")
    errSortConf.setJarByClass(this.getClass)

    // set output key-value class
    errSortConf.setOutputKeyClass(classOf[Text])
    errSortConf.setOutputValueClass(classOf[NullWritable])

    errSortConf.setMapperClass(classOf[ErrSortMapper])

    // set mapper key-value class
    errSortConf.setMapOutputKeyClass(classOf[CompositeKeyWritableEntity])
    errSortConf.setMapOutputValueClass(classOf[NullWritable])

    // formatting
    errSortConf.setInputFormat(classOf[TextInputFormat])
    errSortConf.set(DistributionJobConstants.MAPRED_SEPARATOR_PARAM, ",")
    errSortConf.setOutputFormat(classOf[TextOutputFormat[CompositeKeyWritableEntity, NullWritable]])

    // set input and output dirs
    FileInputFormat.setInputPaths(errSortConf, inputPath)
    FileOutputFormat.setOutputPath(errSortConf, outputPath)

    // run job
    JobClient.runJob(errSortConf)
  }

  def apply(inputPath: String, outputBasePath: String): Unit = {

    val jobsConfig = ObtainConfigReference(JobsConfigConstants.FILE_NAME, JobsConfigConstants.OBJ_NAME) match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot locate job configuration")
    }

    val firstJobOutPath = new Path(outputBasePath +
      jobsConfig.getString(JobsConfigConstants.ERRDIST_OUTPUT_DIR))

    // get error distribution job
    runErrDistJob(new Path(inputPath), firstJobOutPath)

    // sort the error distribution
    runErrSortJob(firstJobOutPath, new Path(outputBasePath +
      jobsConfig.getString(JobsConfigConstants.ERRDISTSORT_OUTPUT_DIR)))

  }