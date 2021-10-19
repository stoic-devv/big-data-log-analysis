import constants.JobsConfigConstants
import jobs.distribution.DistributionJob
import jobs.errdistsort.ErrDistSortJob
import jobs.maxmatching.MaxMatchingJob
import jobs.messagetypes.MessageTypeJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.ToolRunner
import jobs.somepackage.SomeJob
import utils.DistributionUtils.getResetWindow
import utils.ObtainConfigReference

object RunJobs:
  def main(args: Array[String]): Unit = {

    val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, JobsConfigConstants.OBJ_NAME) match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot locate job configuration")
    }

    args.length match {
      case 2 | 3 => {}
      case _ => throw new RuntimeException("Invalid number of arguments")
    }

    args(0) match {

      // distributions of the logs
      case "0" => {
        val distJobConf = new JobConf(MessageTypeJob.getClass)
        distJobConf.setJobName("Distribution of messages by types")
        distJobConf.setJarByClass(this.getClass)

        // set input and output dirs
        FileInputFormat.setInputPaths(distJobConf, new Path(args(1)))
        FileOutputFormat.setOutputPath(distJobConf, new Path(config.getString(JobsConfigConstants.BASE_OUTPUT_DIR) +
          config.getString(JobsConfigConstants.DISTRIBUTION_OUTPUT_DIR)))

        // set job configuration
        DistributionJob(distJobConf)

        // run job
        JobClient.runJob(distJobConf)
      }

      // counting log messages by types
      case "1" => MessageTypeJob(new Path(args(1)))

      // sort error distribution
      case "2" => ErrDistSortJob(new Path(args(1)))

      // finding maximum length of msg by msg type
      case "3" => MaxMatchingJob(new Path(args(1)))

      case _ => {
        ???
      }
    }
  }