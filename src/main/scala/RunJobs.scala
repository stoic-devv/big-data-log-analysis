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
import utils.DistributionUtils.getResetWindow
import utils.ObtainConfigReference

/**
 * Runs the job based on arguments provided
 **/
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
      case "0" => DistributionJob(new Path(args(1)))

      // counting log messages by types
      case "1" => MessageTypeJob(new Path(args(1)))

      // sort error distribution
      case "2" => ErrDistSortJob(new Path(args(1)))

      // finding maximum length of msg by msg type
      case "3" => MaxMatchingJob(new Path(args(1)))

      // run all jobs
      case "4" => {
        val inputPath = new Path(args(1))
        DistributionJob(inputPath)
        MessageTypeJob(inputPath)
        ErrDistSortJob(inputPath)
        MaxMatchingJob(inputPath)
      }

      // not implemented
      case _ => {???}

    }
  }