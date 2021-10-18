package jobs.errdistsort

import constants.{ErrDistSortJobConstants, JobsConfigConstants}
import entity.CompositeKeyEntity
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reporter}
import utils.DistributionUtils.getResetWindow
import utils.LogReader.parseLogStr
import utils.{CreateLogger, DistributionUtils, ObtainConfigReference}

class ErrSortMapper extends MapReduceBase with Mapper[Object, Text, CompositeKeyEntity, NullWritable]{

  val logger = CreateLogger(classOf[ErrSortMapper])
  val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, ErrDistSortJobConstants.OBJ_NAME) match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot locate job configuration in mapper")
  }
  val interval = config.getInt(ErrDistSortJobConstants.INTERVAL)
  val resetInterval = getResetWindow(interval)

  override def map(key: Object, value: Text, output: OutputCollector[CompositeKeyEntity, NullWritable], reporter: Reporter): Unit = {
    val entry = value.toString.split(",")
    output.collect(new CompositeKeyEntity(new Text(entry(0)), new IntWritable(entry(1).toInt)), NullWritable.get())
  }

}

