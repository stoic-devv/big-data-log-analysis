package distribution

import constants.{DistributionJobConstants, JobsConfigConstants}
import entity.MsgTypeCountWritableEntity
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, MapReduceBase, Mapper, OutputCollector, Reporter}
import utils.DistributionUtils.getResetWindow
import utils.{CreateLogger, DistributionUtils, ObtainConfigReference}
import utils.LogReader.parseLogStr;

class DistributionMapper extends MapReduceBase with Mapper[Object, Text, Text, MsgTypeCountWritableEntity]{

    val logger = CreateLogger(classOf[DistributionMapper])
    val one = new IntWritable(1)
    val config = ObtainConfigReference(JobsConfigConstants.FILE_NAME, DistributionJobConstants.OBJ_NAME) match {
        case Some(value) => value
        case None => throw new RuntimeException("Cannot locate job configuration in mapper")
    }
    val interval = config.getInt(DistributionJobConstants.INTERVAL)
    val resetInterval = getResetWindow(interval)

    override def map(key: Object, value: Text, output: OutputCollector[Text, MsgTypeCountWritableEntity], reporter: Reporter): Unit = {
        val logEntity = parseLogStr(value.toString)
        val tmInterval = DistributionUtils.getTimeInterval(logEntity.timestamp, interval,resetInterval )
        val msgWritableEntity = new MsgTypeCountWritableEntity(new Text(logEntity.messageType), one)
        output.collect(new Text(tmInterval.toString), msgWritableEntity)
    }

}