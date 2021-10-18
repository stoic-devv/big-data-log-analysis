package constants


object LogConstants extends Enumeration {
  type String
  val WARN = "WARN"
  val INFO = "INFO"
  val ERROR = "ERROR"
  val DEBUG = "DEBUG"
}

object JobsConfigConstants extends Enumeration {
  type String
  val FILE_NAME = "jobs"
  val OBJ_NAME = "jobs"
  val BASE_OUTPUT_DIR = "BASE_OUTPUT_DIR"
  val DISTRIBUTION_OUTPUT_DIR = "DISTRIBUTION_OUTPUT_DIR"
  val MESSAGE_TYPE_OUTPUT_DIR = "MESSAGE_TYPE_OUTPUT_DIR"
  val ERRDIST_OUTPUT_DIR = "ERRDIST_OUTPUT_DIR"
  val ERRDISTSORT_OUTPUT_DIR = "ERRDISTSORT_OUTPUT_DIR"
}

object DistributionJobConstants extends Enumeration {
  type String
  val OBJ_NAME = "dist-job"
  val INTERVAL = "INTERVAL"
  val MAPRED_SEPARATOR_PARAM = "mapred.textoutputformat.separator"
}

object ErrDistSortJobConstants extends Enumeration {
  type String
  val OBJ_NAME = "err-dist-sort-job"
  val INTERVAL = "INTERVAL"
}