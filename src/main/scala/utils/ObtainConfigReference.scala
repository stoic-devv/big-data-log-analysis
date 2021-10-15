package utils

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success, Try}

object ObtainConfigReference:

  private val logger = CreateLogger(classOf[ObtainConfigReference])

  private def validateConfig(config: Config, confEntry: String):Boolean = Try(config.getConfig(confEntry)) match {
    case Failure(exception) =>
      logger.error(s"Failed to retrieve config entry $confEntry for reason $exception"); false
    case Success(_) => true
  }

  def apply(configFile: String, confEntry: String): Option[Config] = {
    val config = ConfigFactory.load(configFile)
    if validateConfig(config, confEntry) then Some(config.getConfig(confEntry)) else None
  }

class ObtainConfigReference