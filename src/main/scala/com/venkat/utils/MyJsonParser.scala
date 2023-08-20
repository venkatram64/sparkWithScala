package com.venkat.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

object MyJsonParser {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def readJsonFile(): Config = {
    logger.info("Parsing config file")
    val config = ConfigFactory.load("my_config.json")
    config
  }

  def parsePropsForTable(): String = {
    val tableName = readJsonFile().getString("body.my_emp_table")
    tableName
  }

  def getConfigValue(key: String): String = {
    val keyValue = readJsonFile().getString(key)
    keyValue
  }

}
