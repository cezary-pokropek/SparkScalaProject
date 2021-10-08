package common

import common.SparkCommon.logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import org.slf4j.LoggerFactory


object PostgresCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def getPostgresCommonProps() : Properties = {

    logger.info("getPostgresCommonProps method started")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user", "postgres")
    pgConnectionProperties.put("password", "Cezary123!")

    logger.info("getPostgresCommonProps method ended")
    pgConnectionProperties

  }

  def getPostgresServerDatabase() : String ={
    logger.info("getPostgresServerDatabase() started")
    val pgUrl = "jdbc:postgresql://localhost:5432/futurex"

    logger.info("getPostgresServerDatabase() started")
    pgUrl
  }


  def fetchDataFrameFromPgTable(spark : SparkSession, pgTable : String) : Option[DataFrame] = {
    try{
      logger.info("fetchDataFrameFromPgTable method started")
      val pgProp = getPostgresCommonProps()
      val pgURLdetails = getPostgresServerDatabase()
      val pgCourseDataframe = spark.read.jdbc(pgURLdetails,pgTable,pgProp)
      logger.info("fetchDataFrameFromPgTable method ended")
      Some(pgCourseDataframe)
    } catch {
      case e: Exception =>
        logger.error("An error has occured in fetchDataFrameFromPgTable")
        System.exit(1)
        None
    }


  }



  }
