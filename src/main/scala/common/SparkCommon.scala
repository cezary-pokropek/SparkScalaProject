package common

import common.PostgresCommon.getClass
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkCommon {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(): Option[SparkSession] = {
    try{
      // Create a Spark Session
      // For Windows
      logger.info("createSparkSession method started")
      System.setProperty("hadoop.home.dir", "C:\\winutils")

      val spark = SparkSession
        .builder
        .appName("HelloSpark")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()

      logger.info("createSparkSession method ended")
      Some(spark)
      //println("Created Spark Session")
      //val sampleSeq = Seq((1,"spark"),(2,"Big Data"))

      //val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
      //df.show()
      //df.write.format(source = "csv").save(path = "samplesq")
    } catch {
      case e: Exception =>
        logger.error("An error has occured in Spark Session creation")
        System.exit(1)
        None
    }

  }

}
