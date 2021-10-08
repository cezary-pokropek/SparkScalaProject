import common.{FXJsonParser, InputConfig, PostgresCommon, SparkCommon, SparkTransformer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties


object ExampleSparkTransformer {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
  try {
    logger.info("main method started")
    logger.warn("This is a warning")

    if (args.length != 1) {
      System.out.println("No Argument passed..exiting")
      System.exit(1)
    }

    val inputConfig : InputConfig = InputConfig(env = args(0), targetDB = args(1))

    val spark : SparkSession = SparkCommon.createSparkSession(inputConfig).get

    //Create Hive Table
    //SparkCommon.createHiveTable(spark)
    val CourseDF = SparkCommon.readHiveTable(spark).get
    CourseDF.show()


    // Replace Null Value

    val transformedDF1 = SparkTransformer.replaceNullValues(CourseDF)
    transformedDF1.show()

    if (inputConfig.targetDB == "pg") {
      logger.info("Writing to PG Table")
      val pgCourseTable = FXJsonParser.fetchPGTargetTable()
      logger.warn("******** pgCourseTable **** is "+pgCourseTable)

      PostgresCommon.writeDFToPostgresTable(transformedDF1,pgCourseTable)
    } else if (inputConfig.targetDB == "hive") {
      logger.info("Writing to Hive Table")

      // Write to a Hive Table
      SparkCommon.writeToHiveTable(spark,transformedDF1,"customer_transformed")
      logger.info("Finished writing to Hive Table..in main method")

    }
    //transformedDF1.write.format("csv").save("transformed-df")

//    val pgTable = "futureschema.futurex_course_catalog"
//    //server:port/database_name
//    val pgCourseDataFrame = PostgresCommon.fetchDataFrameFromPgTable(spark, pgTable).get
//    logger.info("Fetched PG DataFrame...loger")
//    pgCourseDataFrame.show()
    } catch {
      case e:Exception =>
      logger.error("An error has occured in the main method" + e.printStackTrace())

      }



  }


}
