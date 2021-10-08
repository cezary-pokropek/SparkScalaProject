import common.{PostgresCommon, SparkCommon}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import java.util.Properties


object ExampleSparkTransformer {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
  try {
    logger.info("main method started")
    logger.warn("This is a warning")

    val spark : SparkSession = SparkCommon.createSparkSession().get
    val pgTable = "futureschema.futurex_course_catalog"
    //server:port/database_name
    val pgCourseDataFrame = PostgresCommon.fetchDataFrameFromPgTable(spark, pgTable).get
    logger.info("Fetched PG DataFrame...loger")
    pgCourseDataFrame.show()
    } catch {
      case e:Exception =>
      logger.error("An error has occured in the main method" + e.printStackTrace())

      }


  }


}
