package common

import exceptions.InvalidEnvironmentException

class SparkCommonSpec extends ProjectBase {

  behavior of "Spark Common"

  it should "create session" in {
    val inputConfig : InputConfig = InputConfig(env = "dev", targetDB = "pg")
    val spark = SparkCommon.createSparkSession(inputConfig)
  }

  it should "throw invalid environment exception" in {

    val inputConfig : InputConfig = InputConfig(env = "abc", targetDB = "pg")

    assertThrows[InvalidEnvironmentException] {
      val spark = SparkCommon.createSparkSession(inputConfig)
    }

  }

  it should "check error message for invalid environment" in {

    val inputConfig : InputConfig = InputConfig(env = "abc", targetDB = "pg")

    val exception = intercept[InvalidEnvironmentException] {
      val spark = SparkCommon.createSparkSession(inputConfig).get
    }

    assert(exception.isInstanceOf[InvalidEnvironmentException])
    assert(exception.getMessage.contains("pass a valid environment"))

  }



}
