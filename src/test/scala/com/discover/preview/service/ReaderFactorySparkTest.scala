package com.discover.preview.service
import com.discover.preview.`type`.DataFormat
import com.discover.preview.exception.InvalidDataTypeException
import org.apache.spark.sql.SparkSession
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FunSuite, Matchers}

/**
  * Use ScalaTest property based testing for simple functions and UDFs
  */
class ReaderFactorySparkTest
  extends FunSuite with Matchers
    with GeneratorDrivenPropertyChecks {
    //with Inspectors {


  // create custom generators
  val dataTypes = Gen.oneOf(Seq(
    "JSON",
    "PARQUET"
  ))

  val invalidDataTypes = Gen.oneOf(Seq(
    "AVRO",
    "UNKNOWN"))

  var spark: SparkSession = new SparkSession.Builder().appName("data_load_testing").master("local[*]").getOrCreate()

  test("test ReaderFactory.apply") {

    forAll(dataTypes) { (dataType: String) =>

      val reader = ReaderFactory(spark, DataFormat.fromString(dataType))

      if (DataFormat.fromString(dataType).equals(DataFormat.PARQUET))
        reader.isInstanceOf[ParquetFileReader]
      else (DataFormat.fromString(dataType).equals(DataFormat.JSON))
      reader.isInstanceOf[JsonFileReader]

    }
  }

  /* Example of checking if an exception has been thrown.
     PS - It is not a good idea to throw an exception in Scala */
  test("test Not valid DataFormat") {


    forAll(invalidDataTypes) {
      (dataType: String) =>
        an[InvalidDataTypeException] should be thrownBy ReaderFactory(spark, DataFormat.fromString(dataType))
    }
  }

}
  /*test("test StringUDF.lastNCharacters check an exception message") {


    forAll(Gen.alphaStr, Gen.negNum[Int]) { (testString: String,
                                             testLength: Int) =>

      val exception = intercept[InvalidParameterException]
          { StringUDF.lastNCharacters(testString, testLength)}

       exception.getMessage shouldEqual "Number of characters can not be negative"
    }

  }
  */

