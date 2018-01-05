package com.discover.preview

import com.discover.preview.`trait`.Reader
import com.discover.preview.service.ReaderFactory
import com.discover.preview.validator.ValidateInputFileFormat
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalactic._
import org.scalactic.Accumulation._
import org.scalactic.{Every, One}
import spark.jobserver._
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}

import scala.util.Try

object TempTable extends SparkSessionJob  {

  type JobOutput = Any
  type JobData = scala.collection.mutable.Map[String, String]
  val locationParam:String = "loc"
  //TODO - add validation for reserved words
  val tableParam:String = "tableName"

  def runJob(sparkSession: SparkSession, jobEnvironment: JobEnvironment, data: JobData): JobOutput = {

    val inputData:String = data.get(locationParam).getOrElse("")
    val tableName:String = data.get(tableParam).getOrElse("")


    val reader:Reader = ReaderFactory.apply(sparkSession,ValidateInputFileFormat.getFileExtension(inputData))
    val df:DataFrame = reader.read(inputData)
    df.persist()
    df.createOrReplaceTempView(tableName)
    "Table:"+tableName+" has been created. Total number of records:"+df.count()+ " Schema:"+df.schema

  }

  /**
    * The following parameters should be passed to the context:
    *   "loc" - full url to the input file
    *   "tableName" - name of the temporary table
    *   "file type" - optional file type parameter, if not specified file suffix will be used to determine data type
    *   ( the following file types are excepted - like JSON, PARQUET, AVRO)
    * @param config
    * @param paramName
    * @return
    */
  def evaluateParam(config: Config, paramName:String):
    String  Or Every[ValidationProblem]= {

    Try(config.getString(paramName))
      .map(inputParam => Good(inputParam))
      .getOrElse(Bad(One(SingleProblem(s"No parameter ${paramName} found"))))
  }

  private def evaluateFileExtension(url:String):
  String  Or Every[ValidationProblem]= {

    Try(ValidateInputFileFormat.getFileExtensionString(url))
      .map(inputParam => Good(inputParam))
      .getOrElse(Bad(One(SingleProblem(s"File extension ${url} is not valid"))))
  }

  def validate(sparkSession: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem]= {

    var data:JobData = scala.collection.mutable.Map[String, String]()
    val location = evaluateParam(config, locationParam)

     val loc = withGood(location) { (loc) => {
          loc
      }
    }
    val tableName = evaluateParam(config, tableParam)

      withGood(loc, tableName) { (loc,tbl) => {
        data += (locationParam -> loc)
        data += (tableParam -> tbl)

        data

      }
    }
  }
}