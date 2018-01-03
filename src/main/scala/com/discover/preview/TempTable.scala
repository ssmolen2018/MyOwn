package com.discover.preview

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalactic._
import org.scalactic.Accumulation._
import org.scalactic.{ Every, One }
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

    val df:DataFrame = sparkSession.read.json(inputData)
    df.persist()
    df.createOrReplaceTempView(tableName)
    df.count()

  }

  def evaluateParam(config: Config, paramName:String):
  String  Or Every[ValidationProblem]= {

      Try(config.getString(paramName))
     .map(inputParam => Good(inputParam))
     .getOrElse(Bad(One(SingleProblem(s"No parameter ${paramName} found"))))
  }

  def validate(sparkSession: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem]= {

    var data:JobData = scala.collection.mutable.Map[String, String]()
    val location = evaluateParam(config, locationParam)
    val tableName = evaluateParam(config, tableParam)

      withGood(location, tableName) { (loc,tbl) => {
        data += (locationParam -> loc)
        data += (tableParam -> tbl)

        data

      }
    }
  }
}