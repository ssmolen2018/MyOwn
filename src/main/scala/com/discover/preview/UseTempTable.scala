package com.discover.preview

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalactic._
import spark.jobserver._
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}

import scala.util.Try

object UseTempTable extends SparkSessionJob {

  type JobData = String
  type JobOutput = Any

  implicit def rddPersister: NamedObjectPersister[NamedRDD[Row]] = new RDDPersister[Row]
  implicit def dataFramePersister: NamedObjectPersister[NamedDataFrame] = new DataFramePersister
  implicit def broadcastPersister[U]: NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]


  def runJob(sparkSession: SparkSession, jobEnvironment: JobEnvironment, data: JobData): JobOutput = {
    sparkSession.sql(data).collect()
  }


  def validate(sparkSession: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem]= {

   /* try {
      val url = config.getString("sql")
      if (!url.isEmpty) Good(url) else Bad(One(SingleProblem("No sql param")))
    }  catch {
      case _: Exception => Bad(One(SingleProblem("No sql param")))
    }
*/
      Try(config.getString("sql"))
      .map(sqlString => Good(sqlString))
      .getOrElse(Bad(One(SingleProblem("No sql param"))))
  }
}