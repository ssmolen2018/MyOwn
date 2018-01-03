package com.discover.preview

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalactic._
import spark.jobserver._
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}

import scala.util.Try

object UseNamedObject extends SparkSessionJob with NamedObjectSupport {

  type JobData = String
  type JobOutput = Any

  implicit def rddPersister: NamedObjectPersister[NamedRDD[Row]] = new RDDPersister[Row]
  implicit def dataFramePersister: NamedObjectPersister[NamedDataFrame] = new DataFramePersister
  implicit def broadcastPersister[U]: NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]


  def runJob(sparkSession: SparkSession, jobEnvironment: JobEnvironment, data: JobData): JobOutput = {
    //val df = sparkSession.sparkContext.parallelize(data)
    //  val df:DataFrame = sparkSession.read.json(data)
    //  val df:DataFrame = sparkSession.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))
    //  val df1 = NamedDataFrame(df, false, StorageLevel.MEMORY_AND_DISK)
    //  jobEnvironment.namedObjects.update("df1",df1)
    // jobEnvironment.namedObjects.getNames().toString

    val NamedDataFrame(df, _, _) = jobEnvironment.namedObjects.get[NamedDataFrame]("df1").get
    df.select(data).collect()

  }


  def validate(sparkSession: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem]= {

    try {
      val url = config.getString("sql")
      if (!url.isEmpty) Good(url) else Bad(One(SingleProblem("No sql param")))
    }  catch {
      case _: Exception => Bad(One(SingleProblem("No sql param")))
    }

  }
}