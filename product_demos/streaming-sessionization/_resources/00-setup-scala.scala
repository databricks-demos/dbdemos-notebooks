// Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", Seq("true", "false"), "Reset all data")
dbutils.widgets.text("db_prefix", "retail", "Database prefix")

// COMMAND ----------

import scala.util.Try
import scala.annotation.tailrec
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}


def getActiveStreams(startWith: String = ""): Seq[StreamingQuery] = {
  spark.streams.active.filter(startWith.isEmpty || _.name.startsWith(startWith))
}

def stopAllStreams(startWith:String = "", sleepTime:Int = 0): Unit = {
  Thread.sleep(sleepTime * 1000) // sleepTime is in seconds, converting to milliseconds
  val streams = getActiveStreams(startWith)
  if (streams.nonEmpty) {
      println(s"Stopping ${streams.length} streams")
      streams.foreach { s => Try(s.stop()).toOption }
      val streamDescr = if (startWith.isEmpty) "streams" else s"streams starting with: $startWith"
      println(s"All $streamDescr stopped.")
  }
}

def waitForAllStreams(startWith: String = ""): Unit = {
  @tailrec
  def stopStreams(streams: Seq[StreamingQuery]): Unit = {
    if (streams.nonEmpty) {
      println(s"${streams.length} streams still active, waiting... (${streams.map(_.name).mkString(", ")})")
      spark.streams.awaitAnyTermination(timeoutMs=1000)
      stopStreams(streams)
    } else println("All streams completed.")
  }
  stopStreams(getActiveStreams(startWith))
}

def waitForTable(tableName: String, timeoutDuration: Int = 120): Unit = {
  (1 to timeoutDuration).foreach { _ =>
    val tablePending = !spark.catalog.tableExists(tableName) || spark.table(tableName).count() == 0
    if (tablePending) Thread.sleep(1000) else return
  }
  throw new Exception(s"couldn't find table $tableName or table is empty. Do you have data being generated to be consumed?")
}

// COMMAND ----------

val catalog = "main__build"
val db = "dbdemos_streaming_sessionization"
val dbName = db
val schema = db

val volumeName = "raw_data"

val rootVolumeFolder = s"/Volumes/$catalog/$db/$volumeName"

// COMMAND ----------

import org.apache.spark.sql.functions._

// Assuming volumeFolder is already defined
val volumeFolder = s"$rootVolumeFolder/scala_sessions"

try {
  // Deliberate error: assigning to an undeclared variable
  spark.conf.set("spark.default.parallelism", "12")
  spark.conf.set("spark.sql.shuffle.partitions", "12")
} catch {
  case e: Exception => 
    println(s"An error occurred: ${e.getMessage} (conf not available in serverless)")
}
