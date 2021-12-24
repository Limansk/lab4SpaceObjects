import ch.qos.logback.classic.{Level, LoggerContext}
import Helpers._

import java.io.File
import org.mongodb.scala._
import org.mongodb.scala.model.Filters.{and, gt, lt}
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.Projections.excludeId
import org.mongodb.scala.model.Sorts.ascending
import org.slf4j.LoggerFactory

import scala.io.Source

object Main {

  def main(args: Array[String]) = {

    val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val rootLogger = loggerContext.getLogger("org.mongodb.driver")
    val rootLogger2 = loggerContext.getLogger("reactor.util.Loggers")
    rootLogger.setLevel(Level.OFF)
    rootLogger2.setLevel(Level.OFF)

   // writeToMongo()
    queries()

  }

  def writeToMongo(): Unit = {
    val uri = "mongodb://localhost:27017"
    val client: MongoClient = MongoClient(uri)
    val db: MongoDatabase = client.getDatabase("space")
    val collection: MongoCollection[Document] = db.getCollection("objects")

    val files = getListOfFiles("C:\\JsonFiles")
    for (i <- files.indices) {
      val tmp = Source.fromFile(files(i))
      collection.insertOne(BsonDocument(tmp.mkString)).results()
      tmp.close()
    }
    client.close()
    println("Информация записана в базу данных!")
  }

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  private def queries() = {
    val uri = "mongodb://localhost:27017"
    val client: MongoClient = MongoClient(uri)
    val db: MongoDatabase = client.getDatabase("space")
    val collection: MongoCollection[Document] = db.getCollection("objects")

    println("\nЗапросы\n")

    println("Вывести 5 планет, открытых после 2009")
    collection
      .find(gt("year","2009"))
      .limit(5)
      .projection(excludeId())
      .printResults()
    println("")

    println("Вывести 5 планет, чья масса относительно Юпитера больше 1 и меньше 3")
    collection
      .find(and(gt("mass","1"), lt("mass","3")))
      .limit(5)
      .projection(excludeId())
      .printResults()
    println("")

    println("Вывести 10 планет с орбитальным периодом меньше 500 дней и годом открытия меньше 2010")
    collection
      .find(and(lt("orbPeriod","500"), lt("year","2010")))
      .limit(10)
      .projection(excludeId())
      .printResults()
    println("")

    println("Вывести 10 планет с массой относительно Юпитера меньшей 5 и большой полуосью больше 1.5")
    collection
      .find(and(lt("mass","5"), gt("semiMajorAxis","1.5")))
      .limit(10)
      .projection(excludeId())
      .printResults()
    println("")

    println("Вывести 3 самых рано открытых планеты с наклонением больше 82 градусов.")
    collection
      .find(gt("incline","82"))
      .sort(ascending("year"))
      .limit(3)
      .projection(excludeId())
      .printResults()
    println("")

    println("Все запросы выполнены")
  }
}