package com.tomtom.eventimporter

import com.mongodb.MongoCredential._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.DurationLong
import scala.languageFeature.postfixOps._


object Importer extends App {

  println("Hello world")




  val user: String = "root" // the user name
  val source: String = "admin" // the source where the user is define

  println(System.getProperty("mongopassword"))
  val password: Array[Char] = System.getProperty("mongopassword").toCharArray // the password as a character array
  val credential: MongoCredential = createCredential(user, source, password)

  val codecRegistry = fromRegistries(fromProviders(classOf[Spot]), DEFAULT_CODEC_REGISTRY)


  // Use a Connection String// or provide custom MongoClientSettings
  val settings: MongoClientSettings =
    MongoClientSettings
      .builder()
      .applyToClusterSettings(b => b.hosts(List(new ServerAddress("104.248.240.148")).asJava))
      .credential(credential)
      .codecRegistry(codecRegistry)
      .build()

  val mongoClient: MongoClient = MongoClient(settings)

  val database: MongoDatabase = mongoClient.getDatabase("parkathon")

  val collection: MongoCollection[Spot] = database.getCollection("spots")

  val spot = Spot(latitude = 52.112233, longitude = 11.332211, source = "testApp")

  Await.result(collection.insertOne(spot).toFuture(), 5 minutes)


  mongoClient.close()

}
