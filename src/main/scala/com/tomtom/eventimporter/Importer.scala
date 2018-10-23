package com.tomtom.eventimporter

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, UUID}

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.mongodb.MongoCredential._
import com.tomtom.parking.donald.fcd.Fix
import com.tomtom.parking.donald.live.io.KinesisMessageStreamFactory
import com.tomtom.parking.donald.live.messages.{ParkOutEvent, ParkingEvent}
import com.tomtom.parking.donald.live.serialization.ParkingEventDeserializer
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.languageFeature.postfixOps._


class Importer(source: Source[(String, ParkingEvent), Future[Done]], mongoCollection: MongoCollection[Spot]) {

  private val decider: Supervision.Decider = { e =>
    println("Unhandled exception in stream", e)
    // TODO: What does this actually mean? Shouldn't we use "Resume" instead?
    Supervision.Resume
  }



  def startStream()(implicit system: ActorSystem): Future[Done] = {
    val matSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
    implicit val materializer: ActorMaterializer = ActorMaterializer(matSettings)

    source
      .collect { case (_, parkOutEvent: ParkOutEvent) => parkOutEvent }
      .filter { e =>
        println("Park-out event: " + new Date(e.timeStamp) +
          s" (last fix time: ${e.trajectory.map(_.getLast.getTime).map(new Date(_))}, " +
          s"detection time: ${new Date(e.creationTimeStamp)})")

        e.timeStamp >= Importer.EarliestEventTime.getEpochSecond
      }
      .filter(_.trajectory.isDefined)
      .map(_.trajectory.get.asScala.toSeq)
      .map(toSpot)
      .mapAsync(3)(insertSpot)
      .to(Sink.ignore)
      .run()
  }


  private def toSpot(fixes: Seq[Fix]): Spot = {
    val firstFix = fixes.head
    val eventTime = new Date(firstFix.getTime)
    Spot(firstFix.getLatLon.latitude, firstFix.getLatLon.getLongitude, new Date(), eventTime)
  }

  private def insertSpot(spot: Spot): Future[Completed] = {
    mongoCollection.insertOne(spot).toFuture
  }

  def shutDown: Unit = {}
}


object Importer extends App {


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

  val DefaultStreamName = "park-out-events-europe-dev"
  val AwsRegion = "eu-west-1"

  val MaxEventAgeMinutes = 5
  val EarliestEventTime: Instant = Instant.now.minus(MaxEventAgeMinutes, ChronoUnit.MINUTES)


  val streamFactory = KinesisMessageStreamFactory(
    "parking-event-importer-hackaton-" + UUID.randomUUID(),
    Some("donald"),
    AwsRegion
  )

  val parkingEventSource = streamFactory
    .createSource(DefaultStreamName, new ParkingEventDeserializer, streamPosition = Some(Instant.now))

  val importer = new Importer(parkingEventSource, collection)

  implicit val system: ActorSystem = ActorSystem("ParkingImporter")
  implicit val executionContext: ExecutionContext = system.dispatcher

  val done = importer.startStream()



}
