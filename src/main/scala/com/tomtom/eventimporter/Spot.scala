package com.tomtom.eventimporter

import java.util.Date

import com.mongodb.client.model.geojson.{Point, Position}
import org.mongodb.scala.bson.ObjectId


case class Spot(_id: ObjectId, location: Point, reportingTime: Date, source: String)

object Spot {
  def apply(latitude: Double, longitude: Double, reportingTime: Date): Spot = {
    val pos = new Position(longitude, latitude)
    val point = new Point(pos)
    new Spot(new ObjectId(), point, reportingTime, "TomTomLive")
  }
}

