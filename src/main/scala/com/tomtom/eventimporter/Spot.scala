package com.tomtom.eventimporter

import java.util.Date

import com.mongodb.client.model.geojson.{CoordinateReferenceSystem, NamedCoordinateReferenceSystem, Point, Position}
import org.mongodb.scala.bson.ObjectId


case class Spot(_id: ObjectId, location: Point, reportingTime: Date, source: String)

object Spot {
  def apply(latitude: Double, longitude: Double, reportingTime: Date, eventTime: Date): Spot = {
    val pos = new Position(longitude, latitude)
    val point = new Point(NamedCoordinateReferenceSystem.EPSG_4326, pos)
    new Spot(new ObjectId(), point, reportingTime, "TomTomLive")
  }
}

