package com.tomtom.eventimporter

import java.util.Date

import org.mongodb.scala.bson.ObjectId


case class Spot(_id: ObjectId, latitude: Double, longitude: Double, source: String)

object Spot {
  def apply(latitude: Double, longitude: Double, source: String): Spot =
    new Spot(new ObjectId(), latitude, longitude, source)
}

