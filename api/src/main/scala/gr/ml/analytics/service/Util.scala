package gr.ml.analytics.service

import gr.ml.analytics.domain.Item

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

object Util {

  def schemaToString(schemaMap: Map[String, Any]): String = JSONObject(
    schemaMap.mapValues {
      case mp: Map[String, Any] => JSONObject(mp)
      case lm: List[Map[String, Any]] => JSONArray(lm.map(JSONObject))
      case x => x
    }
  ).toString

  def schemaToMap(schemaString: String): Map[String, Any] = {
    val json = JSON.parseFull(schemaString)
    json match {
      case Some(schema: Map[String, Any]) =>
        require(schema.contains("id"))
        require(schema("id").asInstanceOf[Map[String, Any]].contains("name"))
        require(schema("id").asInstanceOf[Map[String, Any]].contains("type"))
        schema
      case None => throw new RuntimeException("schema validation error")
    }
  }

  def convertJson(itemString: String): Map[String, Any] = {
    val json = JSON.parseFull(itemString)
    json match {
      case Some(item: Map[String, Any]) => item
      case None => throw new RuntimeException("item validation error")
    }
  }

  def extractIdMetadata(schemaMap: Map[String, Any]): (String, String) = {
    val idName = schemaMap("id").asInstanceOf[Map[String, String]]("name")
    val idType = schemaMap("id").asInstanceOf[Map[String, String]]("type")
    (idName, idType)
  }

  def extractFeaturesMetadata(schemaMap: Map[String, Any]): List[Map[String, String]] = {
    schemaMap.get("features") match {
      case None => Nil
      case Some(featuresList: List[Map[String, String]]) => featuresList
      case Some(_) => throw new RuntimeException("Wrong features type")
    }
  }

  def itemsTableName(schemaId: Int): String = s"items_$schemaId"

  def getFeaturesValues(schemaMap: Map[String, Any], item: Item): List[Double] ={

    val allowedTypes = Set("double", "float")
    val featureColumnNames = schemaMap("features").asInstanceOf[List[Map[String, String]]]
      .filter((colDescription: Map[String, Any]) => allowedTypes.contains(colDescription("type").asInstanceOf[String].toLowerCase))
      .map(colDescription => colDescription("name"))

    val featureValues: List[Double] = featureColumnNames
      .map(name => item.get(name))
      .map(some => some.get.toString.toDouble)
      .toArray.toList
      featureValues
    }

}