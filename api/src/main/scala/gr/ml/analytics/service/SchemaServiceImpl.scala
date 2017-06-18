package gr.ml.analytics.service

import java.util.UUID
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.{CassandraStorage, Schema}
import org.json4s.{DefaultFormats, Formats}

import scala.concurrent.Future

class SchemaServiceImpl(inputDatabase: CassandraStorage) extends SchemaService with LazyLogging {

  private lazy val schemasModel = inputDatabase.schemasModel

  implicit val jsonFormats: Formats = DefaultFormats

  override def get(id: UUID): Future[Option[Schema]] = {
    schemasModel.getOne(id)
  }

  override def save(schema: Schema): Unit = {


    schemasModel.save(schema)

    val tableName = Util.itemsTableName(schema.schemaId)

    // create cassandra table for items
    val (pkColumnName, pkColumnType) = Util.extractIdMetadata(schema.jsonSchema)
    val featureColumnsInfo: List[Map[String, String]] = Util.extractFeaturesMetadata(schema.jsonSchema)

    val columnsString: String = featureColumnsInfo
      .map(feature => (feature("name"), feature("type")))
      .foldLeft("") { (s: String, pair: (String, String)) =>
        s + " ," + pair._1 + " " + pair._2
      }

    val q = s"CREATE TABLE ${schemasModel.keySpace}.$tableName ($pkColumnName $pkColumnType PRIMARY KEY $columnsString )"

    logger.info(s"Creating: $q")
    schemasModel.session.execute(q).getColumnDefinitions
  }

  override def getAll: Future[List[Schema]] = {
    schemasModel.getAll
  }
}
