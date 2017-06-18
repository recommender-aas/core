package gr.ml.analytics.service

import java.util.UUID
import gr.ml.analytics.cassandra.Schema

import scala.concurrent.Future

/**
  * Schema service exposes interface to manage a schema of a particular set of items.
  */
trait SchemaService {

  def getAll: Future[List[Schema]]

  def get(id: UUID): Future[Option[Schema]]

  def save(schema: Schema): Unit
}
