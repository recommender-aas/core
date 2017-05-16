package gr.ml.analytics.online

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.online.cassandra.{OnlineCassandraStorage, Similarity, SimilarityIndex, User}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.math.{Ordering, min, sqrt}

class ItemItemRecommender(storage: OnlineCassandraStorage) extends LazyLogging {

  def learn(event: Interaction): Future[_] = {
    val userId = event.userId
    val itemId = event.itemId
    val weight = event.weight
    val user = storage.users.getById(userId)

    user.flatMap {
      case Some(u) =>
        val currentWeight: Double = u.items.getOrElse(itemId, 0)
        if (currentWeight < weight) {
          Future.sequence(
            Seq(
              storage.users.updateUser(User(u.id, u.items + (itemId -> weight))),
              recalculateSimilarity(u, itemId, currentWeight, weight)
            )
          )
        } else
          Future()

      case None =>
        saveNewUser(userId, itemId, weight)
    }

  }

  private def recalculateSimilarity(user: User, itemId: String, currentWeight: Double, newWeight: Double): Future[_] = {
    logger.info("RECALCULATING SIMILARITY")
    val currentItemCount = storage.itemCounts.getById(itemId)

    def callback(currentItemCount: Double): Future[_] = {
      val itemCountDelta = newWeight - currentWeight
      val newItemCount = currentItemCount + itemCountDelta

      val f = updateItemCount(itemId, itemCountDelta)
      val futures = for {
        anotherItem <- user.items if anotherItem._1 != itemId
      } yield {
        updatePair(itemId, currentWeight, newWeight, newItemCount, anotherItem)
      }

      val fSeq: Seq[Future[Any]] = futures.toSeq :+ f
      Future.sequence(fSeq)
    }

    currentItemCount.flatMap {
      case Some(item) => callback(item.count)
      case None => callback(0)
    }
  }


  private def updateItemCount(itemId: String, deltaWeight: Double): Future[_] = {
    storage.itemCounts.incrementCount(itemId, deltaWeight)
  }

  private def updatePair(eventItemId: String, currentItemWeight: Double, newItemWeight: Double,
                         newItemCount: Double, anotherItem: (String, Double)): Future[_] = {
    val anotherItemId = anotherItem._1
    val anotherItemWeight = anotherItem._2
    updatePairCount(eventItemId, currentItemWeight, newItemWeight, newItemCount, anotherItemId, anotherItemWeight)
  }

  private def getPairId(firstItemId: String, secondItemId: String): String =
    Ordering[String].min(firstItemId, secondItemId) + "_" + Ordering[String].max(firstItemId, secondItemId)

  private def updatePairCount(eventItemId: String, currentItemWeight: Double, newItemWeight: Double,
                              newItemCount: Double, anotherItemId: String, anotherItemWeight: Double): Future[_] = {

    val deltaCoRating = {
      if (currentItemWeight == 0) min(newItemWeight, anotherItemWeight)
      else {
        (currentItemWeight < anotherItemWeight, newItemWeight < anotherItemWeight) match {
          case (true, true) => newItemWeight - currentItemWeight
          case (true, false) => anotherItemWeight - currentItemWeight
          case (false, _) => 0
        }
      }
    }

    val pairId = getPairId(eventItemId, anotherItemId)

    def callback(initCount: Double): Future[_] = {
      if (deltaCoRating != 0) {
        Future.sequence(
          Seq(
            storage.pairCounts.incrementCount(pairId, deltaCoRating),
            updateSimilarity(eventItemId, newItemCount, anotherItemId, initCount + deltaCoRating)
          )
        )
      } else {
        updateSimilarity(eventItemId, newItemCount, anotherItemId, initCount + deltaCoRating)
      }
    }

    storage.pairCounts.getById(pairId).flatMap {
      case Some(count) => callback(count.count)
      case None => callback(0)
    }
  }


  private def updateSimilarity(firstItem: String, newItemCount: Double, secondItem: String, pairCount: Double): Future[_] = {
    logger.info("PAIRCOUNT: " + pairCount + ", NEWITEMCOUNT: " + newItemCount)

    storage.itemCounts.getById(secondItem).flatMap {
      case Some(item) =>
        val similarity: Double = pairCount / (sqrt(newItemCount) * sqrt(item.count))
        saveSimilarity(firstItem, item.itemId, similarity)
    }
  }

  private def saveSimilarity(firstItem: String, secondItem: String, similarity: Double): Future[_] = {
    println("SAVING SIMILARITY")
    val pairId = getPairId(firstItem, secondItem)

    storage.similaritiesIndex.getById(pairId).flatMap {
      case Some(currentSimilarity) =>
        Future.sequence(
          Seq(
            storage.similarities.updateSimilarity(Similarity(firstItem, secondItem, similarity)),
            storage.similarities.updateSimilarity(Similarity(secondItem, firstItem, similarity)),
            storage.similaritiesIndex.updateSimilarity(SimilarityIndex(pairId, similarity))))

      case None =>
        Future.sequence(
          Seq(
            storage.similarities.store(Similarity(firstItem, secondItem, similarity)),
            storage.similarities.store(Similarity(secondItem, firstItem, similarity)),
            storage.similaritiesIndex.store(SimilarityIndex(pairId, similarity))))
    }
  }

  private def saveNewUser(userId: String, itemId: String, weight: Double): Future[_] = {
    logger.info(s"SAVING NEW USER $userId")
    val f1 = storage.users.store(User(userId, Map(itemId -> weight)))
    val f2 = updateItemCount(itemId, weight)
    Future.sequence(Seq(f1, f2))
  }

  def getSimilarItems(itemId: String, limit: Int = 10): Future[Seq[Similarity]] = {
    val similarities = storage.similarities.getById(itemId, limit)
    similarities.map(simList => Similarity(itemId, itemId, 1) +: simList)
  }

  def getRecommendations(userId: String, limit: Int = 10): Future[Seq[(String, Double)]] = {

    type UserItem = (String, Double)

    def getUserItems(user: Option[User]): Seq[UserItem] = user match {
      case Some(u) => u.items.toList
      case None => Nil
    }

    def getItemSimilarity(userItems: Seq[UserItem]): Seq[(UserItem, Future[Seq[Similarity]])] = {
      userItems.map(userItem => (userItem, storage.similarities.getById(userItem._1, limit)))
    }

    def reformatSimilarity(similarities: Seq[(UserItem, Future[Seq[Similarity]])]): Future[Seq[(UserItem, Seq[Similarity])]] = {
      val (items, sims) = similarities.unzip
      val sims1 = Future.sequence(sims)
      sims1.map(x => items.zip(x))
    }

    def getSimilaritySummands(similarities: Seq[(UserItem, Seq[Similarity])]): Seq[(String, Double, Double)] = {
      for {
        (item, sims) <- similarities
        sim <- sims
      } yield (sim.anotherItemId, sim.similarity * item._2, sim.similarity)
    }

    def getSimilaritySum(ratingsRaw: Seq[(String, Double, Double)]): Seq[(String, Double)] = {
      ratingsRaw.groupBy(_._1).map {
        case (x, y) => (x, y.map(_._2).sum / y.map(_._3).sum)
      }.toSeq
    }

    for {
      items <- storage.users.getById(userId).map(getUserItems)
      recommendations <- reformatSimilarity(getItemSimilarity(items)).map(getSimilaritySummands).map(getSimilaritySum)
    } yield recommendations.filter(rec => !items.map(_._1).contains(rec._1)).sortWith(_._2 > _._2)
  }


}