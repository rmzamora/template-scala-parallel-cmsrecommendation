package org.template.cmsrecommendation

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Article)
    val articleRDD: RDD[(String, Article)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "article"
    )(sc).map { case (entityId, properties) =>
      val article = try {
        // Assume categories is optional property of article.
        Article(categories = properties.getOpt[List[String]]("categories"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" article ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, article)
    }.cache()

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view", "like", "share", "rate")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("article")))(sc)
      .cache()

    val viewEventsRDD: RDD[ViewEvent] = eventsRDD
      .filter { event => event.event == "view" }
      .map { event =>
        try {
          ViewEvent(
            user = event.entityId,
            article = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val likeEventsRDD: RDD[LikeEvent] = eventsRDD
      .filter { event => event.event == "like" }
      .map { event =>
        try {
          LikeEvent(
            user = event.entityId,
            article = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to LikeEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val shareEventsRDD: RDD[ShareEvent] = eventsRDD
      .filter { event => event.event == "share" }
      .map { event =>
      try {
        ShareEvent(
          user = event.entityId,
          article = event.targetEntityId.get,
          t = event.eventTime.getMillis
        )
      } catch {
        case e: Exception =>
          logger.error(s"Cannot convert ${event} to ShareEvent." +
            s" Exception: ${e}.")
          throw e
      }
    }

    val rateEventsRDD: RDD[RateEvent] = eventsRDD
      .filter { event => event.event == "rate" }
      .map { event =>
      try {
        RateEvent(
          user = event.entityId,
          article = event.targetEntityId.get,
          t = event.eventTime.getMillis
        )
      } catch {
        case e: Exception =>
          logger.error(s"Cannot convert ${event} to RateEvent." +
            s" Exception: ${e}.")
          throw e
      }
    }

    new TrainingData(
      users = usersRDD,
      articles = articleRDD,
      viewEvents = viewEventsRDD,
      likeEvents = likeEventsRDD,
      shareEvents = shareEventsRDD,
      rateEvents = rateEventsRDD
    )
  }
}

case class User()

case class Article(categories: Option[List[String]])

case class ViewEvent(user: String, article: String, t: Long)

case class LikeEvent(user: String, article: String, t: Long)

case class ShareEvent(user: String, article: String, t: Long)

case class RateEvent(user: String, article: String, t: Long)

class TrainingData(
  val users: RDD[(String, User)],
  val articles: RDD[(String, Article)],
  val viewEvents: RDD[ViewEvent],
  val likeEvents: RDD[LikeEvent],
  val shareEvents: RDD[ShareEvent],
  val rateEvents: RDD[RateEvent]

) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
    s"articles: [${articles.count()} (${articles.take(2).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)" +
    s"likeEvents: [${likeEvents.count()}] (${likeEvents.take(2).toList}...)" +
    s"shareEvents: [${shareEvents.count()}] (${shareEvents.take(2).toList}...)" +
    s"rateEvents: [${rateEvents.count()}] (${rateEvents.take(2).toList}...)"

  }
}
