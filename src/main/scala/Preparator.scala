package org.template.cmsrecommendation

import io.prediction.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      users = trainingData.users,
      articles = trainingData.articles,
      viewEvents = trainingData.viewEvents,
      likeEvents = trainingData.likeEvents,
      shareEvents = trainingData.shareEvents,
      rateEvents = trainingData.rateEvents)
  }
}

class PreparedData(
  val users: RDD[(String, User)],
  val articles: RDD[(String, Article)],
  val viewEvents: RDD[ViewEvent],
  val likeEvents: RDD[LikeEvent],
  val shareEvents: RDD[ShareEvent],
  val rateEvents: RDD[RateEvent]
) extends Serializable
