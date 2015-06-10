package org.template.cmsrecommendation

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  user: String,
  num: Int,
  categories: Option[Set[String]],
  whiteList: Option[Set[String]],
  blackList: Option[Set[String]],
  tags: Option[Set[String]]
) extends Serializable

case class PredictedResult(
  articleScores: Array[ArticleScore]
) extends Serializable

case class ArticleScore(
  article: String,
  score: Double
) extends Serializable

object CMSRecommendationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("cms" -> classOf[CMSAlgorithm]),
      classOf[Serving])
  }
}
