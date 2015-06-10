package org.template.cmsrecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class PreparatorTest
  extends FlatSpec with EngineTestSparkContext with Matchers {

  val preparator = new Preparator()
  val users = Map(
    "u0" -> User(),
    "u1" -> User()
  )

  val articles = Map(
    "i0" -> Article(categories = Some(List("c0", "c1"))),
    "i1" -> Article(categories = None)
  )

  val view = Seq(
    ViewEvent("u0", "i0", 1000010),
    ViewEvent("u0", "i1", 1000020),
    ViewEvent("u1", "i1", 1000030)
  )

  val share = Seq(
    ShareEvent("u0", "i0", 1000020),
    ShareEvent("u0", "i1", 1000030),
    ShareEvent("u1", "i1", 1000040)
  )

  val like = Seq(
    LikeEvent("u0", "i0", 1000020),
    LikeEvent("u0", "i1", 1000030),
    LikeEvent("u1", "i1", 1000040)
  )

  val rate = Seq(
    RateEvent("u0", "i0", 1000020),
    RateEvent("u0", "i1", 1000030),
    RateEvent("u1", "i1", 1000040)
  )

  // simple test for demonstration purpose
  "Preparator" should "prepare PreparedData" in {

    val trainingData = new TrainingData(
      users = sc.parallelize(users.toSeq),
      articles = sc.parallelize(articles.toSeq),
      viewEvents = sc.parallelize(view.toSeq),
      likeEvents = sc.parallelize(like.toSeq),
      shareEvents = sc.parallelize(share.toSeq),
      rateEvents = sc.parallelize(rate.toSeq)

    )

    val preparedData = preparator.prepare(sc, trainingData)

    preparedData.users.collect should contain theSameElementsAs users
    preparedData.articles.collect should contain theSameElementsAs articles
    preparedData.viewEvents.collect should contain theSameElementsAs view
    preparedData.likeEvents.collect should contain theSameElementsAs like
    preparedData.shareEvents.collect should contain theSameElementsAs share
    preparedData.rateEvents.collect should contain theSameElementsAs rate
  }
}
