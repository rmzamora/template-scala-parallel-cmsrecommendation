package org.template.cmsrecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import io.prediction.data.storage.BiMap

import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}

class CMSAlgorithmTest
  extends FlatSpec with EngineTestSparkContext with Matchers {

  val algorithmParams = new CMSAlgorithmParams(
    appName = "test-app",
    unseenOnly = true,
    seenEvents = List("like", "view", "share", "rate"),
    similarEvents = List("view"),
    rank = 10,
    numIterations = 20,
    lambda = 0.01,
    seed = Some(3)
  )
  val algorithm = new CMSAlgorithm(algorithmParams)

  val userStringIntMap = BiMap(Map("u0" -> 0, "u1" -> 1))

  val articleStringIntMap = BiMap(Map("i0" -> 0, "i1" -> 1, "i2" -> 2))

  val users = Map("u0" -> User(), "u1" -> User())


  val i0 = Article(categories = Some(List("c0", "c1")), tags = None)
  val i1 = Article(categories = None, tags = Some(List("t1", "t2")))
  val i2 = Article(categories = Some(List("c0", "c2")), tags = Some(List("t0", "t1")))

  val articles = Map(
    "i0" -> i0,
    "i1" -> i1,
    "i2" -> i2
  )

  val view = Seq(
    ViewEvent("u0", "i0", 1000010),
    ViewEvent("u0", "i1", 1000020),
    ViewEvent("u0", "i1", 1000020),
    ViewEvent("u1", "i1", 1000030),
    ViewEvent("u1", "i2", 1000040)
  )

  val like = Seq(
    LikeEvent("u0", "i0", 1000020),
    LikeEvent("u0", "i1", 1000030),
    LikeEvent("u1", "i1", 1000040)
  )

  val share = Seq(
    ShareEvent("u0", "i0", 1000020),
    ShareEvent("u0", "i1", 1000030),
    ShareEvent("u1", "i1", 1000040)
  )

  val rate = Seq(
    RateEvent("u0", "i0", 1000020),
    RateEvent("u0", "i1", 1000030),
    RateEvent("u1", "i1", 1000040)
  )


  "CMSAlgorithm.genMLlibRating()" should "create RDD[MLlibRating] correctly" in {

    val preparedData = new PreparedData(
      users = sc.parallelize(users.toSeq),
      articles = sc.parallelize(articles.toSeq),
      viewEvents = sc.parallelize(view.toSeq),
      likeEvents = sc.parallelize(like.toSeq),
      shareEvents = sc.parallelize(share.toSeq),
      rateEvents = sc.parallelize(rate.toSeq)
    )

    val mllibRatings = algorithm.genMLlibRating(
      userStringIntMap = userStringIntMap,
      articleStringIntMap = articleStringIntMap,
      data = preparedData
    )

    val expected = Seq(
      MLlibRating(0, 0, 1),
      MLlibRating(0, 1, 2),
      MLlibRating(1, 1, 1),
      MLlibRating(1, 2, 1)
    )

    mllibRatings.collect should contain theSameElementsAs expected
  }

  "CMSAlgorithm.trainDefault()" should "return popular count for each item" in {
    val preparedData = new PreparedData(
      users = sc.parallelize(users.toSeq),
      articles = sc.parallelize(articles.toSeq),
      viewEvents = sc.parallelize(view.toSeq),
      likeEvents = sc.parallelize(like.toSeq),
      shareEvents = sc.parallelize(share.toSeq),
      rateEvents = sc.parallelize(rate.toSeq)
    )

    val popCount = algorithm.trainDefault(
      userStringIntMap = userStringIntMap,
      articleStringIntMap = articleStringIntMap,
      data = preparedData
    )

    val expected = Map(0 -> 1, 1 -> 2)

    popCount should contain theSameElementsAs expected
  }

  "CMSAlgorithm.predictKnownuser()" should "return top article" in {

    val top = algorithm.predictKnownUser(
      userFeature = Array(1.0, 2.0, 0.5),
      articleModels = Map(
        0 -> ArticleModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ArticleModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ArticleModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = "u0",
        num = 5,
        categories = Some(Set("c0")),
        whiteList = None,
        blackList = None,
        tags = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((2, 7.5), (0, 5.0))
    top shouldBe expected
  }

  "CMSAlgorithm.predictDefault()" should "return top article" in {

    val top = algorithm.predictDefault(
      articleModels = Map(
        0 -> ArticleModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ArticleModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ArticleModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = "u0",
        num = 5,
        categories = None,
        whiteList = None,
        blackList = None,
        tags = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((1, 4.0), (0, 3.0), (2, 1.0))
    top shouldBe expected
  }

  "CMSAlgorithm.predictSimilar()" should "return top article" in {

    val top = algorithm.predictSimilar(
      recentFeatures = Vector(Array(1.0, 2.0, 0.5), Array(1.0, 0.2, 0.3)),
      articleModels = Map(
        0 -> ArticleModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ArticleModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ArticleModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = "u0",
        num = 5,
        categories = Some(Set("c0")),
        whiteList = None,
        blackList = None,
        tags = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((0, 1.605), (2, 1.525))

    top(0)._1 should be (expected(0)._1)
    top(1)._1 should be (expected(1)._1)
    top(0)._2 should be (expected(0)._2 plusOrMinus 0.001)
    top(1)._2 should be (expected(1)._2 plusOrMinus 0.001)
  }
}
