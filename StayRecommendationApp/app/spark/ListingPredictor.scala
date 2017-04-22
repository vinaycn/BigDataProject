package spark

import breeze.numerics.sqrt
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  * Created by Vinayon 04/16/17.
  */
class ListingPredictor(val sc: SparkContext) {

  val directory = "/Users/akashnagesh/Desktop/AIRBNB/airbnb/public/sparkInput"
  val numPartitions = 20
  val topTenListings = getRatingFromUser
  val numTraining = getTrainingRating.count()
  val numTest = getTestingRating.count()
  val numValidate = getValidationRating.count()
  //val PATH_TO_SAVED_MODEL = "/Users/akashnagesh/Desktop/AIRBNB/airbnb/public/sparkModel/ALS"

  val ratings = getRatingRDD.map(_.split(",") match { case Array(user, item, rate, price) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
  val listings = getListingRDD.map { str =>
    val data = str.split(",")
    (data(0), data(1))
  }
    .map { case (listing_id, name) => (listing_id.toInt, name) }
  val myRatingsRDD = topTenListings
  val training = ratings.filter { case Rating(listing_id, reviewer_id, rating) => (listing_id * reviewer_id) % 10 <= 1 }.persist
  val test = ratings.filter { case Rating(listing_id, reviewer_id, rating) => (listing_id * reviewer_id) % 10 > 1 }.persist
  //val model = if (new File(PATH_TO_SAVED_MODEL).exists()) MatrixFactorizationModel.load(sc, PATH_TO_SAVED_MODEL)
  //else ALS.train(training.union(myRatingsRDD), 8, 10, 0.01)
  val model = MatrixFactorizationModel.load(sc, "/Users/akashnagesh/Desktop/sparkModel")
  val ListingsSeen = myRatingsRDD.map(x => x.product).collect().toList
  val ListingsNotSeen = listings.filter { case (listing_id, name) => !ListingsSeen.contains(listing_id) }.map(_._1)
  val predictedRates =
    model.predict(test.map { case Rating(user, item, rating) => (user, item) }).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.persist()
  val ratesAndPreds = test.map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }.join(predictedRates)
  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => Math.pow(r1 - r2, 2) / 10 }.mean()
  val RMSE = sqrt(MSE)
  val recommendedListingsId = model.predict(ListingsNotSeen.map { product =>
    (0, product)
  }).map { case Rating(user, listings, rating) => (listings, rating) }
    .sortBy(x => x._2, ascending = false).take(10).map(x => x._1)
  var userPreference = "111"

  def recommendListing(userPreference: String = "111") = getListingRDD.map { str =>
    println("inside tejesh class++++++++++++")
    this.userPreference = userPreference
    val data = str.split(",")
    (data(0).toInt, data(1))
  }.filter { case (listing_id, name) => recommendedListingsId.contains(listing_id) }.collect().toList

  private def getListingRDD: RDD[String] = {

    sc.textFile(directory + "/name_listings.csv")
  }

  //model.save(airbnbRecommendation.sc, "/Users/akashnagesh/Desktop/sparkModel")

  private def getRatingFromUser: RDD[Rating] = {
    val listOFRating = for {
      topThree <- getTopThreeListings
      up <- userPreference.toCharArray
    } yield Rating(0, topThree._1, up.toLong)
    sc.parallelize(listOFRating)
  }

  private def getTopThreeListings: List[(Int, String)] = {

    val top20ListingIDs = getRDDOfRating.map { rating => rating._2.product }
      .countByValue()
      .toList
      .sortBy(-_._2)
      .take(10)
      .map { ratingData => ratingData._1 }

    top20ListingIDs.filter(id => getListingMap.contains(id))
      .map { listing_id => (listing_id, getListingMap.getOrElse(listing_id, "Not Found")) }
      .sorted
      .take(3)
  }

  private def getListingMap: Map[Int, String] = {

    getListingRDD.map { line =>
      val fields = line.split(",")

      (fields(0).toInt, fields(1))
    }.collect().toMap
  }

  private def getTrainingRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 < 7)
      .values
      .union(topTenListings)
      .repartition(numPartitions)
      .persist()
  }

  private def getValidationRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 >= 7 && data._1 <= 10)
      .values
      .union(topTenListings)
      .repartition(numPartitions)
      .persist()
  }

  println("Mean Squared Error = " + MSE)

  private def getRDDOfRating: RDD[(Long, Rating)] = {

    getRatingRDD.map { line =>
      val fields = line.split(",")

      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
  }

  println("Root Mean Squared Error = " + RMSE)

  private def getRatingRDD: RDD[String] = {

    sc.textFile(directory + "/training_with_price.csv")
  }

  private def getTestingRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 > 6)
      .values
      .union(topTenListings)
      .repartition(numPartitions)
      .persist()
  }

}
