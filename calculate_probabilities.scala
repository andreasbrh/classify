import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
class calculate_probabilities(dataset : RDD[String], inputTweet : String,outputPath: String) extends Serializable {

  // Global values

  val inputTweetWords: Set[String] = inputTweet.toLowerCase.split(" ").toSet
  val splittedDataSet = dataset.map(_.split("\t"))


  def mainTransformFunction(): RDD[String] = {
    // Function that finds the number of unique tweets from each location, used later
    val countDistinctSetRDD: RDD[(String, Int)] = countDistinctTweetsFromEachPlace()

    // Filter away unwanted tweets
    val placesAndTweetsRDD: RDD[(String, Array[String])] = splittedDataSet
      // Extract location and tweet data
    .map(col => (col(4),col(10)))
      // Make tweets to lower case
      .mapValues(tweet => tweet.toLowerCase)
      .mapValues(tweet => tweet.split(" "))

    // --------------- //  Filter out words from training set that aren't in input tweet // -----------
    val filteredAndCombinedRDD: RDD[(String, Float)] = placesAndTweetsRDD
      .mapValues(filterRelevantWords)

      //  Combines each row of tweets from same place into one
      .flatMapValues(word => word.toSet)
      .groupByKey()

      // Convert to a tuple, and make the list of words into an actual list that is easy to manipulate
      .map(word => (word._1,word._2.toList)) // ._1 is place name, ._2 is a list of relevant words

      // Map the tweets into tuples that are of the form (word,occurences) for each word
      .map(col => (col._1,countUniqueWords(col._2)))
      // Join with countDistinctSet that has the number of tweets from each place.
      .join(countDistinctSetRDD) // Now its format (place name, ((relwords,occurences),totalTweetsFromPlaceName))
      // Calculate the probability for each place
      .map(words => (words._1,calculate(words._2._1,words._2._2))) // Format: (PlaceName,Probability)


    //find the maximum values and write to file
    val finishedRDD: RDD[String] = extractMaxProb(filteredAndCombinedRDD)
      .map(_.productIterator.mkString("\t"))
    finishedRDD.repartition(1).saveAsTextFile(outputPath)

    return dataset
  }

  def extractMaxProb(placesAndProbs: RDD[(String, Float)]): RDD[(String,Float)] = {
    // Reduce finds the maximum value, check if it's zero otherwise move on
    val maxPlaceAndValue: (String, Float) = placesAndProbs.reduce((x, y) =>   if(x._2 > y._2) x else y)
    if (maxPlaceAndValue._2 <= 0) {
      return null
    }
    // filters out all valus that arent at least the maximum value (Can't use equals because of float)
    val allMaxValues: RDD[(String, Float)] = placesAndProbs.filter(PlaceAndValue =>  PlaceAndValue._2 >= maxPlaceAndValue._2)

    return allMaxValues
  }

  def countDistinctTweetsFromEachPlace(): RDD[(String,Int)] = {

    // Groups by place name, and finds how many values there are of each
    val countDistinctSet: RDD[(String, Int)] = splittedDataSet
      .map(col => col(4))
      .groupBy(identity)
      .mapValues(_.size)
    return countDistinctSet
  }
  def countUniqueWords(listOfWords: List[String]) : Seq[(String,Int)] = {
    // groups the list of words by each unique word, then uses ._size to find how many occurences of each word
      val wordsList = listOfWords
        .groupBy(identity)
        .mapValues(_.size)
        .toArray
      return wordsList
  }


    def filterRelevantWords(trainingTweet: Array[String]): Array[String] = {
      // finds the intersection between a tweet from training set and the inputtweet
     val filteredTweet = trainingTweet
        .toSet
        .intersect(inputTweetWords)
        .toArray
      //
      return filteredTweet
    }

  def calculate(wordsList: (Seq[(String,Int)]), countForThisPlace: Int) : (Float) = {
    val totalTweets = dataset.count() // Find total tweets
    // Initilize probability outside for-loop
    var probability = countForThisPlace.toFloat/totalTweets
    for ((word,occurences) <- wordsList) {
      // Calculates final probability by occurences of each word A from place B
      probability = probability * occurences/countForThisPlace
    }
    return probability
  }


}
