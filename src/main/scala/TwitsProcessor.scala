import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}

import java.sql.Timestamp

object TwitsProcessor {

  case class Tweet(user_name: String,
                   user_location: String,
                   user_description: String,
                   user_created: Timestamp,
                   user_followers: Option[Int],
                   user_friends: Option[Int],
                   user_favourites: Option[Int],
                   user_verified: Boolean,
                   date: Timestamp,
                   text: String,
                   hashtags: String,
                   source: String,
                   is_Retweet: Boolean)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("twits_processor")
      .master("local[*]")
      .getOrCreate()

    val caseClassSchema = Encoders.product[Tweet].schema

    import spark.implicits._
    val df = spark.read
      .schema(caseClassSchema)
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("/home/dawid/Documents/studia/praca_licencjacka/dane/bitcoin_tweets/Bitcoin_tweets.csv")
      .as[Tweet]

    df.printSchema()
    //df.show()

    df.createOrReplaceTempView("tweets")
    val tweetsDf = spark.sql("Select * from tweets where user_name = 'DeSota Wilson' order by date limit 1")
    val tweets = tweetsDf.as[Tweet].collect()

    tweets.foreach(x => println(SentimentAnalyzer.mainSentiment(x.text)))
    tweets.foreach(x => println(x.text))
    spark.stop()
  }
}

