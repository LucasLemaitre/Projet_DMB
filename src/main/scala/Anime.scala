import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util
import scala.Console.println

object anime_stats extends App {
  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)

  // Récupération des données
  val DATA_PATH = "anime.csv"
  val caract_rdd = sc.textFile(DATA_PATH).map(row => row.split(','))
  val headerRow = caract_rdd.first()
  val caract_rdd_clean = caract_rdd.filter(row => row(0) != headerRow(0))

  val col_anime_id = headerRow.indexOf("MAL_ID")
  val col_name = headerRow.indexOf("Name")
  val col_score = headerRow.indexOf("Score")
  val col_genres = headerRow.indexOf("Genres")
  val col_english_name = headerRow.indexOf("English name")
  val col_japanese_name = headerRow.indexOf("Japanese name")
  val col_type = headerRow.indexOf("Type")
  val col_episodes = headerRow.indexOf("Episodes")
  val col_aired = headerRow.indexOf("Aired")
  val col_premiered = headerRow.indexOf("Premiered")
  val col_producers = headerRow.indexOf("Producers")
  val col_licensors = headerRow.indexOf("Licensors")
  val col_studios = headerRow.indexOf("Studios")
  val col_source = headerRow.indexOf("Source")
  val col_duration = headerRow.indexOf("Duration")
  val col_rating = headerRow.indexOf("Rating")
  val col_ranked = headerRow.indexOf("Ranked")
  val col_popularity = headerRow.indexOf("Popularity")
  val col_members = headerRow.indexOf("Members")
  val col_favorites = headerRow.indexOf("Favorites")
  val col_watching = headerRow.indexOf("Watching")
  val col_completed = headerRow.indexOf("Completed")
  val col_on_hold = headerRow.indexOf("On-Hold")
  val col_dropped = headerRow.indexOf("Dropped")
  val col_plan_to_watch = headerRow.indexOf("Plan to Watch")
  val col_score_amount = new Array[Int](11);
  for (i <- 1 to 10) {
    col_score_amount(i) = headerRow.indexOf("Score-" + i)
  }

  // Calcul de la médiane

  val mediane = caract_rdd_clean.map(
    anime => (
      anime(col_anime_id),
      med(col_score_amount.slice(1, 10).map(i_col => anime(i_col).toInt)))
  ).groupByKey().map(stat => (stat._1, stat._2.map(nb => nb.toFloat).sum / stat._2.size, stat._2.size)).
    filter(stat => stat._3 >= 4).
    sortBy(stat => stat._2, ascending = false).
    take(10);

  def med(amounts: Array[Int]): Int = {
    val half_amount = amounts.sum / 2

    var current_total_amount = 0
    for (i <- 0 to 9) {
      current_total_amount += amounts(i)
      if (current_total_amount > half_amount) {
        return i
      }
    }
    return 0
  };

  println(mediane)

}