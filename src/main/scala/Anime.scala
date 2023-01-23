import org.apache.spark.{SparkConf, SparkContext}

import scala.Console.println
import scala.collection.mutable
import scala.math._

object anime_stats extends App {
  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)

  // Récupération des données
  val DATA_PATH = "anime.csv"
  val caract_rdd = sc.textFile(DATA_PATH).map(row => deal_with_quotes(row.replace(';', '!')).split(','))
  val headerRow = caract_rdd.first()
  var caract_rdd_clean = caract_rdd.filter(row => row(0) != headerRow(0))

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
  val col_score_amount = new Array[Int](10);
  for (i <- 0 to 9) {
    col_score_amount(i) = headerRow.indexOf("Score-" + (i+1))
  }

  // Calcul de la médiane
  val rdd_score_median = caract_rdd_clean.map(
    anime => (
      anime(col_anime_id),
      anime(col_name),
      str_to_float(anime(col_score)),
      med(col_score_amount.map(
        i_col => str_to_float(anime(i_col).trim())
      ))
    )
  )





  // Utilisation
  /*
  println("Please enter the id of an anime :")
  val anime_id_input = scala.io.StdIn.readLine()
  println("The median for this anime is:")
  println(rdd_score_median.filter(row => row._1 == anime_id_input).first()._4)
  */

  // On prend les 10 meilleurs moyennes (TODO? pour les animes notés au moins 1000 fois)
  val best_med = rdd_score_median.sortBy(
    (row=>row._3),
    false
  ).take(10)

  println("Top 10 best scores:")
  println(best_med.mkString("\n"))



  /* On regarde l'impact du nombre d'épisodes sur le score. */
  val nb_ep_rdd = caract_rdd_clean.groupBy(row => max(200,str_to_int(row(col_episodes))))

  // array_nbEp_addedScores est un tableau de couples de flottant (nombre d'animes, score cumulé des animés) ;
  // a(0) contiendra un tel couple pour les animés d'entre 0 et 6 épisodes ;
  // a(1) contiendra un tel couple pour les animés d'entre 7 et 15 épisodes ;
  // a(2) contiendra un tel couple pour les animés d'entre 16 et 28 épisodes ;
  // a(3) contiendra un tel couple pour les animés d'entre 29 et 60 épisodes ;
  // a(4) contiendra un tel couple pour les animés de plus de 60 épisodes.
  var array_nbEp_addedScores = new Array[Array[Float]](5)
  for (i <- array_nbEp_addedScores.indices) {
    array_nbEp_addedScores(i) = new Array[Float](2)
    array_nbEp_addedScores(i)(0) = 0
    array_nbEp_addedScores(i)(1) = 0
  }

  def seqOp = (accu: Array[Array[Float]], row: Array[String]) => {
    if(str_to_float(row(col_score)) != 0) { //Si le score est 0, cela signifie que personne n'a voté ; on ne retient donc pas l'animé.
      var i = -1
      val nb_ep = str_to_int(row(col_episodes))
      if (nb_ep < 7) i = 0
      else if (nb_ep < 16) i = 1
      else if (nb_ep < 29) i = 2
      else if (nb_ep < 61) i = 3
      else i = 4
      accu(i)(0) += 1
      accu(i)(1) += str_to_float(row(col_score))
    }
    accu
  }

  def combOp = (accu1:Array[Array[Float]], accu2:Array[Array[Float]]) => {
    for (i <- accu1.indices) {
      accu1(i)(0) += accu2(i)(0)
      accu1(i)(1) += accu2(i)(1)
    }
    accu1
  }

  array_nbEp_addedScores = caract_rdd_clean.aggregate(array_nbEp_addedScores)(seqOp,combOp)

  // J'aurais aimé afficher un graphe, mais je n'arrive pas à importer les dépendances nécessaires.
  // À la place, on affiche quelques lignes.
  for (i<-array_nbEp_addedScores.indices) println("nombre d'animés : " + array_nbEp_addedScores(i)(0) + " | score moyen : " + array_nbEp_addedScores(i)(1)/array_nbEp_addedScores(i)(0))



  /*  On regarde l'impact de la source sur le score.  */
  val dict_source = new mutable.HashMap[String,Array[Float]]()  //Dictionnaire qui contiendra pour chaque source un couple (nombre d'animés, score cumulé des animés)
  caract_rdd_clean.foreach(row => {
    if (str_to_float(row(col_score)) != 0){
      if (!dict_source.contains(row(col_source))) {
        val new_value = new Array[Float](2)
        new_value(0) = 0
        new_value(1) = 0
        dict_source.put(row(col_source), new_value) //Couple (nombre d'animés, score cumulé)
      }
      val v = dict_source.getOrElse(row(col_source), null)
      v(0) += 1
      v(1) += str_to_float(row(col_score))
    }
  })

  for (s<-dict_source.keys){
    val v = dict_source.getOrElse(s, null)
    println(s + " : " + v(0) + " animés | score moyen : " + v(1)/v(0))
  }













  /*  This function takes for an anime an array A st A(i) is the amount of people who put a score of (i+1) for this anime.
      It returns the median score.
  */
  def med(amounts: Array[Float]): Int = {
    val half_amount = amounts.sum / 2

    var current_total_amount = 0.0
    for (i <- 0 to 9) {
      current_total_amount += amounts(i)
      if (current_total_amount > half_amount) {
        return i
      }
    }
    return 0
  };

  //This function takes a String s and replace every substring of the form: "_,_,_" with a new one of the form: _/_/_
  def deal_with_quotes(s: String): String = {
    var res = s

    var i_next_quote = res.indexOf("\"")

    var substr = ""
    var i_end_substr = -1
    while(i_next_quote != -1){
      //We suppose quotes come in pairs ("_")
      substr = res.substring(i_next_quote+1)
      i_end_substr = substr.indexOf("\"")
      substr = substr.substring(0,i_end_substr)

      res = res.substring(0,i_next_quote) +
        substr.replace(",","/") +
        res.substring(i_next_quote + substr.length + 2) // +2 for the quotes

      i_next_quote = res.indexOf("\"")
    }

    return res
  }

  def str_to_float(s: String): Float = {
    try{
      return s.toFloat
    }
    catch {
      case e:NumberFormatException => 0
    }
  }

  def str_to_int(s: String): Int = {
    try {
      return s.toInt
    }
    catch {
      case e: NumberFormatException => 0
    }
  }
}