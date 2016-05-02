/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
    def main(args: Array[String]) {
        val file = "hdfs://pp11:9000/shared/Lab6/googlebooks-1gram-shuffle.txt"
        val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)
        val lines = sc.textFile(file)

        // cache this RDD for action branch
        val nouns = lines.filter(_.contains("_NOUN")).cache

        // Transformations (filter) and Actions (count)
        val num_tion = nouns.filter(_.contains("tion_")).count
        val num_sion = nouns.filter(_.contains("sion_")).count
        val num_ism  = nouns.filter(_.contains("ism_")) .count
        val num_ity  = nouns.filter(_.contains("ity_")) .count
        val num_ness = nouns.filter(_.contains("ness_")).count
        val num_ship = nouns.filter(_.contains("ship_")).count

        // Output
        println("lines of nouns with")
        println("-tion: " + num_tion.toString)
        println("-sion: " + num_tion.toString)
        println("-ism: " + num_ism.toString)
        println("-ity: " + num_ism.toString)
        println("-ness: " + num_ness.toString)
        println("-ship: " + num_ship.toString)
        sc.stop
    }
}
