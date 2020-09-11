import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // Environment:
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    // val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // Read data:
    val text = sc.textFile("C:\\Users\\zailinyuan\\IdeaProjects\\WordCount\\File\\pg2542.txt");

    // println(text.first().getClass().getSimpleName())

    // Clean data:
    val words = text.flatMap(line => line.split("\\W+")).filter(x => x.matches("\\S+"))
      .collect().mkString(" ").toLowerCase()

    // Get Named Entity: (NER)
    val entities = PretrainedPipeline("explain_document_dl").annotate(words).get("entities")

    // Statistic:
    sc.makeRDD(entities.get).map((_, 1)).reduceByKey(_ + _).sortBy(-_._2).collect().take(20).foreach(println)
  }
}
