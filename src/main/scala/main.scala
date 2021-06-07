import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window, when}

object main extends App {
  val spark = SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions","5")
    .appName("partie1")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Create DataFrame immo
  val staticDataFrameImmo = spark.read.format("csv")
    .option("header", "true")
    .load("C:/Users/MOI/dev/Spark-Streaming-immo/data/immo/data-spark.csv")

  // Création d'une variable qui contient le shema
  val shemaDataframe = staticDataFrameImmo.schema
  staticDataFrameImmo.printSchema()
  staticDataFrameImmo.show()
  println("-----------------ICI 1 -------------------------")

  // STREAMING
  val streamingDataFrameImmo = spark.readStream
    .schema(shemaDataframe)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .load("C:/Users/MOI/dev/Spark-Streaming-immo/data/immo/")

  println("-----------------ICI 2 -------------------------")

  // Streaming transformation
  val activityCounts = streamingDataFrameImmo.select("*").groupBy("nature_mutation").count()
  println("-----------------ICI 3 -------------------------")
  activityCounts.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("immo") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start
  println("-----------------ICI 4 -------------------------")

  for (i <- 1 to 50) {
    spark.sql("""
      SELECT *
      FROM immo""")
      .show( false)
    Thread.sleep(10000)
  }
}
