import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Case 4 : Nombre de vente par region

object transformation_4 extends App{
  val spark = SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions","5")
    .appName("partie1")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.streaming.schemaInference", true)

  // Create DataFrame immo
  var staticDataFrameImmo = spark.read.format("csv")
    .option("header", "true")
    .load("C:/Users/MOI/dev/Spark-Streaming-immo/data/immo/data-spark.csv")

  // Création d'une variable qui contient le shema
  val shemaDataframeImmo = staticDataFrameImmo.schema

  // Create dataframe region
  var staticDataFrameRegion = spark.read.format("csv")
    .option("header", "true")
    .load("C:/Users/MOI/dev/Spark-Streaming-immo/data/departement/departments.csv")

  // Création d'une variable qui contient le shema
  val shemaDataframeRegion = staticDataFrameRegion.schema

  val staticDataFrameJoinShema = staticDataFrameImmo.join(
    staticDataFrameRegion,
    staticDataFrameImmo("code_departement") ===  staticDataFrameRegion("code"),
  ).schema

  // STREAMING IMMO
  var streamingDataFrameImmo = spark.readStream
    .schema(shemaDataframeImmo)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .load("C:/Users/MOI/dev/Spark-Streaming-immo/data/immo-split/*.csv")

    // STREAMING Region
  var streamingDataFrameRegion = spark.readStream
    .schema(shemaDataframeRegion)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .load("C:/Users/MOI/dev/Spark-Streaming-immo/data/departement/")

//   Jointure des DF
  val streamSparktest = streamingDataFrameImmo.join(
    streamingDataFrameRegion,
    streamingDataFrameImmo("code_departement") ===  streamingDataFrameRegion("code"),
  )

  // Ecriture des tream dans un fichier tmp
  streamSparktest.writeStream
    .format("csv") // memory = store in-memory table
    .option("path", "C:/Users/MOI/dev/Spark-Streaming-immo/data/stream-file/")
    .option("checkpointLocation","C:/Users/MOI/dev/Spark-Streaming-immo/data/cheackpoint/")
    .outputMode("append") // complete = all the counts should be in the table
    .start

  // Lecture du fichier avec la jointure
  var streamingDataFrameJoin = spark.readStream
    .schema(staticDataFrameJoinShema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .load("C:/Users/MOI/dev/Spark-Streaming-immo/data/stream-file/*.csv")

  // Transformation Group by par region
  val streamSpark = streamingDataFrameJoin.withColumn("timestamp", unix_timestamp(col("date_mutation"), "y-M-d"))
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withWatermark("timestamp", "1 days")
    .groupBy("region_code")
    .count()

  // Write stream
  streamSpark.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("immo_join") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start

  // print des données
  for (i <- 1 to 50) {
    spark.sql("""
      SELECT *
      FROM immo_join""")
      .show( false)
    Thread.sleep(10000)
  }

}
