import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Case 6:  Moyenne des valeurs foncier par Code postal

object transformation_6 extends App{
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


  // STREAMING IMMO
  var streamingDataFrameImmo = spark.readStream
    .schema(shemaDataframeImmo)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .load("C:/Users/MOI/dev/Spark-Streaming-immo/data/immo-split/*.csv")

  // Transformation
  val streamSpark = streamingDataFrameImmo.withColumn("timestamp", unix_timestamp(col("date_mutation"), "y-M-d"))
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withWatermark("timestamp", "1 days")
    .filter(col("type_local").isin("Maison", "Appartement"))
    .select(col("id_mutation"), col("code_postal"), col("valeur_fonciere"))
    .distinct()
    .groupBy("code_postal")
    .agg(avg(col("valeur_fonciere")).alias("valeur_fonciere_avg"))
    .select(col("code_postal"), round(col("valeur_fonciere_avg"), 2).alias("valeur_fonciere_avg"))
    .orderBy(desc("valeur_fonciere_avg"))

  // Write stream
  streamSpark.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("immo_join") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start

  // print
  for (i <- 1 to 100) {
    spark.sql("""
      SELECT *
      FROM immo_join""")
      .show( false)
    Thread.sleep(10000)
  }

}
