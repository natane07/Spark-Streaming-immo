import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Case 2 :
// Moyenne du nombre de vente par mois
//- Nature_mutation == Vente
//- date_mutation split en mois

object transformation_2 extends App{
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
    .withColumn("année", split(col("date_mutation"), "-").getItem(0))
    .withColumn("valeur_fonciere",col("valeur_fonciere").cast("int"))
    .withColumn("mois", split(col("date_mutation"), "-").getItem(1))
    .withColumn("jour", split(col("date_mutation"), "-").getItem(2))
    .filter(col("nature_mutation") === "Vente")
    .select(col("id_mutation"),col("mois"),col("valeur_fonciere"))
    .distinct
    .groupBy("mois")
    .agg(sum("valeur_fonciere").alias("Sommes ventes"),
      avg("valeur_fonciere").alias("Moyennes ventes"))
    .select(col("mois"),round(col("Moyennes ventes"),2).alias("Moyennes des ventes"),col("Sommes ventes"))
    .orderBy("mois")

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
