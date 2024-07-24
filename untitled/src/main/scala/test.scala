import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{count, input_file_name, col, desc}

object StructuredSt extends App {

  // Chargement des configurations depuis le fichier application.conf
  val configFilePath = "src/main/resources/application.conf"
  val conf = ConfigFactory.parseFile(new java.io.File(configFilePath))

  val appName = conf.getString("app.name")
  val master = conf.getString("app.master")
  val inputDirectory = conf.getString("app.inputDirectory")
  val checkpointLocation = conf.getString("app.checkpointLocation")
  val outputDirectory = conf.getString("app.outputDirectory")

  // Initialisation de la session Spark avec la configuration spécifique
  val spark = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  // Définition du schéma des données pour les fichiers CSV
  val flightDataSchema = StructType(Array(
    StructField("timeline", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true),
    StructField("id", StringType, true),
    StructField("icao_24bit", StringType, true),
    StructField("heading", IntegerType, true),
    StructField("altitude", IntegerType, true),
    StructField("ground_speed", IntegerType, true),
    StructField("aircraft_code", StringType, true),
    StructField("registration", StringType, true),
    StructField("time", LongType, true),
    StructField("origin_airport_iata", StringType, true),
    StructField("destination_airport_iata", StringType, true),
    StructField("number", StringType, true),
    StructField("airline_iata", StringType, true),
    StructField("on_ground", IntegerType, true),
    StructField("vertical_speed", IntegerType, true),
    StructField("callsign", StringType, true),
    StructField("airline_icao", StringType, true),
    StructField("origin_airport_name", StringType, true),
    StructField("origin_airport_coordinates", StringType, true),
    StructField("destination_airport_name", StringType, true),
    StructField("destination_airport_coordinates", StringType, true),
    StructField("airline_name", StringType, true),
    StructField("aircraft_name", StringType, true),
    StructField("manufacturer", StringType, true)
  ))

  // Configuration de la lecture des fichiers CSV en mode streaming
  val df = spark
    .readStream
    .option("header", "true") // Indique que les fichiers CSV contiennent une ligne d'en-tête
    .option("sep", ",") // Délimiteur utilisé dans les fichiers CSV
    .option("maxFilesPerTrigger", 1) // Nombre de fichiers à lire à chaque déclenchement du streaming
    .schema(flightDataSchema) // Utilisation du schéma défini précédemment pour les données
    .csv(inputDirectory) // Chemin vers les fichiers CSV
    .withColumn("filename", input_file_name()) // Ajout d'une colonne contenant le nom du fichier source
    .filter(col("on_ground") === 0) // Filtrer les données pour ne conserver que les vols en cours


  // Agrégations pour les Top 5

  val top5DF = df.limit(5)

  // Nombre d'avions en vol
  val totalFlightsDF = df
    .agg(count("aircraft_name").as("total_flights"))

  // Top 5 des compagnies aériennes
  val topAirlinesDF = df
    .groupBy("airline_name")
    .agg(count("airline_name").as("count"))
    .orderBy(desc("count"))
    .limit(5)

  // Top 5 des types d'avions
  val topAircraftDF = df
    .groupBy("aircraft_name")
    .agg(count("aircraft_name").as("count"))
    .orderBy(desc("count"))
    .limit(5)

  // Top 5 des aéroports d'origine les plus fréquentés
  val topOriginAirportsDF = df
    .groupBy("origin_airport_name")
    .agg(count("origin_airport_name").as("count"))
    .orderBy(desc("count"))
    .limit(5)

  // Top 5 des aéroports de destination les plus fréquentés
  val topDestinationAirportsDF = df
    .groupBy("destination_airport_name")
    .agg(count("destination_airport_name").as("count"))
    .orderBy(desc("count"))
    .limit(5)

  // Fonction pour écrire chaque micro-batch
  //  def writeBatch(batchDF: DataFrame, batchId: Long): Unit = {
  //    batchDF.coalesce(1)
  //      .write
  //      .mode("append")
  //      .csv(s"$outputDirectory/batch-$batchId")
  //  }

  // Fonction pour écrire chaque micro-batch dans un fichier CSV
  def writeBatchToFile(batchDF: DataFrame, batchId: Long, path: String): Unit = {
    batchDF.write
      .option("header", "true")
      .option("sep", ",")
      .mode(SaveMode.Append)
      .csv(s"$path/$batchId")
  }


  val query = top5DF.writeStream
    .outputMode("append")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/top5DF")
    }
    .option("checkpointLocation", checkpointLocation)
    .start()

  val query2 = totalFlightsDF.writeStream
    .outputMode("complete")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/total_flights")
    }
    .option("checkpointLocation", checkpointLocation)
    .start()

  val query3 = topAirlinesDF.writeStream
    .outputMode("complete")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/top_airlines")
    }
    .option("checkpointLocation", checkpointLocation)
    .start()

  val query4 = topAircraftDF.writeStream
    .outputMode("complete")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/top_aircraft")
    }
    .option("checkpointLocation", checkpointLocation)
    .start()

  val query5 = topOriginAirportsDF.writeStream
    .outputMode("complete")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/top_origin_airports")
    }
    .option("checkpointLocation", checkpointLocation)
    .start()

  val query6 = topDestinationAirportsDF.writeStream
    .outputMode("complete")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/top_destination_airports")
    }
    .option("checkpointLocation", checkpointLocation)
    .start()


  query.awaitTermination()
  query2.awaitTermination()
  query3.awaitTermination()
  query4.awaitTermination()
  query5.awaitTermination()
  query6.awaitTermination()
}