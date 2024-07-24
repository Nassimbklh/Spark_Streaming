import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{count, input_file_name, col, desc, max, current_timestamp}
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties

object StructuredStreamingApp extends App {

  // Chargement des configurations depuis le fichier application.conf
  val configFilePath = "src/main/resources/application.conf"
  val conf = ConfigFactory.parseFile(new java.io.File(configFilePath))

  val appName = conf.getString("app.name")
  val master = conf.getString("app.master")
  val inputDirectory = conf.getString("app.inputDirectory")
  val checkpointLocation = conf.getString("app.checkpointLocation")
  val outputDirectory = conf.getString("app.outputDirectory")

  // Configuration pour la connexion PostgreSQL
  val jdbcUrl = "jdbc:postgresql://localhost:8070/postgres"

  // Propriétés de connexion JDBC
  val jdbcProperties = new Properties()
  jdbcProperties.put("user", "postgres")
  jdbcProperties.put("password", "Nassim")
  jdbcProperties.put("driver", "org.postgresql.Driver")

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

  // Lecture des fichiers CSV en mode streaming
  val df = spark
    .readStream
    .option("header", "true")
    .option("sep", ",")
    .option("maxFilesPerTrigger", 1)
    .schema(flightDataSchema)
    .csv(inputDirectory)
    .withColumn("filename", input_file_name())
    .filter(col("on_ground") === 0) // Filtrer les vols en l'air


  // Nouvelle agrégation pour le nombre total de vols par id
  val totalFlightsByIdDF = df
    .groupBy("id")
    .agg(count("id").as("total_flights"))

  // Top 5 des fabricants
  val topManufacturersDF = df
    .groupBy("manufacturer")
    .agg(count("manufacturer").as("count"))

  // Top 5 des compagnies aériennes
  val topAirlinesDF = df
    .groupBy("airline_name")
    .agg(count("airline_name").as("count"))

  // Top 5 des types d'avions
  val topAircraftDF = df
    .groupBy("aircraft_name")
    .agg(count("aircraft_name").as("count"))

  // Top 5 des aéroports d'origine les plus fréquentés
  val topOriginAirportsDF = df
    .groupBy("origin_airport_name")
    .agg(count("origin_airport_name").as("count"))

  // Top 5 des aéroports de destination les plus fréquentés
  val topDestinationAirportsDF = df
    .groupBy("destination_airport_name")
    .agg(count("destination_airport_name").as("count"))

  // Top 5 des routes aériennes les plus fréquentées
  val topRoutesDF = df
    .groupBy("origin_airport_name", "destination_airport_name")
    .agg(count("*").as("count"))

  // la vitesse au sol la plus élevée
  val topGroundSpeedDF = df
    .select("aircraft_name", "ground_speed", "destination_airport_coordinates")
    .groupBy("aircraft_name")
    .agg(
      max("ground_speed").as("max_ground_speed")
    )

  // Nouvelle agrégation pour les id distincts avec les informations souhaitées
  val distinctIdsDF = df
    .select("id", "manufacturer", "aircraft_name", "airline_name")
    .distinct()

  // Fonction pour écrire chaque micro-batch dans PostgreSQL
  def writeBatchToPostgres(batchDF: DataFrame, batchId: Long, tableName: String): Unit = {
    batchDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, tableName, jdbcProperties)
  }

  // Fonction pour écrire chaque micro-batch dans un fichier CSV
  def writeBatchToFile(batchDF: DataFrame, batchId: Long, path: String): Unit = {
    batchDF.write
      .option("header", "true")
      .option("sep", ",")
      .mode(SaveMode.Overwrite)
      .csv(s"$path/$batchId")
  }

  // Utilisation de foreachBatch pour écrire les résultats agrégés dans PostgreSQL et fichiers

  val totalFlightsQuery = totalFlightsByIdDF.writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "total_flights_by_id")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/total_flights_by_id")
    }
    .option("checkpointLocation", s"$checkpointLocation/total_flights_by_id")
    .start()

  val manuQuery = topManufacturersDF.writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "top_manufacturers")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/manufacturers")
    }
    .option("checkpointLocation", s"$checkpointLocation/manufacturers")
    .start()

  val airlinesQuery = topAirlinesDF.writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "top_airlines")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/airlines")
    }
    .option("checkpointLocation", s"$checkpointLocation/airlines")
    .start()

  val aircraftQuery = topAircraftDF.writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "top_aircrafts")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/aircrafts")
    }
    .option("checkpointLocation", s"$checkpointLocation/aircrafts")
    .start()

  val originAirportsQuery = topOriginAirportsDF.writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "top_origin_airports")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/origin_airports")
    }
    .option("checkpointLocation", s"$checkpointLocation/origin_airports")
    .start()

  val destinationAirportsQuery = topDestinationAirportsDF.writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "top_destination_airports")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/destination_airports")
    }
    .option("checkpointLocation", s"$checkpointLocation/destination_airports")
    .start()

  val routesQuery = topRoutesDF.writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "top_routes")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/routes")
    }
    .option("checkpointLocation", s"$checkpointLocation/routes")
    .start()

  val groundSpeedQuery = topGroundSpeedDF.writeStream
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "top_ground_speeds")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/ground_speeds")
    }
    .option("checkpointLocation", s"$checkpointLocation/ground_speeds")
    .start()

  val distinctIdsQuery = distinctIdsDF.writeStream
    .outputMode("append")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatchToPostgres(batchDF, batchId, "distinct_ids")
      writeBatchToFile(batchDF, batchId, s"$outputDirectory/distinct_ids")
    }
    .option("checkpointLocation", s"$checkpointLocation/distinct_ids")
    .start()

  // Attente de la terminaison des requêtes de streaming
  totalFlightsQuery.awaitTermination()
  manuQuery.awaitTermination()
  airlinesQuery.awaitTermination()
  aircraftQuery.awaitTermination()
  originAirportsQuery.awaitTermination()
  destinationAirportsQuery.awaitTermination()
  routesQuery.awaitTermination()
  groundSpeedQuery.awaitTermination()
  distinctIdsQuery.awaitTermination()
}
