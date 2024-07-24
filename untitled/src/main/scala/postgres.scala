import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, input_file_name}
import java.util.Properties

object postgres extends App {
  // Configuration de l'application
  val configFilePath = "src/main/resources/application.conf"
  val conf = ConfigFactory.parseFile(new java.io.File(configFilePath))

  val appName = conf.getString("app.name")
  val master = conf.getString("app.master")
  val inputDirectory = conf.getString("app.inputDirectory")
  val checkpointLocationFlights = conf.getString("app.checkpointLocationFlights")
  val checkpointLocationAircraftNames = conf.getString("app.checkpointLocationAircraftNames")

  // Configuration pour la connexion PostgreSQL
  val jdbcUrl = "jdbc:postgresql://localhost:8070/postgres"

  // Propriétés de connexion JDBC
  val jdbcProperties = new Properties()
  jdbcProperties.put("user", "postgres")
  jdbcProperties.put("password", "Nassim")
  jdbcProperties.put("driver", "org.postgresql.Driver")

  // Initialisation de la session Spark
  val spark = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  // Définition du schéma pour les fichiers CSV à lire
  val flightDataSchema = StructType(Array(
    StructField("timeline", StringType, true),
    StructField("latitude", DecimalType(8,4), true),
    StructField("longitude", DecimalType(8,4), true),
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
    StructField("airline_name", StringType, true)
  ))

  // Lecture des fichiers CSV en mode streaming
  val df = spark.readStream
    .option("header", "true")
    .option("sep", ",")
    .option("maxFilesPerTrigger", 1)
    .schema(flightDataSchema)
    .csv(inputDirectory)
    .withColumn("filename", input_file_name())

  // Imprime le schéma pour assurance
  df.printSchema()

  // Fonction pour écrire chaque micro-batch dans PostgreSQL
  def writeBatch(batchDF: DataFrame, batchId: Long, tableName: String): Unit = {
    // Suppression de la colonne `filename` avant d'écrire les données
    val cleanedDF = batchDF.drop("filename")
    cleanedDF.write
      .mode("append")
      .jdbc(jdbcUrl, tableName, jdbcProperties)
  }

  // Requête pour écrire les résultats dans la table "flights"
  val queryFlights = df.writeStream
    .outputMode("append")  // Utilisez "append" pour ajouter les enregistrements en continu
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeBatch(batchDF, batchId, "flights")
    }
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", checkpointLocationFlights)
    .start()

  // Requête pour écrire les résultats dans la table "AircraftNames"
  val queryAircraftNames = df.writeStream
    .outputMode("append")  // Utilisez "append" pour ajouter les enregistrements en continu
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      val selectedDF = batchDF.select(
        "airline_name",
        "destination_airport_coordinates",
        "destination_airport_name",
        "aircraft_code"
      )
      writeBatch(selectedDF, batchId, "AircraftNames")
    }
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", checkpointLocationAircraftNames)
    .start()

  // Attente de la terminaison des requêtes de streaming
  queryFlights.awaitTermination()
  queryAircraftNames.awaitTermination()
}