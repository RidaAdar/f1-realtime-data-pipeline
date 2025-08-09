from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import psycopg2

# Configs
BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_INPUT_TOPIC = "race_results_topic"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/f1stream_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

# Créer les tables dans PostgreSQL si elles n'existent pas
def create_postgres_tables():
    conn = psycopg2.connect(
        dbname="f1stream_db",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host="localhost"
    )
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS race_results (
        grand_prix TEXT,
        date TIMESTAMP,
        driver_number TEXT,
        position INTEGER,
        laps_completed INTEGER,
        dnf BOOLEAN,
        gap_to_leader TEXT,
        meeting_key TEXT,
        session_key TEXT,
        points INTEGER
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS drivers (
        driver_number TEXT PRIMARY KEY,
        driver_name TEXT,
        headshot_url TEXT
    );
    """)

    conn.commit()
    cur.close()
    conn.close()

# Appeler au lancement
create_postgres_tables()

# Spark session
spark = (
    SparkSession.builder
    .appName("F1 Race Results Ingestion + Points")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.postgresql:postgresql:42.6.0")
    .config("spark.ui.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schéma Kafka
schema = StructType([
    StructField("grand_prix", StringType()),
    StructField("date", StringType()),
    StructField("driver_number", StringType()),
    StructField("position", IntegerType()),
    StructField("laps_completed", IntegerType()),
    StructField("dnf", BooleanType()),
    StructField("gap_to_leader", StringType()),
    StructField("meeting_key", StringType()),
    StructField("session_key", StringType())
])

# Lecture depuis Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parsing JSON
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")
# Transformation avant l'insertion
points_df = parsed_df.withColumn(
    "points",
    when(col("position") == 1, 25)
    .when(col("position") == 2, 18)
    .when(col("position") == 3, 15)
    .when(col("position") == 4, 12)
    .when(col("position") == 5, 10)
    .when(col("position") == 6, 8)
    .when(col("position") == 7, 6)
    .when(col("position") == 8, 4)
    .when(col("position") == 9, 2)
    .when(col("position") == 10, 1)
    .otherwise(0)
).withColumn(
    "date", to_timestamp("date")  # 👈 ici tu castes proprement
)

# Ne garder que les données avec une position (course terminée)
race_results_only = points_df.filter(col("position").isNotNull())

# Fonction d’écriture dans Postgres (append)
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "race_results") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Écriture en streaming
query = (
    race_results_only.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "./checkpoint_race_results_pg")
    .start()
)

query.awaitTermination()
