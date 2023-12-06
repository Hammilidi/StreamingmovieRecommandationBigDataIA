from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType, TimestampType
from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from datetime import datetime
import json, logging, os


# Configuration des logs pour Spark
def setup_spark_logging():
    log_directory = "/home/StreamingmovieRecommandationBigDataIA/Logs/Spark_Log_Files"
    os.makedirs(log_directory, exist_ok=True)
    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)
    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("MovieRecommendationStreaming")\
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1") \
    .getOrCreate()

logger = setup_spark_logging()

# Création du stream direct Kafka en utilisant spark.readStream
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movierecommandation") \
    .option("startingOffsets", "earliest") \
    .load()


# Définition du StructType pour le schéma
schema = StructType([
    StructField("userinfo", StructType([
        StructField("userId", StringType(), True),
        StructField("userage", StringType(), True),
        StructField("usergender", StringType(), True)
    ])),
    StructField("movie", StructType([
        StructField("movieId", StringType(), True),
        StructField("title", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("genres", ArrayType(StringType()), True)
    ])),
    StructField("rating", StringType(), True),
    StructField("timestamp", StringType(), True)
])
# Parse Kafka messages and apply schema
parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING)")
df = parsed_stream.withColumn("values", from_json(parsed_stream["value"], schema))

# Access fields within the struct
df = df.select("values.*")

# Transformation des colonnes pour correspondre au format attendu
df = df.select(
    col("userinfo.userId").alias("userId").cast(IntegerType()),
    col("userinfo.userage").alias("age").cast(IntegerType()),
    col("userinfo.usergender").alias("gender"),
    col("movie.movieId").alias("movieId").cast(IntegerType()),
    col("movie.title").alias("title"),
    col("movie.release_date").alias("release_date"),
    col("movie.genres").alias("genres"),
    col("rating").cast(FloatType()),
    col("timestamp")
)

# # Affichage des résultats dans la console
# console_query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# console_query.awaitTermination()



#----------------------------------------ELASTICSEARCH----------------------------------------------------------------

# def init_es_connection(cloud_id, username, password):
#     '''
#     Initialise la connexion à Elasticsearch en utilisant les informations fournies.
    
#     Args:
#     - cloud_id (str): Identifiant du cluster Elastic Cloud.
#     - username (str): Nom d'utilisateur pour l'authentification.
#     - password (str): Mot de passe pour l'authentification.
    
#     Returns:
#     - Elasticsearch: Instance de la connexion Elasticsearch.
#     '''
#     es = Elasticsearch(
#         cloud_id=cloud_id,
#         http_auth=(username, password)
#     )
    
#     return es

# # Remplacez ces valeurs par les informations de votre cluster Elastic Cloud
# cloud_id = "movie_recommandation:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ4OTAxYzljOWNiNTQ0MGJlOWExZDZjZGRiZjFhNGExYSQzMzY1ZGQyNjFjMTA0YzY4OGQwN2QwZTE4M2QwNjI3OA=="
# username = "elastic"
# password = "wBrbOPeiGOMO2G0i4R6BHKAs"

# # Initialisation de la connexion
# es = init_es_connection(cloud_id, username, password)



# Connexion à Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Vérification de la connexion
if es.ping():
    print("Connecté à Elasticsearch !")
else:
    print("Échec de la connexion à Elasticsearch.")

# Création de l'index
index_name = "movierecommendation_index"

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)

def insert_to_elasticsearch(df, epoch_id):
    try:
        # Insérer les données dans Elasticsearch
        df.write.format("org.elasticsearch.spark.sql") \
            .option("es.resource", "movierecommendation_index/_doc") \
            .option("es.nodes.wan.only", "true") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des données dans Elasticsearch : {e}")

# Appliquer la logique d'insertion à chaque batch du stream
df.writeStream \
    .foreachBatch(insert_to_elasticsearch) \
    .start() \
    .awaitTermination()

