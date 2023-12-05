# Importation des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, expr, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType, FloatType, DateType
from elasticsearch import Elasticsearch


# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("movielensApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.0") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Lecture des données depuis Kafka
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movielens-data") \
    .load()

# Définition du schéma pour les données des films
movie_schema = StructType([
    StructField("page", IntegerType(), True),
    StructField("results", ArrayType(
        StructType([
            StructField("adult", BooleanType(), True),
            StructField("backdrop_path", StringType(), True),
            StructField("genre_ids", ArrayType(IntegerType()), True),
            StructField("id", IntegerType(), True),
            StructField("original_language", StringType(), True),
            StructField("original_title", StringType(), True),
            StructField("overview", StringType(), True),
            StructField("popularity", FloatType(), True),
            StructField("poster_path", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("title", StringType(), True),
            StructField("video", BooleanType(), True),
            StructField("vote_average", FloatType(), True),
            StructField("vote_count", IntegerType(), True)
        ])
    ), True)
])

# Parsing des messages Kafka et application du schéma
parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING)")
df = parsed_stream.withColumn("values", from_json(parsed_stream["value"], movie_schema))

# Explosion de l'array "results"
df = df.select("values.page", explode("values.results").alias("movie_data"))

# Accès aux champs dans la structure
df = df.select("page", "movie_data.*")


#*******************************************TRANSFORMATIONS*****************************************************************#

# Combinaison du titre et de l'aperçu pour créer un champ "description"
df = df.withColumn("description", concat_ws(" - ", col("title"), col("overview")))

# Normalisation des champs "vote_average" et "popularity"
df = df.withColumn("normalized_vote_average", col("vote_average") * 10)
df = df.withColumn("normalized_popularity", col("popularity") * 1000)

# Conversion de la date de sortie en DateType
df = df.withColumn("release_date", expr("TO_DATE(release_date, 'yyyy-MM-dd')").cast(DateType()))

# Sélection des colonnes pertinentes pour Elasticsearch
elasticsearch_df = df.select("id", "title", "original_language", "description", "release_date", "normalized_vote_average", "normalized_popularity")

#*******************************************ELASTICSEARCH****************************************************************

# Écriture du DataFrame en streaming vers Elasticsearch
query = elasticsearch_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: write_to_elasticsearch(batch_df)) \
    .start()

# Fonction pour écrire chaque batch dans Elasticsearch
def write_to_elasticsearch(batch_df):
    batch_df.foreachPartition(lambda partition: write_partition_to_es(partition))

def write_partition_to_es(partition):
    # Connexion à Elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    for row in partition:
        doc = row.asDict()
        doc_id = doc.pop('id')  # Utilise 'id' comme ID de document
        es.index(index='movielens_data', doc_type='_doc', id=doc_id, body=doc)

# Attente de la fin de la requête
query.awaitTermination()


# # Écriture du DataFrame en streaming vers Elasticsearch
# query = elasticsearch_df.writeStream \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.nodes", "localhost") \
#     .option("es.port", "9200") \
#     .option("es.resource", "movielens_data") \
#     .option("es.nodes.wan.only", "false") \
#     .option("checkpointLocation", "./checkpoint/location") \
#     .option("es.spark.sql.version", "7.17.0") \
#     .option("es.mapping.id", "id") \
#     .option("es.write.operation", "upsert") \
#     .start()  # start() method initiates the streaming query

# # Attente de la fin de la requête
# query.awaitTermination()


# # Attente de la fin de la requête
# query.awaitTermination()  # awaitTermination() should be called on the 'query' object
