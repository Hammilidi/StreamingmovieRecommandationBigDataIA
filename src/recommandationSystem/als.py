from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Créer une session Spark
spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()

# Chargement des données d'évaluation des films (user, movie, rating)
data = spark.read.csv("path/to/ratings.csv", header=True, inferSchema=True)

# Construction du modèle ALS
als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop"
)

# Division des données en ensembles de formation et de test
(training, test) = data.randomSplit([0.8, 0.2])

# Entraînement du modèle
model = als.fit(training)

# Faire des prédictions sur l'ensemble de test
predictions = model.transform(test)

# Évaluation du modèle
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Utiliser le modèle pour recommander des films à un utilisateur donné
user_id = 123  # ID de l'utilisateur pour lequel vous voulez faire des recommandations
user_recs = model.recommendForUserSubset(spark.createDataFrame([(user_id,)]), 10)  # 10 recommandations pour l'utilisateur
user_movies = user_recs.select("recommendations.movieId")

# Afficher les recommandations pour l'utilisateur
user_movies.show()
