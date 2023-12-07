import logging
import os, random
from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.ml.recommendation import ALSModel
from datetime import datetime

app = Flask(__name__)

# Set up Logging Function
def setup_producer_logging():
    log_directory = "/home/StreamingmovieRecommandationBigDataIA/Logs/FLASK_APP/"
    os.makedirs(log_directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    producer_logger = logging.getLogger(__name__)  

    return producer_logger

# Set up logger
logger = setup_producer_logging()

# Connexion à Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Configuration et initialisation de Spark
spark = SparkSession.builder \
    .appName("MovieRecommendationSystem") \
    .getOrCreate()


# Chargement du modèle sauvegardé
saved_model_path = "/home/StreamingmovieRecommandationBigDataIA/src/recommandationSystem/model"
try:
    loaded_model = ALSModel.load(saved_model_path)
    logger.info("Modèle chargé avec succès.")
except Exception as e:
    logger.error(f"Erreur lors du chargement du modèle : {str(e)}")
    raise SystemExit


def get_random_image_url(title, genre):
    # Exemple de fonction pour générer des URLs d'images aléatoires en se basant sur le titre et le genre
    # Ceci est une fonction factice, elle devrait être remplacée par une logique de génération d'URLs réelle
    keywords = title.lower().split() + genre.lower().split()
    seed = sum(ord(char) for char in ''.join(keywords))
    random.seed(seed)
    image_url = f'https://source.unsplash.com/300x400/?{"+".join(keywords)}'
    return image_url

@app.route('/', methods=['GET'])
def home():
    films = [
        {
            'title': 'Film 1',
            'genre': 'Action',
            'year': '2023',
            'rating': '4.5'
        },
        {
            'title': 'Film 2',
            'genre': 'Comedy',
            'year': '2000',
            'rating': '3.8'
        },
        {
            'title': 'Film 3',
            'genre': 'Drama',
            'year': '2017',
            'rating': '3.2'
        },
        {
            'title': 'Film 4',
            'genre': 'Thriller',
            'year': '2014',
            'rating': '4.8'
        },        
    ]

    film_cards = ''

    for film in films:
        image_url = get_random_image_url(film['title'], film['genre'])
        film_cards += f'''
            <div style="border: 1px solid #ccc; border-radius: 5px; padding: 10px; margin: 10px; display: inline-block; width: calc(33.33% - 20px);">
                <img src="{image_url}" alt="{film['title']}" style="width: 100%; height: 200px; object-fit: cover;">
                <h3>{film['title']}</h3>
                <p><strong>Genre:</strong> {film['genre']}</p>
                <p><strong>Année:</strong> {film['year']}</p>
                <p><strong>Note:</strong> {film['rating']}</p>
            </div>
        '''

    return f'''
    <!DOCTYPE html>
    <html>
    <head>
        <title>FidelisProd - Accueil</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                display: flex;
                flex-direction: column;
                height: 100vh;
            }}
            header {{
                background-color: #333;
                color: white;
                text-align: center;
                padding: 10px 0;
            }}
            header h1 {{
                margin: 0;
            }}
            nav {{
                background-color: #eee;
                padding: 10px 0;
                text-align: center;
            }}
            nav input[type="text"] {{
                padding: 5px;
                width: 200px;
            }}
            main {{
                padding: 20px;
                display: flex;
                flex-wrap: wrap;
                justify-content: space-around;
                flex-grow: 1;
            }}
            footer {{
                background-color: #333;
                color: white;
                text-align: center;
                padding: 10px 0;
                width: 100%;
                flex-shrink: 0;
            }}
        </style>
    </head>
    <body>
        <header>
            <h1>FidelisProd</h1>
        </header>
        <nav>
            <form action="/recommendation" method="get">
                <input type="text" name="title" placeholder="Rechercher un film...">
                <input type="submit" value="Rechercher">
            </form>
        </nav>
        <main>
            {film_cards}
        </main>
        <footer>
            &copy; 2023 FidelisProd. Tous droits réservés.
        </footer>
    </body>
    </html>
    '''
# Route pour la recommandation de films
@app.route('/recommendation', methods=['GET'])
def get_recommendations():
    try:
        # Récupérer le titre du film depuis la requête
        movie_title = request.args.get('title')

        # Requête Elasticsearch pour obtenir les données du film recherché
        es_query = {
            "query": {
                "match": {
                    "title": movie_title
                }
            }
        }
        # Récupérer les résultats de la recherche Elasticsearch
        res = es.search(index="movierecommendation_index", body=es_query)

        # Si aucun résultat n'est trouvé, renvoyer un message indiquant que le film n'est pas disponible
        if res['hits']['total']['value'] == 0:
            logger.warning(f"Aucun film trouvé pour le titre : {movie_title}")
            return jsonify({'message': 'Film non disponible'})

        # Obtenir l'ID du film recherché
        movie_id = res['hits']['hits'][0]['_source']['movieId']

        # Faire une recommandation de films similaires basée sur le film recherché
        user_id = 196  # Ici, l'ID de l'utilisateur est défini arbitrairement à 1
        recommendations = loaded_model.recommendForUserSubset(spark.createDataFrame([(user_id, movie_id)], ["userId", "movieId"]), 10)

        # Récupérer les titres des films recommandés
        recommended_movies = [row['movieId'] for row in recommendations.select("movieId").collect()]

        result = f'<h1>Film demandé : {movie_title}</h1>'
        result += '<h2>Films recommandés :</h2>'

        # Récupérer les détails des films recommandés depuis Elasticsearch
        for movie_id in recommended_movies:
            es_query = {
                "query": {
                    "match": {
                        "movieId": movie_id 
                    }
                }
            }
            res = es.search(index="movierecommendation_index", body=es_query)
            if res['hits']['total']['value'] > 0:
                recommended_movie_title = res['hits']['hits'][0]['_source']['title']
                result += f'<p>{recommended_movie_title}</p>'

        return result
    except Exception as e:
        logger.error(f"Erreur lors de la recommandation : {str(e)}")
        return jsonify({'message': 'Une erreur s\'est produite lors de la recommandation'})

if __name__ == '__main__':
    app.run(debug=True)
