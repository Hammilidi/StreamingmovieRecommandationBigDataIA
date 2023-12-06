from elasticsearch import Elasticsearch

# Remplacez ces valeurs par les informations de votre cluster Elastic Cloud
cloud_id = "movie_recommandation:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ4OTAxYzljOWNiNTQ0MGJlOWExZDZjZGRiZjFhNGExYSQzMzY1ZGQyNjFjMTA0YzY4OGQwN2QwZTE4M2QwNjI3OA=="
username = "elastic"
password = "wBrbOPeiGOMO2G0i4R6BHKAs"

# Initialisation de la connexion
es = Elasticsearch(
    cloud_id=cloud_id,
    http_auth=(username, password)
)

# Vérifier la connexion
if es.ping():
    print("Connecté à Elastic Cloud !")
else:
    print("La connexion a échoué.")
