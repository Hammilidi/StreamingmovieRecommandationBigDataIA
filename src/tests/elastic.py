from elasticsearch import Elasticsearch

# Connexion à Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Vérification de la connexion
if es.ping():
    print("Connecté à Elasticsearch !")
else:
    print("Échec de la connexion à Elasticsearch.")
