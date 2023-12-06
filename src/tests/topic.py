from confluent_kafka.admin import AdminClient

# Configuration pour l'AdminClient
admin_config = {'bootstrap.servers': 'localhost:9092'}  # Assurez-vous que c'est le bon broker

# Création de l'AdminClient
admin_client = AdminClient(admin_config)

# Liste des topics
topic_metadata = admin_client.list_topics()

# Vérification de l'existence du topic
if 'movierecommandation' in topic_metadata.topics:
    print("Le topic 'movierecommandation' existe.")
else:
    print("Le topic 'movierecommandation' n'existe pas.")