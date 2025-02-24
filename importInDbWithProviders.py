import json
from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, MOVIES_COLLECTION_NAME

# Fichiers JSON contenant les films et les fournisseurs
MOVIES_FILE = "movies.json"
PROVIDERS_FILE = "providers.json"


def connect_to_mongo(uri):
    """ Établit la connexion à MongoDB """
    try:
        client = MongoClient(uri)
        print("✅ Connexion à MongoDB établie.")
        return client
    except Exception as e:
        print(f"❌ Erreur de connexion à MongoDB : {e}")
        return None


def load_json_data(filename):
    """ Charge les données depuis un fichier JSON. """
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"📁 {len(data)} éléments chargés depuis {filename}.")
            return data
    except Exception as e:
        print(f"⚠️ Erreur lors du chargement du fichier {filename} : {e}")
        return {}


def add_providers_to_movies(movies, providers):
    """ Ajoute les fournisseurs de streaming aux films """
    for movie in movies:
        movie_id = str(movie.get('id'))  # Convertir en string pour correspondre aux clés de providers.json

        # Vérifie si des providers existent pour ce film
        if movie_id in providers:
            movie['providers'] = providers[movie_id]  # Ajoute les providers existants
        else:
            movie['providers'] = {"FR": {}, "US": {}}  # Ajoute un champ vide par défaut

    return movies


def insert_movies_into_mongo(db, collection_name, movies):
    """ Insère les films dans MongoDB. """
    try:
        collection = db[collection_name]
        collection.create_index("id", unique=True)  # Crée un index unique sur "id" pour éviter les doublons
        result = collection.insert_many(movies, ordered=False)  # Ignore les doublons
        print(f"🎬 {len(result.inserted_ids)} films ont été insérés dans MongoDB.")
    except Exception as e:
        print(f"⚠️ Erreur lors de l'insertion des films dans MongoDB : {e}")


if __name__ == "__main__":
    # Chargement des films et des fournisseurs
    movies = load_json_data(MOVIES_FILE)
    providers = load_json_data(PROVIDERS_FILE)

    # Connexion MongoDB
    client = connect_to_mongo(MONGO_URI)

    if client:
        db = client[DB_NAME]

        # Ajoute les providers aux films
        movies_with_providers = add_providers_to_movies(movies, providers)

        # Insère les films dans la base de données
        insert_movies_into_mongo(db, MOVIES_COLLECTION_NAME, movies_with_providers)

        # Ferme la connexion
        client.close()
        print("🔒 Connexion MongoDB fermée.")
