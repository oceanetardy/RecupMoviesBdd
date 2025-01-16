import json
from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, COLLECTION_NAME

# Fichier JSON contenant les films
INPUT_FILE = "movies.json"


def connect_to_mongo(uri):
    try:
        client = MongoClient(uri)
        print("Connexion à MongoDB établie.")
        return client
    except Exception as e:
        print(f"Erreur de connexion à MongoDB : {e}")
        return None


def insert_movies_into_mongo(db, collection_name, movies):
    try:
        collection = db[collection_name]
        # Crée un index unique sur "id" pour éviter les doublons
        collection.create_index("id", unique=True)
        result = collection.insert_many(movies, ordered=False)  # `ordered=False` ignore les doublons
        print(f"{len(result.inserted_ids)} films ont été insérés dans MongoDB.")
    except Exception as e:
        print(f"Erreur lors de l'insertion des films dans MongoDB : {e}")


def load_movies_from_json(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            movies = json.load(f)
            print(f"{len(movies)} films ont été chargés depuis le fichier {filename}.")
            return movies
    except Exception as e:
        print(f"Erreur lors du chargement du fichier JSON : {e}")
        return []


if __name__ == "__main__":
    movies = load_movies_from_json(INPUT_FILE)

    client = connect_to_mongo(MONGO_URI)

    if client:
        db = client[DB_NAME]

        insert_movies_into_mongo(db, COLLECTION_NAME, movies)

        client.close()
        print("Connexion à MongoDB fermée.")
