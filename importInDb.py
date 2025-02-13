import json
from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, MOVIES_COLLECTION_NAME, GENRES_COLLECTION_NAME

# Fichiers JSON contenant les films et les genres
MOVIES_FILE = "movies.json"
GENRES_FILE = "genres.json"


def connect_to_mongo(uri):
    try:
        client = MongoClient(uri)
        print("✅ Connexion à MongoDB établie.")
        return client
    except Exception as e:
        print(f"❌ Erreur de connexion à MongoDB : {e}")
        return None


def insert_genres_into_mongo(db, collection_name, genres):
    """ Insère les genres dans MongoDB. """
    try:
        collection = db[collection_name]
        # Crée un index unique sur "id" pour éviter les doublons
        collection.create_index("id", unique=True)
        result = collection.insert_many(genres, ordered=False)  # `ordered=False` ignore les doublons
        print(f"🎭 {len(result.inserted_ids)} genres ont été insérés dans MongoDB.")
    except Exception as e:
        print(f"⚠️ Erreur lors de l'insertion des genres dans MongoDB : {e}")


def insert_movies_into_mongo(db, collection_name, movies):
    """ Insère les films dans MongoDB. """
    try:
        collection = db[collection_name]
        # Crée un index unique sur "id" pour éviter les doublons
        collection.create_index("id", unique=True)
        result = collection.insert_many(movies, ordered=False)  # `ordered=False` ignore les doublons
        print(f"🎬 {len(result.inserted_ids)} films ont été insérés dans MongoDB.")
    except Exception as e:
        print(f"⚠️ Erreur lors de l'insertion des films dans MongoDB : {e}")


def load_json_data(filename):
    """ Charge les données depuis un fichier JSON. """
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"📁 {len(data)} éléments chargés depuis le fichier {filename}.")
            return data
    except Exception as e:
        print(f" ⚠️ Erreur lors du chargement du fichier JSON : {e}")
        return []


def add_genres_to_movies(movies, genres):
    """ Ajoute les genres aux films. """
    genre_dict = {genre['id']: genre['name'] for genre in genres}

    for movie in movies:
        movie['genres'] = [genre_dict.get(genre_id) for genre_id in movie.get('genre_ids', [])]

    return movies


if __name__ == "__main__":
    # Charge les films et les genres depuis les fichiers JSON
    movies = load_json_data(MOVIES_FILE)
    genres = load_json_data(GENRES_FILE)

    # Connecte-vous à MongoDB
    client = connect_to_mongo(MONGO_URI)

    if client:
        db = client[DB_NAME]

        # Insère les genres dans MongoDB
        insert_genres_into_mongo(db, GENRES_COLLECTION_NAME, genres)

        # Ajoute les genres aux films
        movies_with_genres = add_genres_to_movies(movies, genres)

        # Insère les films dans MongoDB
        insert_movies_into_mongo(db, MOVIES_COLLECTION_NAME, movies_with_genres)

        # Ferme la connexion
        client.close()
        print("🔒 Connexion à MongoDB fermée.")
