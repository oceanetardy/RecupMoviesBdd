import json
import requests
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import MONGO_URI, DB_NAME, MOVIES_COLLECTION_NAME, GENRES_COLLECTION_NAME, TOKEN

# Fichiers JSON
MOVIES_FILE = "movies.json"
GENRES_FILE = "genres.json"
PROVIDERS_FILE = "providers.json"
CAST_CACHE_FILE = "cast_cache.json"

TMDB_API_URL = "https://api.themoviedb.org/3/movie/{}/credits"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# 🚀 Connexion à MongoDB
def connect_to_mongo(uri):
    try:
        client = MongoClient(uri)
        print("✅ Connexion à MongoDB établie.")
        return client
    except Exception as e:
        print(f"❌ Erreur de connexion à MongoDB : {e}")
        return None

# 📥 Chargement des fichiers JSON
def load_json_data(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"📁 {len(data)} éléments chargés depuis {filename}.")
            return data
    except Exception as e:
        print(f"⚠️ Erreur lors du chargement de {filename} : {e}")
        return []

# 🎭 Ajout des genres aux films
def add_genres_to_movies(movies, genres):
    genre_dict = {genre['id']: genre['name'] for genre in genres}
    for movie in movies:
        movie['genres'] = [genre_dict.get(genre_id) for genre_id in movie.get('genre_ids', [])]
    return movies

# 📺 Ajout des providers aux films
def add_providers_to_movies(movies, providers):
    for movie in movies:
        movie_id = str(movie["id"])
        movie["providers"] = providers.get(movie_id, {})
    return movies

# 🎭 Chargement du cache des acteurs
def load_cast_cache():
    try:
        with open(CAST_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

# 🗄️ Sauvegarde du cache des acteurs
def save_cast_cache(cache):
    with open(CAST_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4)

# 🎭 Récupération du casting pour un film
def fetch_cast(movie, cast_cache):
    movie_id = str(movie["id"])

    # Vérifier si les données sont déjà en cache
    if movie_id in cast_cache:
        print(f"🔄 Casting déjà en cache pour {movie['title']}")
        movie["cast"] = cast_cache[movie_id]
        return movie

    url = TMDB_API_URL.format(movie_id)

    try:
        response = requests.get(url, headers=HEADERS, timeout=5)
        response.raise_for_status()
        data = response.json()

        movie["cast"] = [
            {
                "id": actor["id"],
                "name": actor["name"],
                "character": actor.get("character", ""),
                "profile_path": actor.get("profile_path", "")
            }
            for actor in data.get("cast", [])[:10]
        ]

        # Ajouter au cache
        cast_cache[movie_id] = movie["cast"]
        print(f"✅ Casting récupéré pour {movie['title']}")

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Erreur pour {movie['id']} : {e}")

    return movie

# 🎭 Ajout des acteurs aux films en parallèle
def add_cast_to_movies(movies):
    cast_cache = load_cast_cache()

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_movie = {executor.submit(fetch_cast, movie, cast_cache): movie for movie in movies}
        for future in as_completed(future_to_movie):
            future.result()

    save_cast_cache(cast_cache)  # Sauvegarder le cache mis à jour
    return movies

# 🗄️ Insertion des genres en BDD
def insert_genres_into_mongo(db, collection_name, genres):
    try:
        collection = db[collection_name]
        collection.create_index("id", unique=True)
        collection.insert_many(genres, ordered=False)
        print(f"🎭 Genres insérés en MongoDB : {len(genres)}")
    except Exception as e:
        print(f"⚠️ Erreur insertion genres : {e}")

# 🎬 Insertion des films en BDD
def insert_movies_into_mongo(db, collection_name, movies):
    try:
        collection = db[collection_name]
        collection.create_index("id", unique=True)
        collection.insert_many(movies, ordered=False)
        print(f"🎬 Films insérés en MongoDB : {len(movies)}")
    except Exception as e:
        print(f"⚠️ Erreur insertion films : {e}")

# 🏁 Script principal
if __name__ == "__main__":
    movies = load_json_data(MOVIES_FILE)
    genres = load_json_data(GENRES_FILE)
    providers = load_json_data(PROVIDERS_FILE)

    client = connect_to_mongo(MONGO_URI)

    if client:
        db = client[DB_NAME]

        #  Insérer les genres
        insert_genres_into_mongo(db, GENRES_COLLECTION_NAME, genres)

        #  Ajouter les genres, providers et cast aux films
        movies = add_genres_to_movies(movies, genres)
        movies = add_providers_to_movies(movies, providers)
        movies = add_cast_to_movies(movies)

        #  Insérer les films en BDD
        insert_movies_into_mongo(db, MOVIES_COLLECTION_NAME, movies)

        # 🔚 Fermer MongoDB
        client.close()
        print("🔒 Connexion MongoDB fermée.")
