import requests
import json
from config import TOKEN

# Configuration
BASE_URL_MOVIES = "https://api.themoviedb.org/3/movie/popular"
BASE_URL_GENRES = "https://api.themoviedb.org/3/genre/movie/list"

HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}

# Fichiers de sortie
MOVIES_FILE = "movies.json"
GENRES_FILE = "genres.json"


def check_api_status():
    """ Vérifie si l'API est accessible. """
    print("🔍 Vérification de l'état de l'API...")
    try:
        response = requests.get(BASE_URL_MOVIES, headers=HEADERS, params={"language": "en-US", "page": 1})
        response.raise_for_status()
        print("✅ L'API est accessible. Début de la récupération des données.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur : Impossible d'accéder à l'API. Détails : {e}")
        return False


def fetch_all_movies():
    """ Récupère tous les films populaires de l'API TMDB. """
    all_movies = []
    page = 1
    total_pages = 1  # On commence avec l'hypothèse qu'il y a au moins une page

    while page <= total_pages:
        print(f"📄 Récupération de la page {page}...")
        try:
            response = requests.get(BASE_URL_MOVIES, headers=HEADERS, params={"language": "en-US", "page": page})
            response.raise_for_status()

            data = response.json()
            total_pages = data.get("total_pages", 1)  # Mise à jour du nombre total de pages

            # Ajoute les films de la page actuelle à la liste
            all_movies.extend(data.get("results", []))
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Erreur lors de la récupération des films : {e}")
            break

    print(f"✅ Récupération terminée ({len(all_movies)} films récupérés).")
    return all_movies


def fetch_genres():
    """ Récupère la liste des genres de films. """
    print("🎭 Récupération des genres...")
    try:
        response = requests.get(BASE_URL_GENRES, headers=HEADERS, params={"language": "en-US"})
        response.raise_for_status()

        data = response.json()
        genres = data.get("genres", [])

        print(f"✅ {len(genres)} genres récupérés.")
        return genres

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Erreur lors de la récupération des genres : {e}")
        return []


def save_to_json_file(data, filename):
    """ Sauvegarde les données dans un fichier JSON. """
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"📁 Données enregistrées dans {filename}.")
    except Exception as e:
        print(f"❌ Erreur lors de l'enregistrement du fichier {filename} : {e}")


if __name__ == "__main__":
    # Vérifie si l'API est accessible avant de récupérer les données
    if check_api_status():
        # Récupère les films et les genres
        movies = fetch_all_movies()
        genres = fetch_genres()

        # Sauvegarde les données
        save_to_json_file(movies, MOVIES_FILE)
        save_to_json_file(genres, GENRES_FILE)

        print("🎬 Fin du processus. Les films et les genres ont été enregistrés avec succès.")
    else:
        print("🚫 Le programme s'est arrêté car l'API n'est pas accessible.")
