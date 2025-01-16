import requests
import json
from config import API_KEY

# Configuration
BASE_URL = "https://api.themoviedb.org/3/movie/popular"
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# Fichier de sortie
OUTPUT_FILE = "movies.json"


def check_api_status():
    print("Vérification de l'état de l'API...")
    try:
        response = requests.get(BASE_URL, headers=HEADERS, params={"language": "en-US", "page": 1})
        response.raise_for_status()
        print("L'API est accessible. Début de la récupération des données.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Erreur : Impossible d'accéder à l'API. Détails : {e}")
        return False


def fetch_all_movies():
    all_movies = []
    page = 1
    total_pages = 1  # On commence en supposant qu'il y a au moins une page

    while page <= total_pages:
        print(f"Récupération de la page {page}...")
        try:
            # Requête à l'API avec la pagination
            response = requests.get(BASE_URL, headers=HEADERS, params={"language": "en-US", "page": page})
            response.raise_for_status()

            data = response.json()
            total_pages = data.get("total_pages", 1)

            # Ajoute les films de la page actuelle à la liste
            all_movies.extend(data.get("results", []))
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la récupération des données : {e}")
            break

    print("Récupération terminée.")
    return all_movies


def save_to_json_file(data, filename):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Les données ont été enregistrées dans {filename}.")
    except Exception as e:
        print(f"Erreur lors de l'enregistrement du fichier : {e}")


if __name__ == "__main__":
    # Vérifie si l'API est accessible avant de récupérer les données
    if check_api_status():
        # Récupère toutes les données
        movies = fetch_all_movies()

        # Sauvegarde dans un fichier JSON
        save_to_json_file(movies, OUTPUT_FILE)
    else:
        print("Le programme s'est arrêté car l'API n'est pas accessible.")
