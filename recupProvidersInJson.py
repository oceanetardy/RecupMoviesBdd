import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import API_KEY

# Configuration
BASE_URL_PROVIDERS = "https://api.themoviedb.org/3/movie/{movie_id}/watch/providers"
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

MOVIES_FILE = "movies.json"
PROVIDERS_FILE = "providers.json"
NUM_THREADS = 10


def load_existing_providers():
    """Charge les fournisseurs déjà récupérés pour éviter de refaire des appels inutiles."""
    try:
        with open(PROVIDERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def fetch_providers(movie_id):
    url = BASE_URL_PROVIDERS.format(movie_id=movie_id)
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json().get("results", {})

        return {
            "FR": extract_provider_info(data.get("FR", {})),
            "US": extract_provider_info(data.get("US", {}))
        }
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Erreur lors de la récupération pour le film {movie_id} : {e}")
        return {}


def extract_provider_info(country_data):
    return {
        "link": country_data.get("link"),
        "rent": [p["provider_name"] for p in country_data.get("rent", [])],
        "buy": [p["provider_name"] for p in country_data.get("buy", [])],
        "flatrate": [p["provider_name"] for p in country_data.get("flatrate", [])],
    }


def save_providers(providers_data):
    with open(PROVIDERS_FILE, "w", encoding="utf-8") as f:
        json.dump(providers_data, f, ensure_ascii=False, indent=4)


def main():
    print("🔍 Chargement des films...")
    try:
        with open(MOVIES_FILE, "r", encoding="utf-8") as f:
            movies = json.load(f)
    except FileNotFoundError:
        print("❌ Erreur : Le fichier movies.json n'existe pas.")
        return

    existing_providers = load_existing_providers()
    new_providers = existing_providers.copy()  # Copie pour mise à jour

    movie_ids = [movie["id"] for movie in movies]
    total_movies = len(movie_ids)
    remaining_movies = [m_id for m_id in movie_ids if str(m_id) not in existing_providers]

    print(f"🎬 {total_movies} films trouvés, {len(remaining_movies)} à traiter.")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_to_id = {executor.submit(fetch_providers, m_id): m_id for m_id in remaining_movies}

        for i, future in enumerate(as_completed(future_to_id)):
            movie_id = future_to_id[future]
            providers = future.result()

            if providers:
                new_providers[str(movie_id)] = providers

            elapsed_time = time.time() - start_time
            movies_done = i + 1
            avg_time_per_movie = elapsed_time / movies_done
            estimated_remaining_time = avg_time_per_movie * (len(remaining_movies) - movies_done)

            print(f"✅ {movies_done}/{len(remaining_movies)} traités - Temps restant estimé : {estimated_remaining_time:.2f} sec")

            if movies_done % 50 == 0:
                save_providers(new_providers)

    save_providers(new_providers)
    print("📁 Sauvegarde terminée dans providers.json")


if __name__ == "__main__":
    main()
