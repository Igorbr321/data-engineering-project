import requests
import logging
import pandas as pd

API_POKEMON = "https://pokeapi.co/api/v2/pokemon?limit=1000"
API_POKEMON_UNITARIO = "https://pokeapi.co/api/v2/pokemon/{}"


def extract_pokemons_bronze(csv_path: str = "endpoints.csv"):
    logging.info("Iniciando extração dos endpoints da PokeAPI")

    response = requests.get(API_POKEMON)
    df = pd.DataFrame(response.json()["results"])

    detalhes = []

    for name in df["name"].head(100):
        url = API_POKEMON_UNITARIO.format(name)
        response = requests.get(url)
        detalhes.append(response.json())

    df_detalhes = pd.DataFrame(detalhes)
    df_detalhes.to_csv(f'Data\{csv_path}', index=False)

    logging.info("Extração dos endpoints concluída e salva em CSV")
    
    return df_detalhes





