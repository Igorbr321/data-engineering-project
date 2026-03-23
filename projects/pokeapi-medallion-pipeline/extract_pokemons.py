import requests
import logging
import pandas as pd

API_POKEMON = "https://pokeapi.co/api/v2/pokemon?limit=1000"
API_POKEMON_UNITARIO = "https://pokeapi.co/api/v2/pokemon/{}"


def extract_endpoints(csv_path: str = "endpoints.csv"):
    logging.info("Iniciando extração dos endpoints da PokeAPI")

    response = requests.get(API_POKEMON)
    df = pd.DataFrame(response.json()["results"])

    detalhes = []

    for name in df["name"]:
        url = API_POKEMON_UNITARIO.format(name)
        response = requests.get(url)
        detalhes.append(response.json())

    df_detalhes = pd.DataFrame(detalhes)
    df_detalhes.to_csv(f'Data\{csv_path}', index=False)

    
    return df_detalhes





